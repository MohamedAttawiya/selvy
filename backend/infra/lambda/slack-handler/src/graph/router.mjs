import { InvokeModelCommand } from "@aws-sdk/client-bedrock-runtime";
import { BEDROCK_EXTRACT_MODEL_ID } from "../config.mjs";

function scoreIntentSignals(signalList, text) {
  if (!Array.isArray(signalList) || !signalList.length) return 0;
  const lower = String(text || "").toLowerCase();
  let score = 0;
  for (const signal of signalList) {
    if (typeof signal === "string") {
      if (lower.includes(signal.toLowerCase())) score += 1;
      continue;
    }
    if (!signal || typeof signal !== "object") continue;
    if (!signal.pattern || typeof signal.pattern !== "string") continue;
    try {
      const weight = Number.isFinite(signal.weight) ? Number(signal.weight) : 1;
      if (new RegExp(signal.pattern, "i").test(text)) score += weight;
    } catch {
      // ignore malformed pattern
    }
  }
  return score;
}

/**
 * @param {{messages:any[], registry:Map<string, any>}} input
 */
export function routeCandidatesDeterministic(input) {
  const text = (input.messages || [])
    .map((msg) => String(msg?.content || "").trim())
    .filter(Boolean)
    .slice(-3)
    .join(" | ");

  const scores = [];
  for (const [id, workflow] of input.registry.entries()) {
    const score = scoreIntentSignals(workflow.intentSignals, text);
    scores.push({ id, score });
  }

  const maxScore = scores.reduce((max, item) => Math.max(max, item.score), 0);
  const candidates = maxScore > 0
    ? scores.filter((item) => item.score === maxScore).map((item) => item.id)
    : scores.map((item) => item.id);

  return {
    candidateIds: candidates,
    reason: maxScore > 0 ? "deterministic signal match" : "no deterministic match; all workflows eligible",
  };
}

async function llmSelectWorkflow(services, text, candidates) {
  const system = `You are a strict workflow router.\nChoose exactly one workflow ID from the candidate list.\nReturn only compact JSON: {\"workflowId\":\"...\",\"confidence\":0.0-1.0,\"reason\":\"...\"}.`;
  const prompt = `User request:\n${text}\n\nCandidates:\n${JSON.stringify(candidates, null, 2)}\n\nSelect one workflow ID from the list only.`;

  const response = await services.clients.getBedrock().send(new InvokeModelCommand({
    modelId: BEDROCK_EXTRACT_MODEL_ID,
    contentType: "application/json",
    accept: "application/json",
    body: JSON.stringify({
      anthropic_version: "bedrock-2023-05-31",
      max_tokens: 180,
      temperature: 0,
      system,
      messages: [{ role: "user", content: prompt }],
    }),
  }));

  const raw = JSON.parse(new TextDecoder().decode(response.body));
  const textOut = String(raw?.content?.[0]?.text || "").trim();
  const match = textOut.match(/\{[\s\S]*\}/);
  if (!match) return null;

  try {
    return JSON.parse(match[0]);
  } catch {
    return null;
  }
}

/**
 * @param {{services:any, messages:any[], registry:Map<string,any>, candidateIds:string[]}} input
 * @returns {Promise<{workflowId:string, confidence:number, candidateIds:string[], reason:string}>}
 */
export async function routeSelectHybrid(input) {
  const candidateIds = Array.isArray(input.candidateIds) ? input.candidateIds : [];
  if (!candidateIds.length) {
    return {
      workflowId: null,
      confidence: 0,
      candidateIds: [],
      reason: "no eligible workflows",
    };
  }

  if (candidateIds.length === 1) {
    return {
      workflowId: candidateIds[0],
      confidence: 1,
      candidateIds,
      reason: "single deterministic candidate",
    };
  }

  const text = (input.messages || [])
    .map((msg) => String(msg?.content || "").trim())
    .filter(Boolean)
    .slice(-3)
    .join(" | ");

  const candidates = candidateIds.map((id) => {
    const wf = input.registry.get(id);
    return {
      id,
      capabilities: wf?.capabilities || [],
      intentSignals: wf?.intentSignals || [],
    };
  });

  const decision = await llmSelectWorkflow(input.services, text, candidates);
  const chosen = decision?.workflowId;
  if (typeof chosen === "string" && candidateIds.includes(chosen)) {
    return {
      workflowId: chosen,
      confidence: Number.isFinite(Number(decision?.confidence)) ? Number(decision.confidence) : 0.65,
      candidateIds,
      reason: String(decision?.reason || "LLM choice among deterministic candidates"),
    };
  }

  return {
    workflowId: candidateIds[0],
    confidence: 0.5,
    candidateIds,
    reason: "LLM fallback to first candidate",
  };
}
