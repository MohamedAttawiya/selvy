import { START, END, MessagesValue, StateGraph, StateSchema } from "@langchain/langgraph";
import { z } from "zod/v4";
import { METRICS_TABLE } from "../config.mjs";
import { getRecentUserText } from "../domain/common.mjs";
import { buildDateWindowFootnote } from "../domain/summary-format.mjs";
import { embedText } from "../services/embeddings.mjs";
import { searchSchemas } from "../workflows/orders_analytics/clients.mjs";
import { routeCandidatesDeterministic, routeSelectHybrid } from "./router.mjs";

const BaseGraphState = new StateSchema({
  conversation_id: z.string().default(""),
  messages: MessagesValue,
  embedding: z.array(z.number()).default(() => []),
  schemas: z.array(z.any()).default(() => []),
  routing: z.any().nullable().default(() => null),
  workflow_result: z.any().nullable().default(() => null),
  date_resolution: z.any().nullable().default(() => null),
  date_footnote: z.string().nullable().default(() => null),
  reply_text: z.string().nullable().default(() => null),
  reply_blocks: z.array(z.any()).default(() => []),
});

export function createBaseGraph({ services, registry, checkpointer }) {
  return new StateGraph(BaseGraphState)
    .addNode("embed", async (state) => {
      const contextText = getRecentUserText(state.messages, 3);
      const embedding = contextText ? await embedText(services, contextText) : [];
      return { embedding };
    })
    .addNode("search", async (state) => {
      if (!state.embedding?.length) return { schemas: [] };
      const schemas = await searchSchemas(services, state.embedding, 5, METRICS_TABLE);
      return { schemas };
    })
    .addNode("route_candidates", async (state) => {
      const narrowed = routeCandidatesDeterministic({
        messages: state.messages,
        registry,
      });
      return {
        routing: {
          workflowId: null,
          confidence: 0,
          candidateIds: narrowed.candidateIds,
          reason: narrowed.reason,
        },
      };
    })
    .addNode("route_select", async (state) => {
      const decision = await routeSelectHybrid({
        services,
        messages: state.messages,
        registry,
        candidateIds: state.routing?.candidateIds || [],
      });
      return { routing: decision };
    })
    .addNode("run_workflow", async (state) => {
      const workflowId = state.routing?.workflowId;
      const workflow = workflowId ? registry.get(workflowId) : null;
      if (!workflow) {
        return {
          workflow_result: {
            extraction: { nudge: "No workflow selected for this request." },
            summary: null,
          },
        };
      }

      const workflowResult = await workflow.run({
        input: {
          messages: state.messages,
          schemas: state.schemas,
          embedding: state.embedding,
        },
        threadId: state.conversation_id,
      });

      return { workflow_result: workflowResult };
    })
    .addNode("finalize_reply", async (state) => {
      const extraction = state.workflow_result?.extraction || null;

      let replyText = state.workflow_result?.summary;
      if (!replyText) {
        replyText = extraction?.nudge
          ? extraction.nudge
          : JSON.stringify(extraction || { nudge: "No extraction result." }, null, 2);
      }

      const dateFootnote = buildDateWindowFootnote(extraction);
      const hasDateFootnote = !!dateFootnote && !/Date windows? used:/i.test(replyText);
      if (hasDateFootnote && !replyText.trim().startsWith("{")) {
        replyText = `${replyText}\n\n${dateFootnote}`;
      }

      const dateResolution = extraction
        ? {
          source: extraction.time_range_source || null,
          time_range: extraction.time_range || null,
          comparison_periods: extraction.comparison_periods || null,
        }
        : null;

      return {
        reply_text: replyText,
        date_footnote: hasDateFootnote ? dateFootnote : null,
        date_resolution: dateResolution,
      };
    })
    .addNode("respond", async (state) => {
      const replyText = String(state.reply_text || "").trim();
      if (!replyText) {
        return {
          reply_blocks: [{ type: "section", text: { type: "mrkdwn", text: "I couldn't produce a response." } }],
        };
      }

      const isJsonReply = replyText.startsWith("{");
      const blocks = isJsonReply
        ? [{ type: "section", text: { type: "mrkdwn", text: `\`\`\`${replyText}\`\`\`` } }]
        : [{ type: "section", text: { type: "mrkdwn", text: replyText } }];
      if (isJsonReply && state.date_footnote) {
        blocks.push({ type: "section", text: { type: "mrkdwn", text: state.date_footnote } });
      }

      return { reply_blocks: blocks };
    })
    .addEdge(START, "embed")
    .addEdge("embed", "search")
    .addEdge("search", "route_candidates")
    .addEdge("route_candidates", "route_select")
    .addEdge("route_select", "run_workflow")
    .addEdge("run_workflow", "finalize_reply")
    .addEdge("finalize_reply", "respond")
    .addEdge("respond", END)
    .compile({ checkpointer });
}

export { BaseGraphState };
