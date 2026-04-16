/**
 * @typedef {Object} WorkflowContract
 * @property {string} id
 * @property {Array<string|{pattern:string,weight?:number}>} intentSignals
 * @property {(input:{services:object,checkpointer:object}) => any} buildSubgraph
 * @property {(input:{input:object,threadId:string}) => Promise<object>} run
 * @property {string[]} capabilities
 */

/**
 * @param {any} workflow
 * @returns {WorkflowContract}
 */
export function assertWorkflowContract(workflow) {
  const violations = [];

  if (!workflow || typeof workflow !== "object") {
    throw new Error("Workflow contract violation: workflow must be an object.");
  }

  if (typeof workflow.id !== "string" || !workflow.id.trim()) {
    violations.push("id must be a non-empty string");
  }
  if (!Array.isArray(workflow.intentSignals)) {
    violations.push("intentSignals must be an array");
  }
  if (typeof workflow.buildSubgraph !== "function") {
    violations.push("buildSubgraph must be a function");
  }
  if (typeof workflow.run !== "function") {
    violations.push("run must be a function");
  }
  if (!Array.isArray(workflow.capabilities)) {
    violations.push("capabilities must be an array");
  }

  if (violations.length) {
    throw new Error(`Workflow contract violation for '${workflow.id || "unknown"}': ${violations.join("; ")}`);
  }

  return workflow;
}
