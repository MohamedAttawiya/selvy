import { assertWorkflowContract } from "./contract.mjs";
import { createOrdersAnalyticsWorkflow } from "./orders_analytics/index.mjs";

export function createWorkflowRegistry({ services, checkpointer }) {
  const workflows = [
    createOrdersAnalyticsWorkflow({ services, checkpointer }),
  ];

  const registry = new Map();
  for (const workflow of workflows) {
    const normalized = assertWorkflowContract(workflow);
    if (registry.has(normalized.id)) {
      throw new Error(`Duplicate workflow id: ${normalized.id}`);
    }
    registry.set(normalized.id, normalized);
  }

  return registry;
}
