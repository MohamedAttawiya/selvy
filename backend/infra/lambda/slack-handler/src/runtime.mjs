import { buildServices } from "./services/aws-clients.mjs";
import { createCheckpointSaver } from "./checkpoint/index.mjs";
import { createWorkflowRegistry } from "./workflows/registry.mjs";
import { createBaseGraph } from "./graph/base.mjs";

let cachedRuntime;

export function getRuntime() {
  if (cachedRuntime) return cachedRuntime;

  const services = buildServices();
  const checkpointer = createCheckpointSaver(services);
  const registry = createWorkflowRegistry({ services, checkpointer });
  const graph = createBaseGraph({ services, registry, checkpointer });

  cachedRuntime = {
    services,
    checkpointer,
    registry,
    graph,
  };
  return cachedRuntime;
}
