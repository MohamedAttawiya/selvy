#!/usr/bin/env node
import * as cdk from "aws-cdk-lib";
import { AuthControlPlaneStack } from "../lib/stacks/AuthControlPlaneStack";
import { DataStack } from "../lib/stacks/DataStack";
import { ApiStack } from "../lib/stacks/ApiStack";
import { FrontendEdgeStack } from "../lib/stacks/FrontendEdgeStack";
import { BdtGlueStack } from "../lib/stacks/BdtGlueStack";
import { AndesStack, AndesTableConfig } from "../lib/stacks/AndesStack";
import { AiWorkflowStack } from "../lib/stacks/AiWorkflowStack";
import { SearchStack } from "../lib/stacks/SearchStack";

const app = new cdk.App();

const CFG_STAGE = "dev";
const CFG_APP_NAME = "selvy";
const CFG_PRIMARY_REGION = "eu-central-1";
const CFG_EDGE_REGION = "us-east-1";
const BDT_CATALOG_ID = "277195998886";

const STAGE = String(
  app.node.tryGetContext("stage") ?? process.env.STAGE ?? CFG_STAGE
).toLowerCase();
const PREFIX = `${CFG_APP_NAME}-${STAGE}`;

const ACCOUNT =
  process.env.CDK_DEFAULT_ACCOUNT ??
  process.env.AWS_ACCOUNT_ID ??
  String(app.node.tryGetContext("accountId") ?? "457184123492");

const envPrimary = { account: ACCOUNT, region: CFG_PRIMARY_REGION };
const envUsEast1 = { account: ACCOUNT, region: CFG_EDGE_REGION };

// ─── Andes tables ──────────────────────────────────────
const ANDES_TABLES: AndesTableConfig[] = [
  { localName: "booker.o_reporting_days", catalogId: BDT_CATALOG_ID, databaseName: "bdt_view_booker", tableName: "o_reporting_days-version-6", region: "us-east-1" },
  { localName: "ufg_mena_bi.anow_asin_swoos_daily_snapshot", catalogId: BDT_CATALOG_ID, databaseName: "bdt_view_ufg_mena_bi", tableName: "anow_asin_swoos_daily_snapshot-version-1", region: "us-east-1" },
  { localName: "ufg_mena_bi.anow_mena_search_performance", catalogId: BDT_CATALOG_ID, databaseName: "bdt_view_ufg_mena_bi", tableName: "anow_mena_search_performance-version-3", region: "us-east-1" },
  { localName: "ufg_mena_bi.anow_nib_store_closures_order_loss", catalogId: BDT_CATALOG_ID, databaseName: "bdt_view_ufg_mena_bi", tableName: "anow_nib_store_closures_order_loss-version-1", region: "us-east-1" },
  { localName: "ufg_mena_bi.anow_orders_master", catalogId: BDT_CATALOG_ID, databaseName: "bdt_view_ufg_mena_bi", tableName: "anow_orders_master-version-8", region: "us-east-1" },
];

// ─── Stacks ────────────────────────────────────────────
// Auth publishes user-pool-id and client-id to SSM
const auth = new AuthControlPlaneStack(app, `${PREFIX}-auth-control-plane`, {
  env: envPrimary,
  prefix: PREFIX,
  callbackUrls: ["http://localhost:3000/", "https://d1ylngzuip94i3.cloudfront.net/"],
  logoutUrls: ["http://localhost:3000/", "https://d1ylngzuip94i3.cloudfront.net/"],
});

// Data publishes table names + ARNs to SSM
const data = new DataStack(app, `${PREFIX}-data`, {
  env: envPrimary,
  prefix: PREFIX,
});

// Api reads auth + data from SSM — no direct cross-stack refs
const api = new ApiStack(app, `${PREFIX}-api`, {
  env: envPrimary,
  prefix: PREFIX,
});
api.addDependency(auth);
api.addDependency(data);

const frontend = new FrontendEdgeStack(app, `${PREFIX}-frontend-edge`, {
  env: envUsEast1,
  prefix: PREFIX,
});

new BdtGlueStack(app, "Selvy-dev", {
  env: envUsEast1,
  prefix: PREFIX,
});

new AndesStack(app, `${PREFIX}-andes`, {
  env: envUsEast1,
  prefix: PREFIX,
  tables: ANDES_TABLES,
});

// AiWorkflow reads data table refs from SSM — no direct cross-stack refs
const aiWorkflow = new AiWorkflowStack(app, `${PREFIX}-ai-workflow`, {
  env: envPrimary,
  prefix: PREFIX,
});
aiWorkflow.addDependency(data);

const search = new SearchStack(app, `${PREFIX}-search`, {
  env: envPrimary,
  prefix: PREFIX,
});
search.addDependency(api);
search.addDependency(aiWorkflow);
