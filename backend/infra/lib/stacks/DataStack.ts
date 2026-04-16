import * as cdk from "aws-cdk-lib";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as ssm from "aws-cdk-lib/aws-ssm";
import { Construct } from "constructs";

export interface DataStackProps extends cdk.StackProps {
  prefix: string;
}

export class DataStack extends cdk.Stack {
  // Legacy exports kept so CloudFormation doesn't fail while dependent stacks still import them.
  // Remove these + the old tables once api/ai-workflow are deployed with SSM lookups.
  public readonly capabilitiesTable: dynamodb.Table;
  public readonly categoriesTable: dynamodb.Table;

  constructor(scope: Construct, id: string, props: DataStackProps) {
    super(scope, id, props);

    const metricsTable = new dynamodb.Table(this, "MetricsTable", {
      tableName: `${props.prefix}-metrics`,
      partitionKey: { name: "id", type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const slackRequestsTable = new dynamodb.Table(this, "SlackRequestsTable", {
      tableName: `${props.prefix}-slack-requests`,
      partitionKey: { name: "id", type: dynamodb.AttributeType.STRING },
      sortKey: { name: "timestamp", type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const conversationsTable = new dynamodb.Table(this, "ConversationsTable", {
      tableName: `${props.prefix}-conversations`,
      partitionKey: { name: "conversation_id", type: dynamodb.AttributeType.STRING },
      sortKey: { name: "message_ts", type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const checkpointsTable = new dynamodb.Table(this, "CheckpointsTable", {
      tableName: `${props.prefix}-checkpoints`,
      partitionKey: { name: "thread_id", type: dynamodb.AttributeType.STRING },
      sortKey: { name: "entry_key", type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      timeToLiveAttribute: "expires_at",
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    // ── Legacy tables (keep until dependent stacks migrate to SSM) ──
    this.capabilitiesTable = new dynamodb.Table(this, "CapabilitiesTable", {
      tableName: `${props.prefix}-capabilities`,
      partitionKey: { name: "id", type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });
    this.categoriesTable = new dynamodb.Table(this, "CategoriesTable", {
      tableName: `${props.prefix}-categories`,
      partitionKey: { name: "id", type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });
    new cdk.CfnOutput(this, "CapabilitiesTableName", { value: this.capabilitiesTable.tableName });
    new cdk.CfnOutput(this, "CategoriesTableName", { value: this.categoriesTable.tableName });

    // ── SSM params for decoupled cross-stack lookups ──
    new ssm.StringParameter(this, "SsmMetricsTableName", {
      parameterName: `/${props.prefix}/data/metrics-table-name`,
      stringValue: metricsTable.tableName,
    });
    new ssm.StringParameter(this, "SsmMetricsTableArn", {
      parameterName: `/${props.prefix}/data/metrics-table-arn`,
      stringValue: metricsTable.tableArn,
    });
    new ssm.StringParameter(this, "SsmSlackRequestsTableName", {
      parameterName: `/${props.prefix}/data/slack-requests-table-name`,
      stringValue: slackRequestsTable.tableName,
    });
    new ssm.StringParameter(this, "SsmSlackRequestsTableArn", {
      parameterName: `/${props.prefix}/data/slack-requests-table-arn`,
      stringValue: slackRequestsTable.tableArn,
    });
    new ssm.StringParameter(this, "SsmConversationsTableName", {
      parameterName: `/${props.prefix}/data/conversations-table-name`,
      stringValue: conversationsTable.tableName,
    });
    new ssm.StringParameter(this, "SsmConversationsTableArn", {
      parameterName: `/${props.prefix}/data/conversations-table-arn`,
      stringValue: conversationsTable.tableArn,
    });
    new ssm.StringParameter(this, "SsmCheckpointsTableName", {
      parameterName: `/${props.prefix}/data/checkpoints-table-name`,
      stringValue: checkpointsTable.tableName,
    });
    new ssm.StringParameter(this, "SsmCheckpointsTableArn", {
      parameterName: `/${props.prefix}/data/checkpoints-table-arn`,
      stringValue: checkpointsTable.tableArn,
    });

    new cdk.CfnOutput(this, "MetricsTableName", { value: metricsTable.tableName });
    new cdk.CfnOutput(this, "SlackRequestsTableName", { value: slackRequestsTable.tableName });
    new cdk.CfnOutput(this, "ConversationsTableName", { value: conversationsTable.tableName });
    new cdk.CfnOutput(this, "CheckpointsTableName", { value: checkpointsTable.tableName });
  }
}
