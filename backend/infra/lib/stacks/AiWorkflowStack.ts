import * as cdk from "aws-cdk-lib";
import * as apigatewayv2 from "aws-cdk-lib/aws-apigatewayv2";
import * as integrations from "aws-cdk-lib/aws-apigatewayv2-integrations";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as iam from "aws-cdk-lib/aws-iam";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as secretsmanager from "aws-cdk-lib/aws-secretsmanager";
import * as ssm from "aws-cdk-lib/aws-ssm";
import * as path from "path";
import { Construct } from "constructs";

export interface AiWorkflowStackProps extends cdk.StackProps {
  prefix: string;
}

export class AiWorkflowStack extends cdk.Stack {
  public readonly slackEndpoint: string;

  constructor(scope: Construct, id: string, props: AiWorkflowStackProps) {
    super(scope, id, props);

    // Read from SSM — no cross-stack exports needed
    const metricsTableArn = ssm.StringParameter.valueForStringParameter(this, `/${props.prefix}/data/metrics-table-arn`);
    const slackRequestsTableArn = ssm.StringParameter.valueForStringParameter(this, `/${props.prefix}/data/slack-requests-table-arn`);
    const conversationsTableArn = ssm.StringParameter.valueForStringParameter(this, `/${props.prefix}/data/conversations-table-arn`);
    const checkpointsTableArn = ssm.StringParameter.valueForStringParameter(this, `/${props.prefix}/data/checkpoints-table-arn`);

    const metricsTable = dynamodb.Table.fromTableArn(this, "MetricsTable", metricsTableArn);
    const slackRequestsTable = dynamodb.Table.fromTableArn(this, "SlackRequestsTable", slackRequestsTableArn);
    const conversationsTable = dynamodb.Table.fromTableArn(this, "ConversationsTable", conversationsTableArn);
    const checkpointsTable = dynamodb.Table.fromTableArn(this, "CheckpointsTable", checkpointsTableArn);

    // Secrets Manager secret for Slack credentials
    const slackSecret = new secretsmanager.Secret(this, "SlackSecret", {
      secretName: `${props.prefix}/slack`,
      description: "Slack app credentials for Selvy bot",
      generateSecretString: {
        secretStringTemplate: JSON.stringify({
          SLACK_SIGNING_SECRET: "CHANGE_ME",
          SLACK_BOT_TOKEN: "CHANGE_ME",
          SLACK_APP_ID: "CHANGE_ME",
          SLACK_CLIENT_ID: "CHANGE_ME",
          SLACK_VERIFICATION_TOKEN: "CHANGE_ME",
        }),
        generateStringKey: "_placeholder",
      },
    });

    const slackHandler = new lambda.Function(this, "SlackHandler", {
      functionName: `${props.prefix}-slack-handler`,
      runtime: lambda.Runtime.NODEJS_20_X,
      handler: "index.handler",
      timeout: cdk.Duration.seconds(90),
      memorySize: 256,
      code: lambda.Code.fromAsset(path.join(__dirname, "../../lambda/slack-handler"), {
        exclude: [
          "last*.json",
          "recent*.json",
          "window*.json",
          "read-*.json",
          "write-*.json",
          "req-*.json",
          "orders_analytics_item.json",
          "*.log",
        ],
      }),
      environment: {
        SLACK_REQUESTS_TABLE: slackRequestsTable.tableName,
        CONVERSATIONS_TABLE: conversationsTable.tableName,
        METRICS_TABLE: metricsTable.tableName,
        CHECKPOINTS_TABLE: checkpointsTable.tableName,
        CHECKPOINT_TTL_SECONDS: "2592000",
        BEDROCK_MODEL_ID: "anthropic.claude-3-haiku-20240307-v1:0",
        BEDROCK_EXTRACT_MODEL_ID: "global.anthropic.claude-sonnet-4-6",
        BEDROCK_SUMMARY_MODEL_ID: "global.anthropic.claude-sonnet-4-6",
        BEDROCK_REGION: "us-east-1",
        ATHENA_WORKGROUP: `${props.prefix}-andes`,
        GLUE_DATABASE: "andes",
        ATHENA_RESULTS_BUCKET: `${props.prefix}-athena-results`,
        ATHENA_REGION: "us-east-1",
        ATHENA_QUERY_LAMBDA_NAME: `${props.prefix}-query`,
        BEDROCK_EMBED_MODEL_ID: "amazon.titan-embed-text-v2:0",
        BEDROCK_EMBED_DIMENSIONS: "256",
        SSM_PREFIX: props.prefix,
        OPENSEARCH_REGION: this.region,
        SLACK_SECRET_ARN: slackSecret.secretArn,
      },
    });

    slackSecret.grantRead(slackHandler);
    slackRequestsTable.grantReadWriteData(slackHandler);
    conversationsTable.grantReadWriteData(slackHandler);
    metricsTable.grantReadData(slackHandler);
    checkpointsTable.grantReadWriteData(slackHandler);

    const osEndpointParamArn = cdk.Stack.of(this).formatArn({
      service: "ssm",
      resource: "parameter",
      resourceName: `${props.prefix}/search/opensearch-endpoint`,
    });
    const osIndexParamArn = cdk.Stack.of(this).formatArn({
      service: "ssm",
      resource: "parameter",
      resourceName: `${props.prefix}/search/metrics-index`,
    });
    slackHandler.addToRolePolicy(new iam.PolicyStatement({
      actions: ["ssm:GetParameter"],
      resources: [osEndpointParamArn, osIndexParamArn],
    }));

    // Bedrock invoke permission
    slackHandler.addToRolePolicy(new iam.PolicyStatement({
      actions: ["bedrock:InvokeModel"],
      resources: ["*"],
    }));

    slackHandler.addToRolePolicy(new iam.PolicyStatement({
      actions: ["aoss:APIAccessAll"],
      resources: ["*"],
    }));

    slackHandler.addToRolePolicy(new iam.PolicyStatement({
      actions: [
        "athena:StartQueryExecution",
        "athena:GetQueryExecution",
        "athena:GetQueryResults",
        "athena:StopQueryExecution",
        "athena:GetWorkGroup",
      ],
      resources: ["*"],
    }));

    slackHandler.addToRolePolicy(new iam.PolicyStatement({
      actions: [
        "glue:GetDatabase",
        "glue:GetDatabases",
        "glue:GetTable",
        "glue:GetTables",
        "glue:GetPartition",
        "glue:GetPartitions",
      ],
      resources: ["*"],
    }));

    slackHandler.addToRolePolicy(new iam.PolicyStatement({
      actions: ["lakeformation:GetDataAccess"],
      resources: ["*"],
    }));

    slackHandler.addToRolePolicy(new iam.PolicyStatement({
      actions: ["s3:GetObject", "s3:PutObject", "s3:ListBucket", "s3:GetBucketLocation"],
      resources: [
        `arn:aws:s3:::${props.prefix}-athena-results`,
        `arn:aws:s3:::${props.prefix}-athena-results/*`,
      ],
    }));

    // Self-invoke for async processing
    slackHandler.addToRolePolicy(new iam.PolicyStatement({
      actions: ["lambda:InvokeFunction"],
      resources: [
        `arn:aws:lambda:${this.region}:${this.account}:function:${props.prefix}-slack-handler`,
        `arn:aws:lambda:us-east-1:${this.account}:function:${props.prefix}-query`,
      ],
    }));

    new ssm.StringParameter(this, "SsmSlackHandlerRoleArn", {
      parameterName: `/${props.prefix}/iam/slack-handler-role-arn`,
      stringValue: slackHandler.role?.roleArn || "",
    });

    const httpApi = new apigatewayv2.HttpApi(this, "SlackApi", {
      apiName: `${props.prefix}-slack-api`,
      corsPreflight: {
        allowOrigins: ["*"],
        allowMethods: [apigatewayv2.CorsHttpMethod.POST],
        allowHeaders: ["Content-Type"],
      },
    });

    const slackIntegration = new integrations.HttpLambdaIntegration("SlackInt", slackHandler);

    httpApi.addRoutes({
      path: "/slack/events",
      methods: [apigatewayv2.HttpMethod.POST],
      integration: slackIntegration,
    });

    this.slackEndpoint = `${httpApi.apiEndpoint}/slack/events`;

    new cdk.CfnOutput(this, "SlackEndpoint", { value: this.slackEndpoint });
    new cdk.CfnOutput(this, "SlackApiUrl", { value: httpApi.apiEndpoint });
  }
}
