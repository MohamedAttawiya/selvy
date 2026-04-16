import * as cdk from "aws-cdk-lib";
import * as apigatewayv2 from "aws-cdk-lib/aws-apigatewayv2";
import * as authorizers from "aws-cdk-lib/aws-apigatewayv2-authorizers";
import * as integrations from "aws-cdk-lib/aws-apigatewayv2-integrations";
import * as cognito from "aws-cdk-lib/aws-cognito";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as iam from "aws-cdk-lib/aws-iam";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as ssm from "aws-cdk-lib/aws-ssm";
import * as path from "path";
import { Construct } from "constructs";

export interface ApiStackProps extends cdk.StackProps {
  prefix: string;
}

export class ApiStack extends cdk.Stack {
  public readonly httpApi: apigatewayv2.HttpApi;

  constructor(scope: Construct, id: string, props: ApiStackProps) {
    super(scope, id, props);

    // Read from SSM — no cross-stack exports needed
    const userPoolId = ssm.StringParameter.valueForStringParameter(this, `/${props.prefix}/auth/user-pool-id`);
    const userPoolClientId = ssm.StringParameter.valueForStringParameter(this, `/${props.prefix}/auth/user-pool-client-id`);
    const metricsTableArn = ssm.StringParameter.valueForStringParameter(this, `/${props.prefix}/data/metrics-table-arn`);

    const userPool = cognito.UserPool.fromUserPoolId(this, "UserPool", userPoolId);
    const userPoolClient = cognito.UserPoolClient.fromUserPoolClientId(this, "UserPoolClient", userPoolClientId);
    const metricsTable = dynamodb.Table.fromTableArn(this, "MetricsTable", metricsTableArn);

    const authorizer = new authorizers.HttpUserPoolAuthorizer("CognitoAuthorizer", userPool, {
      userPoolClients: [userPoolClient],
    });

    this.httpApi = new apigatewayv2.HttpApi(this, "Api", {
      apiName: `${props.prefix}-api`,
      corsPreflight: {
        allowOrigins: ["*"],
        allowMethods: [
          apigatewayv2.CorsHttpMethod.GET,
          apigatewayv2.CorsHttpMethod.POST,
          apigatewayv2.CorsHttpMethod.PUT,
          apigatewayv2.CorsHttpMethod.DELETE,
        ],
        allowHeaders: ["Authorization", "Content-Type"],
      },
    });

    const tableEnv = {
      METRICS_TABLE: metricsTable.tableName,
    };

    // Read Lambda
    const readFn = new lambda.Function(this, "ReadFn", {
      functionName: `${props.prefix}-read`,
      runtime: lambda.Runtime.NODEJS_20_X,
      handler: "index.handler",
      timeout: cdk.Duration.seconds(10),
      memorySize: 128,
      code: lambda.Code.fromAsset(path.join(__dirname, "../../lambda/read")),
      environment: tableEnv,
    });
    metricsTable.grantReadData(readFn);

    // Write Lambda
    const writeFn = new lambda.Function(this, "WriteFn", {
      functionName: `${props.prefix}-write`,
      runtime: lambda.Runtime.NODEJS_20_X,
      handler: "index.handler",
      timeout: cdk.Duration.seconds(30),
      memorySize: 256,
      code: lambda.Code.fromAsset(path.join(__dirname, "../../lambda/write")),
      environment: {
        ...tableEnv,
        SSM_PREFIX: props.prefix,
        OPENSEARCH_REGION: this.region,
        BEDROCK_EMBED_MODEL_ID: "amazon.titan-embed-text-v2:0",
        BEDROCK_EMBED_DIMENSIONS: "256",
        BEDROCK_REGION: "us-east-1",
      },
    });
    metricsTable.grantReadWriteData(writeFn);

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
    writeFn.addToRolePolicy(new iam.PolicyStatement({
      actions: ["ssm:GetParameter"],
      resources: [osEndpointParamArn, osIndexParamArn],
    }));

    writeFn.addToRolePolicy(new iam.PolicyStatement({
      actions: ["bedrock:InvokeModel"],
      resources: ["*"],
    }));

    writeFn.addToRolePolicy(new iam.PolicyStatement({
      actions: ["aoss:APIAccessAll"],
      resources: ["*"],
    }));

    new ssm.StringParameter(this, "SsmWriteRoleArn", {
      parameterName: `/${props.prefix}/iam/write-role-arn`,
      stringValue: writeFn.role?.roleArn || "",
    });

    const readInt = new integrations.HttpLambdaIntegration("ReadInt", readFn);
    const writeInt = new integrations.HttpLambdaIntegration("WriteInt", writeFn);

    // Read routes
    this.httpApi.addRoutes({ path: "/metrics", methods: [apigatewayv2.HttpMethod.GET], integration: readInt, authorizer });
    this.httpApi.addRoutes({ path: "/metrics/{id}", methods: [apigatewayv2.HttpMethod.GET], integration: readInt, authorizer });

    // Write routes
    this.httpApi.addRoutes({ path: "/metrics", methods: [apigatewayv2.HttpMethod.POST], integration: writeInt, authorizer });
    this.httpApi.addRoutes({ path: "/metrics/{id}", methods: [apigatewayv2.HttpMethod.PUT, apigatewayv2.HttpMethod.DELETE], integration: writeInt, authorizer });

    new cdk.CfnOutput(this, "ApiUrl", { value: this.httpApi.apiEndpoint });
  }
}
