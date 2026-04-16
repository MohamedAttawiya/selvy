import * as cdk from "aws-cdk-lib";
import * as apigatewayv2 from "aws-cdk-lib/aws-apigatewayv2";
import * as integrations from "aws-cdk-lib/aws-apigatewayv2-integrations";
import * as athena from "aws-cdk-lib/aws-athena";
import * as glue from "aws-cdk-lib/aws-glue";
import * as iam from "aws-cdk-lib/aws-iam";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as path from "path";
import { Construct } from "constructs";

/**
 * Add new Andes tables here. Each entry maps a local table name
 * to its source in the BDT catalog (account 277195998886).
 */
export interface AndesTableConfig {
  /** Local table name in the andes database (e.g. "booker.o_reporting_days") */
  localName: string;
  /** Source catalog account */
  catalogId: string;
  /** Source database name */
  databaseName: string;
  /** Source table name */
  tableName: string;
  /** Source region */
  region: string;
}

export interface AndesStackProps extends cdk.StackProps {
  prefix: string;
  tables: AndesTableConfig[];
  userPoolId?: string;
  userPoolClientId?: string;
  userPoolRegion?: string;
}

export class AndesStack extends cdk.Stack {
  public readonly andesAccessRole: iam.Role;
  public readonly resultsBucket: s3.Bucket;
  public readonly queryApiEndpoint: string;

  constructor(scope: Construct, id: string, props: AndesStackProps) {
    super(scope, id, props);

    // Athena results bucket
    this.resultsBucket = new s3.Bucket(this, "AthenaResultsBucket", {
      bucketName: `${props.prefix}-athena-results`,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
      lifecycleRules: [{ expiration: cdk.Duration.days(30) }],
    });

    // Athena workgroup configured with results location
    new athena.CfnWorkGroup(this, "AndesWorkGroup", {
      name: `${props.prefix}-andes`,
      state: "ENABLED",
      workGroupConfiguration: {
        resultConfiguration: {
          outputLocation: `s3://${this.resultsBucket.bucketName}/`,
        },
        enforceWorkGroupConfiguration: true,
      },
    });

    // Glue database for andes resource links
    const andesDb = new glue.CfnDatabase(this, "AndesDatabase", {
      catalogId: this.account,
      databaseInput: { name: "andes" },
    });

    // Create resource link tables dynamically
    for (const t of props.tables) {
      const safeId = t.localName.replace(/[^a-zA-Z0-9]/g, "");
      new glue.CfnTable(this, `Table${safeId}`, {
        catalogId: this.account,
        databaseName: "andes",
        tableInput: {
          name: t.localName,
          targetTable: {
            catalogId: t.catalogId,
            databaseName: t.databaseName,
            name: t.tableName,
            region: t.region,
          },
        },
      }).addDependency(andesDb);
    }

    // Andes data access role
    this.andesAccessRole = new iam.Role(this, "AndesDataAccessRole", {
      roleName: `${props.prefix}-andes-data-access`,
      assumedBy: new iam.CompositePrincipal(
        new iam.ServicePrincipal("lambda.amazonaws.com"),
        new iam.ServicePrincipal("glue.amazonaws.com"),
        new iam.ServicePrincipal("sagemaker.amazonaws.com"),
        new iam.ServicePrincipal("ec2.amazonaws.com"),
      ),
      maxSessionDuration: cdk.Duration.hours(1),
    });

    this.andesAccessRole.addToPolicy(new iam.PolicyStatement({
      sid: "AthenaQueryAccess",
      actions: [
        "athena:StartQueryExecution", "athena:GetQueryExecution",
        "athena:GetQueryResults", "athena:StopQueryExecution", "athena:GetWorkGroup",
      ],
      resources: ["*"],
    }));

    this.andesAccessRole.addToPolicy(new iam.PolicyStatement({
      sid: "GlueCatalogAccess",
      actions: [
        "glue:GetDatabase", "glue:GetDatabases", "glue:GetTable",
        "glue:GetTables", "glue:GetPartition", "glue:GetPartitions",
      ],
      resources: ["*"],
    }));

    this.andesAccessRole.addToPolicy(new iam.PolicyStatement({
      sid: "LakeFormationAccess",
      actions: ["lakeformation:GetDataAccess"],
      resources: ["*"],
    }));

    this.andesAccessRole.addToPolicy(new iam.PolicyStatement({
      sid: "S3ResultsAccess",
      actions: ["s3:GetObject", "s3:ListBucket", "s3:PutObject", "s3:GetBucketLocation", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"],
      resources: [this.resultsBucket.bucketArn, `${this.resultsBucket.bucketArn}/*`],
    }));

    this.andesAccessRole.addToPolicy(new iam.PolicyStatement({
      sid: "RedshiftAccess",
      actions: [
        "redshift:GetClusterCredentialsWithIAM", "redshift:DescribeClusters",
        "redshift-data:ExecuteStatement", "redshift-data:DescribeStatement",
        "redshift-data:GetStatementResult",
      ],
      resources: ["*"],
    }));

    // Query Lambda — runs Athena queries and lists Glue tables
    const queryFn = new lambda.Function(this, "QueryFn", {
      functionName: `${props.prefix}-query`,
      runtime: lambda.Runtime.NODEJS_20_X,
      handler: "index.handler",
      timeout: cdk.Duration.seconds(90),
      memorySize: 256,
      code: lambda.Code.fromAsset(path.join(__dirname, "../../lambda/query")),
      environment: {
        ATHENA_WORKGROUP: `${props.prefix}-andes`,
        GLUE_DATABASE: "andes",
      },
      role: this.andesAccessRole,
    });

    // Add basic Lambda execution permissions to the andes role
    this.andesAccessRole.addManagedPolicy(
      iam.ManagedPolicy.fromAwsManagedPolicyName("service-role/AWSLambdaBasicExecutionRole"),
    );

    const queryApi = new apigatewayv2.HttpApi(this, "QueryApi", {
      apiName: `${props.prefix}-query-api`,
      corsPreflight: {
        allowOrigins: ["*"],
        allowMethods: [
          apigatewayv2.CorsHttpMethod.GET,
          apigatewayv2.CorsHttpMethod.POST,
          apigatewayv2.CorsHttpMethod.OPTIONS,
        ],
        allowHeaders: ["Authorization", "Content-Type"],
      },
    });

    const queryInt = new integrations.HttpLambdaIntegration("QueryInt", queryFn);

    queryApi.addRoutes({
      path: "/query/tables",
      methods: [apigatewayv2.HttpMethod.GET],
      integration: queryInt,
    });

    queryApi.addRoutes({
      path: "/query/start",
      methods: [apigatewayv2.HttpMethod.POST],
      integration: queryInt,
    });

    queryApi.addRoutes({
      path: "/query/status/{queryId}",
      methods: [apigatewayv2.HttpMethod.GET],
      integration: queryInt,
    });

    queryApi.addRoutes({
      path: "/query/results/{queryId}",
      methods: [apigatewayv2.HttpMethod.GET],
      integration: queryInt,
    });

    this.queryApiEndpoint = queryApi.apiEndpoint;

    new cdk.CfnOutput(this, "QueryApiUrl", { value: queryApi.apiEndpoint });
    new cdk.CfnOutput(this, "AndesDataAccessRoleArn", { value: this.andesAccessRole.roleArn });
    new cdk.CfnOutput(this, "AthenaResultsBucketName", { value: this.resultsBucket.bucketName });
    new cdk.CfnOutput(this, "AndesWorkGroupName", { value: `${props.prefix}-andes` });
  }
}
