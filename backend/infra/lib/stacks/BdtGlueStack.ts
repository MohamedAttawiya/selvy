import * as cdk from "aws-cdk-lib";
import * as iam from "aws-cdk-lib/aws-iam";
import * as s3 from "aws-cdk-lib/aws-s3";
import { Construct } from "constructs";

export interface BdtGlueStackProps extends cdk.StackProps {
  prefix: string;
}

export class BdtGlueStack extends cdk.Stack {
  public readonly bdtGlueRole: iam.Role;
  public readonly bdtTssGlueServiceRole: iam.Role;

  constructor(scope: Construct, id: string, props: BdtGlueStackProps) {
    super(scope, id, props);

    // S3 bucket for BDT Glue
    const bucket = new s3.Bucket(this, "BdtGlueBucket", {
      removalPolicy: cdk.RemovalPolicy.RETAIN,
    });
    (bucket.node.defaultChild as cdk.CfnResource).overrideLogicalId("BdtGlueBucket");

    // BDT Glue Role — assumed by Redshift
    this.bdtGlueRole = new iam.Role(this, "BdtGlueRole", {
      roleName: `bdt-glue-role-${this.region}`,
      assumedBy: new iam.ServicePrincipal("redshift.amazonaws.com"),
    });
    (this.bdtGlueRole.node.defaultChild as cdk.CfnResource).overrideLogicalId("BdtGlueRole");

    this.bdtGlueRole.addToPolicy(new iam.PolicyStatement({
      actions: [
        "glue:CreateDatabase", "glue:DeleteDatabase", "glue:GetDatabase", "glue:GetDatabases", "glue:UpdateDatabase",
        "glue:CreateTable", "glue:DeleteTable", "glue:BatchDeleteTable", "glue:UpdateTable", "glue:GetTable", "glue:GetTables",
        "glue:BatchCreatePartition", "glue:CreatePartition", "glue:DeletePartition", "glue:BatchDeletePartition",
        "glue:UpdatePartition", "glue:GetPartition", "glue:GetPartitions", "glue:BatchGetPartition",
      ],
      resources: ["*"],
    }));
    this.bdtGlueRole.addToPolicy(new iam.PolicyStatement({
      actions: ["lakeformation:GetDataAccess"],
      resources: ["*"],
    }));
    this.bdtGlueRole.addToPolicy(new iam.PolicyStatement({
      actions: ["redshift:GetClusterCredentialsV2", "redshift:GetClusterCredentialsWithIAM"],
      resources: ["*"],
    }));

    // Override the policy logical ID to match original
    const glueRolePolicy = this.bdtGlueRole.node.findChild("DefaultPolicy").node.defaultChild as cdk.CfnResource;
    glueRolePolicy.overrideLogicalId("BdtGlueRolePolicy");
    glueRolePolicy.addPropertyOverride("PolicyName", "bdt-glue-role-policy");

    // BDT TSS Glue Service Role — assumed by external Andes accounts
    this.bdtTssGlueServiceRole = new iam.Role(this, "BdtTssGlueServiceRole", {
      roleName: `bdt-tss-glue-service-role-${this.region}`,
      assumedBy: new iam.CompositePrincipal(
        new iam.AccountPrincipal("853632044513"),
        new iam.AccountPrincipal("981675180736"),
      ),
    });
    (this.bdtTssGlueServiceRole.node.defaultChild as cdk.CfnResource).overrideLogicalId("BdtTssGlueServiceRole");

    // Main policy
    this.bdtTssGlueServiceRole.addToPolicy(new iam.PolicyStatement({
      actions: [
        "lakeformation:BatchGrantPermissions", "lakeformation:BatchRevokePermissions",
        "lakeformation:DeregisterResource", "lakeformation:DescribeResource",
        "lakeformation:GetDataAccess", "lakeformation:GetDataLakeSettings",
        "lakeformation:GetEffectivePermissionsForPath", "lakeformation:GrantPermissions",
        "lakeformation:ListPermissions", "lakeformation:ListResources",
        "lakeformation:RegisterResource", "lakeformation:RevokePermissions",
        "lakeformation:UpdateResource",
        "ram:AcceptResourceShareInvitation", "ram:GetResourceShareInvitations", "ram:ListPendingInvitationResources",
      ],
      resources: ["*"],
    }));
    this.bdtTssGlueServiceRole.addToPolicy(new iam.PolicyStatement({
      actions: [
        "kms:GenerateDataKey*", "kms:CreateKey*", "kms:CreateAlias", "kms:DescribeKey",
        "glue:CreateDatabase", "glue:DeleteDatabase", "glue:GetDatabase", "glue:GetDatabases", "glue:UpdateDatabase",
        "glue:CreateTable", "glue:DeleteTable", "glue:BatchDeleteTable", "glue:UpdateTable", "glue:GetTable", "glue:GetTables",
        "glue:BatchCreatePartition", "glue:CreatePartition", "glue:DeletePartition", "glue:BatchDeletePartition",
        "glue:UpdatePartition", "glue:GetPartition", "glue:GetPartitions", "glue:BatchGetPartition",
        "redshift:DescribeClusters", "redshift:ModifyClusterIamRoles",
      ],
      resources: ["*"],
    }));
    this.bdtTssGlueServiceRole.addToPolicy(new iam.PolicyStatement({
      actions: ["s3:*"],
      resources: ["arn:aws:s3:::/*"],
    }));
    this.bdtTssGlueServiceRole.addToPolicy(new iam.PolicyStatement({
      actions: ["s3:ListBucket"],
      resources: ["arn:aws:s3:::"],
    }));
    this.bdtTssGlueServiceRole.addToPolicy(new iam.PolicyStatement({
      actions: [
        "redshift:RejectDataShare", "redshift:AuthorizeDataShare", "redshift:DeauthorizeDataShare",
        "redshift:AssociateDataShareConsumer", "redshift:DisassociateDataShareConsumer",
      ],
      resources: ["arn:aws:redshift:*:*:datashare:*/*"],
    }));
    this.bdtTssGlueServiceRole.addToPolicy(new iam.PolicyStatement({
      actions: ["redshift-data:BatchExecuteStatement", "redshift-data:ExecuteStatement"],
      resources: [
        `arn:aws:redshift:${this.region}:${this.account}:cluster:*`,
        `arn:aws:redshift-serverless:${this.region}:${this.account}:workgroup/*`,
      ],
    }));
    this.bdtTssGlueServiceRole.addToPolicy(new iam.PolicyStatement({
      actions: ["redshift:GetClusterCredentials"],
      resources: [
        `arn:aws:redshift:${this.region}:${this.account}:dbuser:*/*`,
        `arn:aws:redshift:${this.region}:${this.account}:dbname:*/*`,
      ],
    }));
    this.bdtTssGlueServiceRole.addToPolicy(new iam.PolicyStatement({
      actions: [
        "redshift-serverless:GetCredentials", "redshift-serverless:ListWorkgroups", "redshift-serverless:GetNamespace",
        "redshift-data:CancelStatement", "redshift-data:DescribeStatement", "redshift-data:GetStatementResult",
        "redshift-data:ListStatements", "redshift-data:ListDatabases",
        "redshift:DescribeDataShares", "redshift:DescribeDataSharesForProducer", "redshift:DescribeDataSharesForConsumer",
      ],
      resources: ["*"],
    }));
    this.bdtTssGlueServiceRole.addToPolicy(new iam.PolicyStatement({
      actions: ["iam:PassRole", "iam:GetRole", "iam:GetRolePolicy"],
      resources: [this.bdtGlueRole.roleArn],
    }));

    // Override the default policy logical ID to match original "BdtTssGlueServiceRolePolicy"
    const tssRolePolicy = this.bdtTssGlueServiceRole.node.findChild("DefaultPolicy").node.defaultChild as cdk.CfnResource;
    tssRolePolicy.overrideLogicalId("BdtTssGlueServiceRolePolicy");
    tssRolePolicy.addPropertyOverride("PolicyName", "bdt-tss-glue-service-role-policy");

    // Supplemental policy (was a separate resource in original template)
    const supplementalPolicy = new iam.Policy(this, "BdtTssGlueServiceRolePolicyAddition", {
      policyName: "bdt-tss-glue-service-role-policy-tip-supplement",
      roles: [this.bdtTssGlueServiceRole],
      statements: [
        new iam.PolicyStatement({
          sid: "RedshiftServerlessExecuteStatement",
          actions: ["redshift-data:BatchExecuteStatement", "redshift-data:ExecuteStatement"],
          resources: [`arn:aws:redshift-serverless:${this.region}:${this.account}:workgroup/*`],
        }),
        new iam.PolicyStatement({
          sid: "GetRedshiftServerless",
          actions: ["redshift-serverless:GetCredentials", "redshift-serverless:ListWorkgroups", "redshift-serverless:GetNamespace"],
          resources: ["*"],
        }),
        new iam.PolicyStatement({
          sid: "ValidateRedshiftIdCConfig",
          actions: ["redshift:DescribeRedshiftIdcApplications"],
          resources: ["*"],
        }),
        new iam.PolicyStatement({
          sid: "ValidateLFIdCConfig",
          actions: ["lakeformation:DescribeLakeFormationIdentityCenterConfiguration", "sso:DescribeApplication"],
          resources: ["*"],
        }),
        new iam.PolicyStatement({
          sid: "CreateLFIdCConfig",
          actions: [
            "lakeformation:CreateLakeFormationIdentityCenterConfiguration",
            "sso:PutApplicationAssignmentConfiguration", "sso:CreateApplication",
            "sso:PutApplicationAuthenticationMethod", "sso:PutApplicationGrant",
          ],
          resources: ["*"],
        }),
        new iam.PolicyStatement({
          sid: "DeleteLFIdCConfig",
          actions: ["lakeformation:DeleteLakeFormationIdentityCenterConfiguration", "sso:DeleteApplication"],
          resources: ["*"],
        }),
        new iam.PolicyStatement({
          sid: "GrantToIdCInstanceARN",
          actions: ["sso:DescribeInstance"],
          resources: ["*"],
        }),
        new iam.PolicyStatement({
          sid: "ValidateRole",
          actions: ["iam:ListRolePolicies", "iam:GetRolePolicy", "iam:GetRole"],
          resources: [this.bdtTssGlueServiceRole.roleArn],
        }),
      ],
    });
    (supplementalPolicy.node.defaultChild as cdk.CfnResource).overrideLogicalId("BdtTssGlueServiceRolePolicyAddition");
  }
}
