import * as cdk from "aws-cdk-lib";
import * as opensearchserverless from "aws-cdk-lib/aws-opensearchserverless";
import * as ssm from "aws-cdk-lib/aws-ssm";
import { Construct } from "constructs";

export interface SearchStackProps extends cdk.StackProps {
  prefix: string;
}

export class SearchStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: SearchStackProps) {
    super(scope, id, props);

    const collectionName = `${props.prefix}-search`;
    const indexName = "metrics-variations-v3";

    const encryptionPolicy = new opensearchserverless.CfnSecurityPolicy(this, "SearchEncryptionPolicy", {
      name: `${props.prefix}-search-encryption`,
      type: "encryption",
      policy: JSON.stringify({
        Rules: [
          { ResourceType: "collection", Resource: [`collection/${collectionName}`] },
        ],
        AWSOwnedKey: true,
      }),
    });

    const networkPolicy = new opensearchserverless.CfnSecurityPolicy(this, "SearchNetworkPolicy", {
      name: `${props.prefix}-search-network`,
      type: "network",
      policy: JSON.stringify([
        {
          Rules: [
            { ResourceType: "collection", Resource: [`collection/${collectionName}`] },
            { ResourceType: "dashboard", Resource: [`collection/${collectionName}`] },
          ],
          AllowFromPublic: true,
        },
      ]),
    });

    const collection = new opensearchserverless.CfnCollection(this, "SearchCollection", {
      name: collectionName,
      type: "VECTORSEARCH",
      description: `${props.prefix} vector search collection`,
    });
    collection.addDependency(encryptionPolicy);
    collection.addDependency(networkPolicy);

    const writeRoleArn = ssm.StringParameter.valueForStringParameter(this, `/${props.prefix}/iam/write-role-arn`);
    const slackRoleArn = ssm.StringParameter.valueForStringParameter(this, `/${props.prefix}/iam/slack-handler-role-arn`);

    const collectionResource = `collection/${collectionName}`;
    const indexResource = `index/${collectionName}/*`;

    new opensearchserverless.CfnAccessPolicy(this, "SearchWriteAccessPolicy", {
      name: `${props.prefix}-search-write`,
      type: "data",
      policy: JSON.stringify([
        {
          Rules: [
            {
              ResourceType: "collection",
              Resource: [collectionResource],
              Permission: ["aoss:DescribeCollectionItems"],
            },
            {
              ResourceType: "index",
              Resource: [indexResource],
              Permission: [
                "aoss:CreateIndex",
                "aoss:UpdateIndex",
                "aoss:WriteDocument",
                "aoss:DescribeIndex",
                "aoss:ReadDocument",
                "aoss:DeleteIndex",
              ],
            },
          ],
          Principal: [writeRoleArn],
        },
      ]),
    });

    new opensearchserverless.CfnAccessPolicy(this, "SearchReadAccessPolicy", {
      name: `${props.prefix}-search-read`,
      type: "data",
      policy: JSON.stringify([
        {
          Rules: [
            {
              ResourceType: "collection",
              Resource: [collectionResource],
              Permission: ["aoss:DescribeCollectionItems"],
            },
            {
              ResourceType: "index",
              Resource: [indexResource],
              Permission: ["aoss:ReadDocument", "aoss:DescribeIndex"],
            },
          ],
          Principal: [slackRoleArn],
        },
      ]),
    });

    new ssm.StringParameter(this, "SsmOpenSearchEndpoint", {
      parameterName: `/${props.prefix}/search/opensearch-endpoint`,
      stringValue: collection.attrCollectionEndpoint,
    });

    new ssm.StringParameter(this, "SsmOpenSearchIndex", {
      parameterName: `/${props.prefix}/search/metrics-index`,
      stringValue: indexName,
    });
  }
}
