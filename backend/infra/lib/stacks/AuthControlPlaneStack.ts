import * as cdk from "aws-cdk-lib";
import * as cognito from "aws-cdk-lib/aws-cognito";
import * as ssm from "aws-cdk-lib/aws-ssm";
import { Construct } from "constructs";

export interface AuthControlPlaneStackProps extends cdk.StackProps {
  prefix: string;
  callbackUrls: string[];
  logoutUrls: string[];
}

export class AuthControlPlaneStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: AuthControlPlaneStackProps) {
    super(scope, id, props);

    const userPool = new cognito.UserPool(this, "UserPool", {
      userPoolName: `${props.prefix}-users`,
      signInAliases: { email: true },
      selfSignUpEnabled: false,
      standardAttributes: {
        email: { required: true, mutable: true },
        givenName: { required: true, mutable: true },
        familyName: { required: true, mutable: true },
      },
      passwordPolicy: {
        minLength: 8,
        requireLowercase: true,
        requireUppercase: true,
        requireDigits: true,
        requireSymbols: true,
      },
      accountRecovery: cognito.AccountRecovery.EMAIL_ONLY,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    new cognito.CfnUserPoolGroup(this, "UserGroup", {
      userPoolId: userPool.userPoolId,
      groupName: "User",
      description: "Regular user access",
      precedence: 10,
    });

    userPool.addDomain("CognitoDomain", {
      cognitoDomain: { domainPrefix: props.prefix },
    });

    const client = new cognito.UserPoolClient(this, "AppClient", {
      userPool,
      userPoolClientName: `${props.prefix}-app`,
      generateSecret: false,
      oAuth: {
        flows: { authorizationCodeGrant: true },
        scopes: [cognito.OAuthScope.OPENID, cognito.OAuthScope.EMAIL, cognito.OAuthScope.PROFILE],
        callbackUrls: props.callbackUrls,
        logoutUrls: props.logoutUrls,
      },
      supportedIdentityProviders: [cognito.UserPoolClientIdentityProvider.COGNITO],
    });

    const cognitoDomain = `${props.prefix}.auth.${this.region}.amazoncognito.com`;

    // Publish to SSM for decoupled cross-stack lookups
    new ssm.StringParameter(this, "SsmUserPoolId", {
      parameterName: `/${props.prefix}/auth/user-pool-id`,
      stringValue: userPool.userPoolId,
    });
    new ssm.StringParameter(this, "SsmUserPoolClientId", {
      parameterName: `/${props.prefix}/auth/user-pool-client-id`,
      stringValue: client.userPoolClientId,
    });

    new cdk.CfnOutput(this, "UserPoolId", { value: userPool.userPoolId });
    new cdk.CfnOutput(this, "UserPoolClientId", { value: client.userPoolClientId });
    new cdk.CfnOutput(this, "CognitoDomain", { value: cognitoDomain });
  }
}
