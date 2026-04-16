# IAM Export (selvy-dev)

## Primary Athena data-access role
- Name: selvy-dev-andes-data-access
- Used by Lambda: selvy-dev-query (us-east-1)
- Attached managed policy: arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole
- Inline policy name: AndesDataAccessRoleDefaultPolicy5AFE4BE9

Files:
- andes-data-access-trust.json
- andes-data-access-inline-policy.json

## Slack handler execution role (invokes query lambda)
- Name: selvy-dev-ai-workflow-SlackHandlerServiceRoleFAFA5E-g0el6cyUtJxi
- Attached managed policy: arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole
- Inline policy name: SlackHandlerServiceRoleDefaultPolicy5548A3C1

Files:
- slack-handler-trust.json
- slack-handler-inline-policy-template.json

## Notes
- Replace placeholders in template files: <prefix>, <account-id>, <region>
- For strict least-privilege, Athena table access can be isolated to only the query role.
