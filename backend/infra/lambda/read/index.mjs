import { DynamoDBClient, ScanCommand, GetItemCommand } from "@aws-sdk/client-dynamodb";
import { unmarshall } from "@aws-sdk/util-dynamodb";

const ddb = new DynamoDBClient();
const TABLES = {
  metrics: process.env.METRICS_TABLE,
};

export const handler = async (event) => {
  const { rawPath, pathParameters } = event;
  const resource = rawPath.split("/")[1]; // "metrics"
  const tableName = TABLES[resource];

  if (!tableName) return { statusCode: 404, body: JSON.stringify({ error: "Not found" }) };

  try {
    if (pathParameters?.id) {
      const { Item } = await ddb.send(new GetItemCommand({
        TableName: tableName,
        Key: { id: { S: pathParameters.id } },
      }));
      if (!Item) return { statusCode: 404, body: JSON.stringify({ error: "Not found" }) };
      return { statusCode: 200, body: JSON.stringify(unmarshall(Item)) };
    }

    const { Items = [] } = await ddb.send(new ScanCommand({ TableName: tableName }));
    return { statusCode: 200, body: JSON.stringify(Items.map(unmarshall)) };
  } catch (err) {
    return { statusCode: 500, body: JSON.stringify({ error: err.message }) };
  }
};
