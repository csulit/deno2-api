import { Pool, type QueryArguments } from "postgres";

const POOL_CONNECTIONS = 20;

export const dbPool = new Pool(Deno.env.get("DATABASE_URL"), POOL_CONNECTIONS);

export async function runQueryObject(query: string, args?: QueryArguments) {
  using client = await dbPool.connect();
  return await client.queryObject(query, args);
}

export async function runQueryArray(query: string, args?: QueryArguments) {
  using client = await dbPool.connect();
  return await client.queryArray(query, args);
}

export default dbPool;
