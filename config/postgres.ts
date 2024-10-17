import { Pool } from "postgres";

const POOL_CONNECTIONS = 20;

export const dbPool = new Pool(Deno.env.get("DATABASE_URL"), POOL_CONNECTIONS);

export async function runQuery(query: string) {
  using client = await dbPool.connect();
  return await client.queryObject(query);
}

export default dbPool;
