import { Pool } from "jsr:@wok/pg-driver";

const POOL_CONNECTIONS = 20;
const config = {
  database: Deno.env.get("DB_NAME"),
  hostname: Deno.env.get("DB_HOST"),
  password: Deno.env.get("DB_PASSWORD"),
  port: parseInt(Deno.env.get("DB_PORT") || "", 10),
  user: Deno.env.get("DB_USER"),
};

export const dbPool = new Pool(config, POOL_CONNECTIONS);

export async function runQuery(query: string) {
  using client = await dbPool.connect();
  return await client.queryObject(query);
}

export default dbPool;
