import "jsr:@std/dotenv/load";
import { type Context, Hono } from "jsr:@hono/hono";

import { runQuery } from "./config/postgres.ts";
import { getKvInstance, listenQueue, sendMessage } from "./config/deno-kv.ts";

const app = new Hono();
const kv = await getKvInstance();

app.get("/", async (c: Context) => {
  const postgres = await runQuery("SELECT * FROM property LIMIT 10");
  await sendMessage({ kv, data: postgres, options: { delay: 5000 } });
  return c.json(postgres.rows);
});

app.post("/", async (c: Context) => {
  const data = await c.req.json();
  await sendMessage({ kv, data, options: { delay: 3000 } });
  return c.text("Hono!");
});

listenQueue(kv).catch((error) => console.error(error));

Deno.serve({ port: parseInt(Deno.env.get("PORT") || "8000") }, app.fetch);
