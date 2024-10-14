import "jsr:@std/dotenv/load";
import { type Context, Hono } from "jsr:@hono/hono";

import { runQuery } from "./config/postgres.ts";
import { getKvInstance, sendMessage, listenQueue } from "./config/deno-kv.ts";

const app = new Hono();
const kv = await getKvInstance();

app.get("/", async (c: Context) => {
  const postgres = await runQuery("SELECT * FROM property");
  await sendMessage({ kv, data: postgres, options: { delay: 5000 } });
  return c.text("Hono!");
});

listenQueue(kv).catch((error) => console.error(error));

Deno.serve({ port: 8000 }, app.fetch);
