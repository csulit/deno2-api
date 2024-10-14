let db: Deno.Kv | null = null;

export async function getKvInstance(): Promise<Deno.Kv> {
  if (!db) {
    db = await Deno.openKv();
  }
  return db;
}

export async function sendMessage(arg: {
  kv: Deno.Kv;
  data: unknown;
  options?: {
    delay?: number;
    keysIfUndelivered?: Deno.KvKey[];
    backoffSchedule?: number[];
  };
}) {
  const { kv, data, options } = arg;
  await kv.enqueue(data, options);
}

export async function listenQueue(kv: Deno.Kv) {
  await kv.listenQueue((msg) => {
    console.log(msg);
  });
}

export function closeKvInstance(): void {
  if (db) {
    db.close();
    db = null;
  }
}
