import "jsr:@std/dotenv/load";
import { type Context, Hono } from "jsr:@hono/hono";

import { runQuery } from "./config/postgres.ts";
import { getKvInstance, listenQueue, sendMessage } from "./config/deno-kv.ts";

const app = new Hono();
const kv = await getKvInstance();

app.get("/", async (c: Context) => {
  const postgres = await runQuery(`
            SELECT
                l.id AS listing_id,
                l.title,
                l.url,
                l.project_name,
                l.description,
                l.is_scraped,
                l.address AS listing_address,
                l.price,
                l.price_formatted,
                p.id AS property_id,
                p.user_id,
                p.floor_size,
                p.lot_size,
                p.building_size,
                p.ceiling_height,
                p.no_of_bedrooms,
                p.no_of_bathrooms,
                p.no_of_parking_spaces,
                p.longitude,
                p.latitude,
                p.year_built,
                p.primary_image_url,
                p.images,
                p.amenities,
                p.property_features,
                p.indoor_features,
                p.outdoor_features,
                p.ai_generated_description,
                p.ai_generated_basic_features,
                pt.type_name AS property_type_name,
                wt.type_name AS warehouse_type_name,
                p.address AS property_address,
                rg.region AS listing_region_name,
                ct.city AS listing_city_name,
                ar.area AS listing_area_name,
                p.created_at AS property_created_at,
                p.updated_at AS property_updated_at,
                l.created_at AS listing_created_at,
                l.updated_at AS listing_updated_at,
                -- Price change log as an array ordered by latest changes
                (
                    SELECT json_agg(
                        json_build_object(
                            'id', pcl.id,
                            'old_price', pcl.old_price,
                            'new_price', pcl.new_price,
                            'change_timestamp', pcl.change_timestamp
                        ) ORDER BY pcl.change_timestamp DESC
                    )
                    FROM Price_Change_Log pcl
                    WHERE pcl.listing_id = l.id
                ) AS price_change_log
            FROM
                Listing l
                JOIN Property p ON l.property_id = p.id
                LEFT JOIN Property_Type pt ON p.property_type_id = pt.property_type_id
                LEFT JOIN Warehouse_Type wt ON p.warehouse_type_id = wt.warehouse_type_id
                LEFT JOIN Listing_Region rg ON p.listing_region_id = rg.id
                LEFT JOIN Listing_City ct ON p.listing_city_id = ct.id
                LEFT JOIN Listing_Area ar ON p.listing_area_id = ar.id
            ORDER BY l.id DESC LIMIT 50;
    `);
  return c.json(postgres.rows);
});

app.post("/", async (c: Context) => {
  const data = await c.req.json();
  await sendMessage({ kv, data, options: { delay: 3000 } });
  return c.text("Hono!");
});

listenQueue(kv).catch((error) => console.error(error));

Deno.serve({ port: parseInt(Deno.env.get("PORT") || "8000") }, app.fetch);
