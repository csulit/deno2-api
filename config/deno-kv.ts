import type { PoolClient, Transaction } from "postgres";
import { dbPool } from "./postgres.ts";
import { openaiAssistant } from "../services/openai-assistant.ts";

export interface RawLamudiData {
  id: number;
  json_data: Record<string, unknown>;
  raw_title: string;
  property_type_id: number;
  offer_type_id: number;
  agent_name: string;
  product_owner_name: string;
  listing_region_id: string;
  region: string;
  listing_city_id: string;
  city: string;
  listing_area: string;
  listing_area_id: string;
  rooms_total: number;
  floor_size: number;
  lot_size: number;
  land_size: number;
  building_size: number;
  no_of_bedrooms: number;
  no_of_bathrooms: number;
  no_of_parking_spaces: number;
  longitude: number;
  latitude: number;
  year_built: number;
  primary_image_url: string;
  indoor_features: Record<string, unknown>;
  outdoor_features: Record<string, unknown>;
  property_features: Record<string, unknown>;
  address: string;
  project_name: string;
  price: string;
  price_formatted: string;
  description: string;
  full_url: string;
  images: {
    src: string;
    type: "preload" | "secondary";
    alt?: string;
    dataSrc?: string;
  }[];
}

export interface Property {
  id: number;
}

export interface Listing {
  id: number;
  title: string;
  url: string;
  project_name: string | null;
  description: string;
  is_scraped: boolean;
  address: string | null;
  price_formatted: string | null;
  price: number;
  offer_type_id: number;
  property_id: number;
  created_at: Date;
  updated_at: Date;
}

let db: Deno.Kv | null = null;

export interface KvMessage {
  type:
    | "CREATE_LISTING_FROM_RAW_LAMUDI_DATA"
    | "CREATE_RAW_LAMUDI_LISTING_DATA"
    | "CREATE_AI_GENERATED_DESCRIPTION";
  source: "LAMUDI" | "APP";
  // deno-lint-ignore no-explicit-any
  data: any;
}

export interface Location {
  listing_area_id?: string;
  area?: string;
  listing_city_id: string;
  city: string;
  listing_region_id: string;
  region: string;
}

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
  await kv.listenQueue(async (msg: KvMessage) => {
    switch (msg.type) {
      case "CREATE_LISTING_FROM_RAW_LAMUDI_DATA":
        {
          let transaction: Transaction | null = null;
          let client: PoolClient | null = null;

          try {
            client = await dbPool.connect();
            using client2 = await dbPool.connect();
            transaction = client.createTransaction(
              "create_listing_from_raw_lamudi_data",
            );

            if (!transaction) {
              throw Error("Transaction not created");
            }

            if (!client) {
              throw Error("Client not created");
            }

            await transaction.begin();

            const rawPropertiesCount = await transaction.queryObject<
              { count: number }
            >({
              text: `
                SELECT COUNT(*)
                FROM lamudi_raw_data
                WHERE is_process = FALSE
              `,
            });

            console.info(
              `Processing ${rawPropertiesCount.rows[0].count} raw properties`,
            );

            const rawProperties = await transaction.queryObject<RawLamudiData>(
              `
              SELECT
                  id,
                  json_data,
                  json_data->'dataLayer'->>'title' AS raw_title,
                  CASE
                      WHEN json_data->'dataLayer'->'attributes'->>'attribute_set_name' = 'Condominium' THEN 1
                      WHEN json_data->'dataLayer'->'attributes'->>'attribute_set_name' = 'House' THEN 2
                      WHEN json_data->'dataLayer'->'attributes'->>'subcategory' = 'Warehouse' THEN 3
                      WHEN json_data->'dataLayer'->'attributes'->>'attribute_set_name' = 'Land' THEN 4
                  END AS property_type_id,
                  CASE
                      WHEN json_data->'dataLayer'->'attributes'->>'offer_type' = 'Buy' THEN 1
                      WHEN json_data->'dataLayer'->'attributes'->>'offer_type' = 'Rent' THEN 2
                  END AS offer_type_id,
                  json_data->'dataLayer'->>'agent_name' AS agent_name,
                  json_data->'dataLayer'->'attributes'->>'product_owner_name' AS product_owner_name,
                  json_data->'dataLayer'->'attributes'->>'listing_region_id' AS listing_region_id,
                  json_data->'dataLayer'->'location'->>'region' AS region,
                  json_data->'dataLayer'->'attributes'->>'listing_city_id' AS listing_city_id,
                  json_data->'dataLayer'->'location'->>'city' AS city,
                  json_data->'dataLayer'->'attributes'->>'listing_area' AS listing_area,
                  json_data->'dataLayer'->'attributes'->>'listing_area_id' AS listing_area_id,
                  COALESCE((json_data->'dataLayer'->'location'->>'rooms_total')::INTEGER, 0) AS rooms_total,
                  COALESCE((json_data->'dataLayer'->'attributes'->>'floor_size')::DOUBLE PRECISION, 0) AS floor_size,
                  COALESCE((json_data->'dataLayer'->'attributes'->>'lot_size')::DOUBLE PRECISION, 0) AS lot_size,
                  COALESCE((json_data->'dataLayer'->'attributes'->>'land_size')::DOUBLE PRECISION, 0) AS land_size,
                  COALESCE((json_data->'dataLayer'->'attributes'->>'building_size')::DOUBLE PRECISION, 0) AS building_size,
                  COALESCE((json_data->'dataLayer'->'attributes'->>'bedrooms')::INTEGER, 0) AS no_of_bedrooms,
                  COALESCE((json_data->'dataLayer'->'attributes'->>'bathrooms')::INTEGER, 0) AS no_of_bathrooms,
                  COALESCE((json_data->'dataLayer'->'attributes'->>'car_spaces')::INTEGER, 0) AS no_of_parking_spaces,
                  (json_data->'dataLayer'->'attributes'->>'location_longitude')::DOUBLE PRECISION AS longitude,
                  (json_data->'dataLayer'->'attributes'->>'location_latitude')::DOUBLE PRECISION AS latitude,
                  (json_data->'dataLayer'->'attributes'->>'year_built')::INTEGER AS year_built,
                  json_data->'dataLayer'->'attributes'->>'image_url' AS primary_image_url,
                  (json_data->'dataLayer'->'attributes'->>'indoor_features')::jsonb AS indoor_features,
                  (json_data->'dataLayer'->'attributes'->>'outdoor_features')::jsonb AS outdoor_features,
                  (json_data->'dataLayer'->'attributes'->>'other_features')::jsonb AS property_features,
                  json_data->'dataLayer'->'attributes'->>'listing_address' AS address,
                  json_data->'dataLayer'->'attributes'->>'project_name' AS project_name,
                  json_data->'dataLayer'->'attributes'->>'price' AS price,
                  json_data->'dataLayer'->'attributes'->>'price_formatted' AS price_formatted,
                  json_data->'dataLayer'->'description'->>'text' AS description,
                  CONCAT('https://lamudi.com.ph/', json_data->'dataLayer'->'attributes'->>'urlkey_details') AS full_url,
                  (json_data->>'images')::jsonb AS images
              FROM lamudi_raw_data
              WHERE is_process = FALSE
                  AND COALESCE((json_data->'dataLayer'->'attributes'->>'price')::INTEGER, 0) > 5000
                  AND json_data->'dataLayer'->'location'->>'region' IS NOT NULL
                  AND json_data->'dataLayer'->'location'->>'city' IS NOT NULL
                  AND json_data->'dataLayer'->'attributes'->>'listing_area' IS NOT NULL
              LIMIT 50
              `,
            );

            for (const rawProperty of rawProperties.rows) {
              try {
                let region = await client2.queryObject(`
                  SELECT id, listing_region_id 
                  FROM Listing_Region 
                  WHERE listing_region_id = '${rawProperty.listing_region_id}'
                `);

                if (region.rowCount === 0) {
                  region = await client2.queryObject({
                    args: [rawProperty.region, rawProperty.listing_region_id],
                    text: `
                      INSERT INTO Listing_Region (region, listing_region_id)
                      VALUES ($1, $2)
                      RETURNING id, listing_region_id
                    `,
                  });
                }

                let city = await client2.queryObject(`
                  SELECT id, listing_city_id
                  FROM Listing_City
                  WHERE listing_city_id = '${rawProperty.listing_city_id}'
                `);

                if (city.rowCount === 0) {
                  const createdRegion = region.rows[0] as {
                    listing_region_id: number;
                  };

                  city = await client2.queryObject({
                    args: [
                      rawProperty.city,
                      rawProperty.listing_city_id,
                      createdRegion.listing_region_id,
                    ],
                    text: `
                      INSERT INTO Listing_City (city, listing_city_id, listing_region_id)
                      VALUES ($1, $2, $3)
                      RETURNING id, listing_city_id
                    `,
                  });
                }

                let area = await client2.queryObject(`
                  SELECT id
                  FROM Listing_Area
                  WHERE listing_area_id = '${rawProperty.listing_area_id}'
                `);

                if (area.rowCount === 0 && rawProperty.listing_area_id) {
                  area = await client2.queryObject({
                    args: [
                      rawProperty.listing_area,
                      rawProperty.listing_area_id,
                    ],
                    text: `
                      INSERT INTO Listing_Area (area, listing_area_id)
                      VALUES ($1, $2)
                      RETURNING id
                    `,
                  });
                }

                // Verify records exist after creation
                const verifyRegion = await client2.queryObject(`
                  SELECT id FROM Listing_Region 
                  WHERE listing_region_id = '${rawProperty.listing_region_id}'
                `);
                const verifyCity = await client2.queryObject(`
                  SELECT id FROM Listing_City
                  WHERE listing_city_id = '${rawProperty.listing_city_id}'
                `);
                const verifyArea = await client2.queryObject(`
                  SELECT id FROM Listing_Area
                  WHERE listing_area_id = '${rawProperty.listing_area_id}'
                `);

                if (
                  !verifyRegion.rowCount || !verifyCity.rowCount ||
                  !verifyArea.rowCount
                ) {
                  throw new Error("Failed to verify created records");
                }

                rawProperty.listing_region_id =
                  (verifyRegion.rows[0] as { id: number }).id
                    .toString();
                rawProperty.listing_city_id =
                  (verifyCity.rows[0] as { id: number })
                    .id
                    .toString();
                rawProperty.listing_area_id =
                  (verifyArea.rows[0] as { id: number })
                    .id
                    .toString();
              } catch (error) {
                throw error;
              }
            }

            if (rawProperties.rowCount && rawProperties.rowCount > 0) {
              for (const rawProperty of rawProperties.rows) {
                const images = rawProperty.images.map((image) => image.src);

                const listing = await client2.queryObject<Listing>({
                  args: [rawProperty.full_url, rawProperty.raw_title],
                  text: `
                    SELECT url
                    FROM Listing
                    WHERE url = $1 OR title = $2
                  `,
                });

                if (listing.rowCount && listing.rowCount > 0) {
                  console.info("Listing already exists");

                  const updateResult = await transaction.queryObject({
                    args: [rawProperty.id],
                    text: `
                      UPDATE lamudi_raw_data
                      SET is_process = TRUE
                      WHERE id = $1
                    `,
                  });

                  if (updateResult.rowCount === 1) {
                    console.info("1 record updated in lamudi_raw_data");
                  }

                  const updateListingResult = await transaction.queryObject({
                    args: [
                      rawProperty.price,
                      rawProperty.price_formatted,
                      rawProperty.full_url,
                    ],
                    text: `
                      UPDATE Listing 
                      SET price = $1, price_formatted = $2
                      WHERE url = $3
                    `,
                  });

                  if (updateListingResult.rowCount === 1) {
                    console.info(
                      "Listing updated with new price and price_formatted",
                    );
                  }

                  const updatePropertyResult = await transaction.queryObject({
                    args: [
                      JSON.stringify(images),
                      rawProperty.agent_name,
                      rawProperty.product_owner_name,
                      rawProperty.project_name,
                      rawProperty.full_url,
                    ],
                    text: `
                      UPDATE Property p
                      SET images = $1,
                          agent_name = $2,
                          product_owner_name = $3,
                          project_name = $4
                      FROM Listing l 
                      WHERE l.property_id = p.id AND l.url = $5
                    `,
                  });

                  if (updatePropertyResult.rowCount === 1) {
                    console.info(
                      "Property updated with new images, agent_name, product_owner_name, and project_name",
                    );
                  }

                  continue;
                }

                let property;

                try {
                  let lastCreatedPropertyId;
                  try {
                    const lastCreatedProperty = await client2.queryObject<
                      { id: number }
                    >(`
                      SELECT id
                      FROM Property
                      ORDER BY created_at DESC
                      LIMIT 1
                    `);
                    lastCreatedPropertyId = lastCreatedProperty.rows[0].id +
                      Math.floor(100000 + Math.random() * 900000);
                  } catch (error) {
                    console.error(
                      "Error fetching last created property:",
                      error,
                    );
                    throw error;
                  }

                  property = await client2.queryObject<Property>({
                    args: [
                      lastCreatedPropertyId,
                      rawProperty.floor_size,
                      rawProperty.lot_size,
                      rawProperty.building_size,
                      rawProperty.no_of_bedrooms,
                      rawProperty.no_of_bathrooms,
                      rawProperty.no_of_parking_spaces,
                      rawProperty.longitude,
                      rawProperty.latitude,
                      rawProperty.year_built ?? 0,
                      rawProperty.primary_image_url,
                      JSON.stringify(images),
                      JSON.stringify(rawProperty.property_features),
                      JSON.stringify(rawProperty.indoor_features),
                      JSON.stringify(rawProperty.outdoor_features),
                      rawProperty.property_type_id ?? 5, // Default to "Others" property type if null/undefined
                      rawProperty.address ?? "-",
                      parseInt(rawProperty.listing_region_id),
                      parseInt(rawProperty.listing_city_id),
                      parseInt(rawProperty.listing_area_id),
                      rawProperty.project_name,
                      rawProperty.agent_name,
                      rawProperty.product_owner_name,
                      0, // Add missing required field
                      JSON.stringify({}), // Add missing field
                      JSON.stringify({}), // Add missing field
                      JSON.stringify({}), // Add missing field
                    ],
                    text: `
                      INSERT INTO Property 
                      (
                        id, floor_size, lot_size, building_size, no_of_bedrooms,
                        no_of_bathrooms, no_of_parking_spaces, longitude,
                        latitude, year_built, primary_image_url, images,
                        property_features, indoor_features, outdoor_features,
                        property_type_id, address, listing_region_id, listing_city_id,
                        listing_area_id, project_name, agent_name, product_owner_name,
                        ceiling_height, amenities, ai_generated_description,
                        ai_generated_basic_features
                      )
                      VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,
                        $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25,
                        $26, $27
                      )
                      RETURNING id
                    `,
                  });
                } catch (error) {
                  throw error;
                }

                if (property.rowCount && property.rowCount > 0) {
                  console.info(
                    "Newly created property ID:",
                    property.rows[0].id,
                  );

                  try {
                    const lastCreatedListingId = await client2.queryObject<
                      { id: number }
                    >(`
                      SELECT id
                      FROM Listing
                      ORDER BY created_at DESC
                      LIMIT 1
                    `);
                    const newListingId = lastCreatedListingId.rows[0].id +
                      Math.floor(100000 + Math.random() * 900000);

                    const newListing = await transaction.queryObject<Listing>({
                      args: [
                        newListingId,
                        rawProperty.raw_title,
                        rawProperty.full_url,
                        rawProperty.project_name,
                        rawProperty.description,
                        true, // is_scraped
                        rawProperty.address,
                        rawProperty.price_formatted,
                        rawProperty.price,
                        rawProperty.offer_type_id,
                        property.rows[0].id,
                      ],
                      text: `
                        INSERT INTO Listing (
                          id, title, url, project_name, description, is_scraped,
                          address, price_formatted, price, offer_type_id, property_id
                        ) VALUES (
                          $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
                        )
                        RETURNING id
                      `,
                    });

                    if (newListing.rowCount && newListing.rowCount > 0) {
                      console.info(
                        "Newly created listing ID:",
                        newListing.rows[0].id,
                      );
                    }

                    const updateResult = await transaction.queryObject({
                      args: [rawProperty.id],
                      text: `
                        UPDATE lamudi_raw_data
                        SET is_process = TRUE
                        WHERE id = $1
                      `,
                    });

                    if (updateResult.rowCount === 1) {
                      console.info("1 record updated in lamudi_raw_data");
                    }
                  } catch (error) {
                    await client2.queryObject({
                      args: [rawProperty.id],
                      text: `
                      UPDATE lamudi_raw_data
                      SET is_process = TRUE
                      WHERE id = $1
                    `,
                    });
                    throw error;
                  }
                }
              }
            }

            await transaction.commit();
            console.info("Transaction successfully committed");
          } catch (error) {
            if (transaction) {
              try {
                await transaction.rollback();
              } catch (rollbackError) {
                console.error("Error during rollback:", rollbackError);
              }
            }
            console.error("Transaction error:", error);
          } finally {
            if (client) client.release();
          }
        }
        break;
      case "CREATE_RAW_LAMUDI_LISTING_DATA":
        {
          let transaction: Transaction | null = null;
          const client_1 = await dbPool.connect();
          try {
            transaction = client_1.createTransaction("create-raw-lamudi-data");
            await transaction.begin();
            await transaction.queryObject({
              args: [
                JSON.stringify(msg.data),
                msg.data.listingUrl,
                JSON.stringify(msg.data.images),
              ],
              text:
                `INSERT INTO Lamudi_raw_data (json_data, listingUrl, images) VALUES ($1, $2, $3)`,
            });
            await transaction.commit();
            console.log("Transaction successfully committed for create");
          } catch (error) {
            if (transaction) await transaction.rollback();
            console.error(error);
          } finally {
            client_1.release();
            console.log("Connection released");
          }
        }
        break;
      case "CREATE_AI_GENERATED_DESCRIPTION":
        {
          using client = await dbPool.connect();
          try {
            const property = await client.queryObject(
              `SELECT * FROM Property
               WHERE ai_generated_description IS NULL AND property_type_id IN (1, 3) 
               ORDER BY created_at DESC LIMIT 10`,
            );

            if (property.rowCount && property.rowCount > 0) {
              // Process properties in parallel with rate limiting
              const processProperty = async (row: unknown) => {
                const propertyData = row as {
                  id: number;
                };

                const aiGeneratedDescription = await openaiAssistant(
                  JSON.stringify(row),
                );

                try {
                  JSON.parse(
                    aiGeneratedDescription.includes("```json")
                      ? aiGeneratedDescription
                        .replace("```json", "")
                        .replace("```", "")
                      : aiGeneratedDescription,
                  );
                } catch {
                  throw Error("Invalid AI description format");
                }

                if (aiGeneratedDescription) {
                  await client.queryObject({
                    args: [
                      JSON.stringify(aiGeneratedDescription),
                      propertyData.id,
                    ],
                    text:
                      `UPDATE Property SET ai_generated_description = $1 WHERE id = $2`,
                  });
                }
              };

              // Process 2 properties at a time with 2s delay between batches
              for (let i = 0; i < property.rows.length; i += 2) {
                const batch = property.rows.slice(i, i + 2);
                await Promise.all(batch.map(processProperty));
                if (i + 2 < property.rows.length) {
                  await new Promise((resolve) => setTimeout(resolve, 2000));
                }
              }
            }

            console.log("Successfully processed ai generated description");
          } catch (error) {
            console.error(error);
          }
        }
        break;
    }
  });
}

export function closeKvInstance(): void {
  if (db) {
    db.close();
    db = null;
  }
}
