import type { PoolClient, Transaction } from "postgres";
import { dbPool } from "./postgres.ts";
import { openaiAssistant } from "../services/openai-assistant.ts";

let db: Deno.Kv | null = null;

export interface KvMessage {
  type:
    | "CREATE_LISTING"
    | "CREATE_RAW_LAMUDI_DATA"
    | "CREATE_AI_GENERATED_DESCRIPTION"
    | "PROPERTY_VALUATION";
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

async function getLocation(client: PoolClient, dataLayer: Location) {
  let region = await client.queryObject(`
    SELECT id, listing_region_id
    FROM Listing_Region
    WHERE region = '${dataLayer.region}'
  `);

  let city = await client.queryObject(`
    SELECT id, listing_city_id
    FROM Listing_City
    WHERE city = '${dataLayer.city}'
  `);

  let area = await client.queryObject(`
    SELECT id
    FROM Listing_Area
    WHERE listing_area_id = '${dataLayer?.listing_area_id || null}'
  `);

  if (region.rowCount === 0) {
    region = await client.queryObject(`
      INSERT INTO Listing_Region (region, listing_region_id)
      VALUES ('${dataLayer.region}', '${dataLayer.listing_region_id}')
      RETURNING id, listing_region_id
    `);
  }

  if (city.rowCount === 0) {
    const createdRegion = region.rows[0] as { listing_region_id: number };

    city = await client.queryObject(`
      INSERT INTO Listing_City (city, listing_city_id, listing_region_id)
      VALUES ('${dataLayer.city}', '${dataLayer.listing_city_id}', '${createdRegion.listing_region_id}')
      RETURNING id, listing_city_id
    `);
  }

  if (area.rowCount === 0) {
    area = await client.queryObject(`
      INSERT INTO Listing_Area (area, listing_area_id)
      VALUES ('${dataLayer.area}', '${dataLayer.listing_area_id}')
      RETURNING id
    `);
  }

  return {
    region: region.rows[0] as { id: number; listing_region_id: number },
    city: city.rows[0] as { id: number; listing_city_id: number },
    area: area.rows[0] as { id: number },
  };
}

function cleanSpecialCharacters(input: string): string {
  if (!input) return "No description";

  // Encode special characters to ensure they are properly interpreted by the SQL engine
  const encodedString = encodeURIComponent(input);

  // Remove emojis and other special characters
  const cleanedString = encodedString.replace(
    /[\u{1F600}-\u{1F64F}\u{1F300}-\u{1F5FF}\u{1F680}-\u{1F6FF}\u{1F1E0}-\u{1F1FF}\u{2600}-\u{26FF}\u{2700}-\u{27BF}]/gu,
    ""
  );

  // Remove extra whitespace
  const trimmedString = cleanedString.replace(/\s+/g, " ").trim();

  // Remove any remaining non-printable characters
  return trimmedString.replace(/[^\x20-\x7E]/g, "");
}

export async function listenQueue(kv: Deno.Kv) {
  await kv.listenQueue(async (msg: KvMessage) => {
    switch (msg.type) {
      case "CREATE_LISTING":
        if (msg.source === "LAMUDI" && msg.data.listingUrl && msg.data) {
          const handleCondominium = async () => {
            let transaction: Transaction | null = null;
            const client_1 = await dbPool.connect();
            const client_2 = await dbPool.connect();

            try {
              const attributesLength = Object.keys(
                msg.data.dataLayer.attributes
              ).length;

              console.log("attributesLength:", attributesLength);

              transaction = client_1.createTransaction("create-listing");

              await transaction.begin();

              if (!msg.data?.dataLayer) {
                throw new Error("DataLayer is missing or undefined");
              }

              if (!msg.data.dataLayer.attributes || attributesLength < 3) {
                throw new Error(
                  "Attributes are missing, undefined, or have fewer than 3 properties"
                );
              }

              if (
                !msg.data.dataLayer.location ||
                typeof msg.data.dataLayer.location !== "object"
              ) {
                throw new Error(
                  "Location is missing, undefined, or not an object"
                );
              }

              let propertyTypeId;
              let warehouseTypeId;
              const listingUrl = msg.data.listingUrl;
              const images = msg.data.images as { src: string }[];
              const isCondominium =
                msg.data.dataLayer.attributes.attribute_set_name ===
                "Condominium";
              const isHouse =
                msg.data.dataLayer.attributes.attribute_set_name === "House";
              const isWarehouse =
                msg.data.dataLayer.attributes.subcategory === "Warehouse";
              const isLand =
                msg.data.dataLayer.attributes.subcategory === "Land";

              const listingRecord = await transaction.queryObject(`
                SELECT id, property_id
                FROM Listing
                WHERE url = '${listingUrl}' OR title = '${msg.data.dataLayer?.title}'
              `);

              if (listingRecord?.rowCount && listingRecord.rowCount > 0) {
                const listing = listingRecord.rows[0] as {
                  id: number;
                  property_id: number;
                };

                const price = msg.data.dataLayer?.attributes?.price;
                const priceFormatted =
                  msg.data.dataLayer?.attributes?.price_formatted;

                await transaction.queryArray({
                  args: [price, priceFormatted, listing.id],
                  text: `
                    UPDATE Listing
                    SET price = $1, price_formatted = $2
                    WHERE id = $3
                  `,
                });

                await transaction.queryArray({
                  args: [
                    JSON.stringify(msg.data.dataLayer),
                    JSON.stringify(images.map((image) => image.src)),
                    listing.property_id,
                  ],
                  text: `
                    UPDATE Property
                    SET json_data = $1, images = $2
                    WHERE id = $3
                  `,
                });

                if (transaction) await transaction.commit();
                client_1.release();
                client_2.release();
                console.log("Transaction successfully committed for update");

                return;
              }

              if (isCondominium) {
                propertyTypeId = 1;
              }

              if (isHouse) {
                propertyTypeId = 2;
              }

              if (isWarehouse) {
                const warehouseType =
                  msg.data.dataLayer.attributes.attribute_set_name;

                const warehouseTypeRecord = await transaction.queryArray({
                  args: [warehouseType],
                  text: `
                    SELECT warehouse_type_id
                    FROM Warehouse_Type
                    WHERE type_name = $1
                  `,
                });

                if (warehouseTypeRecord.rowCount === 1) {
                  warehouseTypeId = warehouseTypeRecord.rows[0][0] as number;
                } else {
                  const newWarehouseType = await transaction.queryArray({
                    args: [warehouseType],
                    text: `
                      INSERT INTO Warehouse_Type (type_name)
                      VALUES ($1) RETURNING warehouse_type_id
                    `,
                  });

                  warehouseTypeId = newWarehouseType.rows[0][0] as number;
                }

                propertyTypeId = 3;
              }

              if (isLand) {
                propertyTypeId = 4;
              }

              const agentId = msg.data.dataLayer?.agent_id;
              const agentName = msg.data.dataLayer?.agent_name;
              const productOwnerId = msg.data.dataLayer?.product_owner;
              const productOwnerName = msg.data.dataLayer?.product_owner_name;
              const location: Location = msg.data.dataLayer.location;
              const dataLayerAttributes = msg.data.dataLayer.attributes;
              const offerTypeId =
                dataLayerAttributes.offer_type === "Rent" ? 2 : 1;
              const sellerIsTrusted = dataLayerAttributes?.seller_is_trusted;

              const locationData = await getLocation(client_2, {
                ...location,
                listing_area_id: dataLayerAttributes?.listing_area_id,
              });

              const { region, city, area } = locationData;

              let property;

              try {
                property = await transaction.queryObject({
                  args: [
                    dataLayerAttributes?.floor_size || 0,
                    dataLayerAttributes?.land_size || 0,
                    dataLayerAttributes?.building_size || 0,
                    dataLayerAttributes?.ceiling_height || 0,
                    dataLayerAttributes?.bedrooms || 0,
                    dataLayerAttributes?.bathrooms || 0,
                    dataLayerAttributes?.car_spaces || 0,
                    dataLayerAttributes.location_longitude,
                    dataLayerAttributes.location_latitude,
                    dataLayerAttributes?.year_built || 0,
                    dataLayerAttributes?.image_url || null,
                    JSON.stringify(images.map((image) => image.src)),
                    JSON.stringify(dataLayerAttributes?.amenities || {}),
                    JSON.stringify(
                      dataLayerAttributes?.property_features || {}
                    ),
                    JSON.stringify(dataLayerAttributes?.indoor_features || {}),
                    JSON.stringify(dataLayerAttributes?.outdoor_features || {}),
                    propertyTypeId,
                    dataLayerAttributes?.address || null,
                    region.id,
                    city.id,
                    area.id,
                    JSON.stringify(msg.data.dataLayer),
                    warehouseTypeId || null,
                  ],
                  text: `
                    INSERT INTO property (
                      floor_size, 
                      lot_size, 
                      building_size, 
                      ceiling_height, 
                      no_of_bedrooms, 
                      no_of_bathrooms, 
                      no_of_parking_spaces, 
                      longitude, 
                      latitude, 
                      year_built, 
                      primary_image_url,
                      images,
                      amenities, 
                      property_features, 
                      indoor_features, 
                      outdoor_features, 
                      property_type_id, 
                      address, 
                      listing_region_id, 
                      listing_city_id, 
                      listing_area_id,
                      json_data,
                      warehouse_type_id
                    ) VALUES (
                      $1,
                      $2,
                      $3,
                      $4,
                      $5,
                      $6,
                      $7,
                      $8,
                      $9,
                      $10,
                      $11,
                      $12,
                      $13,
                      $14,
                      $15,
                      $16,
                      $17,
                      $18,
                      $19,
                      $20,
                      $21,
                      $22,
                      $23
                    ) RETURNING id
                  `,
                });
              } catch (error) {
                console.error("Error inserting property:", error);
                throw error;
              }

              const newProperty = property.rows[0] as { id: number };

              const address = `${
                dataLayerAttributes?.listing_area
                  ? `${dataLayerAttributes.listing_area}, `
                  : ""
              }${dataLayerAttributes.listing_city}`;

              try {
                await transaction.queryObject({
                  args: [
                    msg.data.dataLayer?.title,
                    `https://www.lamudi.com.ph/${dataLayerAttributes?.urlkey_details}`,
                    dataLayerAttributes?.project_name || null,
                    cleanSpecialCharacters(
                      msg.data.dataLayer?.description?.text
                    ),
                    true,
                    address,
                    dataLayerAttributes?.price_formatted
                      ? `${dataLayerAttributes?.price_formatted}`
                      : null,
                    dataLayerAttributes?.price || 0,
                    offerTypeId,
                    newProperty.id,
                  ],
                  text: `INSERT INTO Listing (title, url, project_name, description, is_scraped, address, price_formatted, price, offer_type_id, property_id)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) RETURNING id`,
                });
              } catch (error) {
                console.error("Error inserting listing:", error);
                throw error;
              }

              await transaction.commit();
              console.log("Transaction successfully committed for create");
              // deno-lint-ignore no-explicit-any
            } catch (error: any) {
              if (transaction) {
                console.log("Transaction rollback");
                await transaction.rollback();
              }
              throw error;
            } finally {
              console.log("Connection released");
              client_1.release();
              client_2.release();
            }
          };

          try {
            await handleCondominium();
            // deno-lint-ignore no-explicit-any
          } catch (error: any) {
            console.error(error?.message || error);
          }
        }
        break;
      case "CREATE_RAW_LAMUDI_DATA":
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
              text: `INSERT INTO Lamudi_raw_data (json_data, listingUrl, images) VALUES ($1, $2, $3)`,
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
          let transaction: Transaction | null = null;
          const client_1 = await dbPool.connect();
          let processedProperty: {
            id: number;
            ai_generated_description: string;
          }[] = [];

          try {
            transaction = client_1.createTransaction(
              "create-ai-generated-description"
            );

            await transaction.begin();

            const property = await transaction.queryObject(
              `SELECT * FROM Property WHERE ai_generated_description IS NULL LIMIT 10 ORDER BY created_at DESC`
            );

            if (property.rowCount && property.rowCount > 0) {
              // Process properties in parallel with rate limiting
              const processProperty = async (row: unknown) => {
                const propertyData = row as {
                  id: number;
                };

                const aiGeneratedDescription = await openaiAssistant(
                  JSON.stringify(row)
                );

                if (aiGeneratedDescription) {
                  processedProperty.push({
                    id: propertyData.id,
                    ai_generated_description: aiGeneratedDescription,
                  });
                }
              };

              // Process 2 properties at a time with 5s delay between batches
              for (let i = 0; i < property.rows.length; i += 2) {
                const batch = property.rows.slice(i, i + 2);
                await Promise.all(batch.map(processProperty));
                if (i + 2 < property.rows.length) {
                  await new Promise((resolve) => setTimeout(resolve, 5000));
                }
              }

              // Update all processed properties in transaction
              // for (const prop of processedProperty) {
              //   await transaction.queryObject({
              //     args: [prop.ai_generated_description, prop.id],
              //     text: `UPDATE Property SET ai_generated_description = $1 WHERE id = $2`,
              //   });
              // }
            }

            console.log(processedProperty);

            await transaction.commit();
            console.log("Transaction successfully committed for create");
            // Reset processed properties after successful commit
            processedProperty = [];
          } catch (error) {
            if (transaction) await transaction.rollback();
            console.error(error);
          } finally {
            client_1.release();
            console.log("Connection released");
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
