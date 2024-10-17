import type { Transaction } from "postgres";
import { dbPool } from "./postgres.ts";

let db: Deno.Kv | null = null;

export interface KvMessage {
  type: "CREATE_LISTING" | "PROPERTY_VALUATION";
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

async function getLocation(transaction: Transaction, dataLayer: Location) {
  let region = await transaction.queryObject(`
    SELECT id, listing_region_id
    FROM Listing_Region
    WHERE region = '${dataLayer.region}'
  `);

  let city = await transaction.queryObject(`
    SELECT id, listing_city_id
    FROM Listing_City
    WHERE city = '${dataLayer.city}'
  `);

  let area = await transaction.queryObject(`
    SELECT id
    FROM Listing_Area
    WHERE listing_area_id = '${dataLayer?.listing_area_id || null}'
  `);

  if (region.rowCount === 0) {
    region = await transaction.queryObject(`
      INSERT INTO Listing_Region (region, listing_region_id)
      VALUES ('${dataLayer.region}', '${dataLayer.listing_region_id}')
      RETURNING id, listing_region_id
    `);
  }

  if (city.rowCount === 0) {
    const createdRegion = region.rows[0] as { listing_region_id: number };

    city = await transaction.queryObject(`
      INSERT INTO Listing_City (city, listing_city_id, listing_region_id)
      VALUES ('${dataLayer.city}', '${dataLayer.listing_city_id}', '${createdRegion.listing_region_id}')
      RETURNING id, listing_city_id
    `);
  }

  if (area.rowCount === 0) {
    area = await transaction.queryObject(`
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
            let transaction;
            const client_1 = await dbPool.connect();

            try {
              const listingUrl = msg.data.listingUrl;
              const isCondominium =
                msg.data?.dataLayer?.attributes?.attribute_set_name ===
                "Condominium";

              transaction = client_1.createTransaction("create-listing");
              await transaction.begin();

              const listingId = await transaction.queryObject(`
                SELECT id
                FROM Listing
                WHERE url = '${listingUrl}' OR title = '${msg.data.dataLayer?.title}'
              `);

              if (listingId?.rowCount && listingId.rowCount > 0) {
                throw new Error("Listing already exists");
              }

              if (!msg.data?.dataLayer) {
                throw new Error("DataLayer is missing or undefined");
              }

              if (!msg.data.dataLayer.agent_name) {
                throw new Error("Agent name is missing or undefined");
              }

              if (
                !msg.data.dataLayer.location ||
                typeof msg.data.dataLayer.location !== "object"
              ) {
                throw new Error(
                  "Location is missing, undefined, or not an object"
                );
              }

              if (
                !msg.data.dataLayer.attributes ||
                Object.keys(msg.data.dataLayer.attributes).length < 5
              ) {
                throw new Error(
                  "Attributes are missing, undefined, or have fewer than 5 properties"
                );
              }

              if (isCondominium) {
                const propertyTypeId = 1;
                const images = msg.data.images as { src: string }[];
                const agentName = msg.data.dataLayer.agent_name;
                const location: Location = msg.data.dataLayer.location;
                const dataLayerAttributes = msg.data.dataLayer.attributes;
                const offerTypeId =
                  dataLayerAttributes.offer_type === "Rent" ? 2 : 1;
                const locationData = await getLocation(transaction, {
                  ...location,
                  listing_area_id: dataLayerAttributes?.listing_area_id,
                });

                const { region, city, area } = locationData;

                const property = await transaction.queryArray({
                  args: [
                    dataLayerAttributes?.floor_size || 0,
                    dataLayerAttributes?.lot_size || 0,
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
                      listing_area_id
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
                      $21
                    ) RETURNING id
                  `,
                });

                const newProperty = property.rows[0][0] as number;

                const address = `${
                  dataLayerAttributes?.listing_area
                    ? `${dataLayerAttributes.listing_area}, `
                    : ""
                }${dataLayerAttributes.listing_city}`;

                await transaction.queryArray({
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
                    newProperty,
                  ],
                  text: `INSERT INTO Listing (title, url, project_name, description, is_scraped, address, price_formatted, price, offer_type_id, property_id)
                  VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) RETURNING id`,
                });

                await transaction.commit();
                console.log("Transaction successfully committed");
              }
            } catch (error) {
              if (transaction) {
                console.log("Transaction rollback");
                await transaction.rollback();
              }
              throw error;
            } finally {
              console.log("Connection released");
              client_1.release();
            }
          };

          try {
            await handleCondominium();
            // deno-lint-ignore no-explicit-any
          } catch (error: any) {
            console.error(
              "Failed to handle condominium:",
              error?.message || error
            );
          }
        }
        break;
      case "PROPERTY_VALUATION":
        if (msg.source === "APP") {
          console.log(JSON.stringify(msg, null, 2));
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
