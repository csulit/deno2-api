import "jsr:@std/dotenv/load";
import { type Context, Hono } from "npm:hono";
import { cors } from "npm:hono/cors";

import { dbPool } from "./config/postgres.ts";
import {
  getKvInstance,
  listenQueue,
  type Listing,
  type Property,
  type RawLamudiData,
  sendMessage,
} from "./config/deno-kv.ts";
import type { PoolClient, Transaction } from "postgres";

const app = new Hono();
const kv = await getKvInstance();

app.use("*", cors({ origin: "*" }));

app.get("/api/properties", async (c: Context) => {
  using client = await dbPool.connect();
  const query = c.req.query() as unknown as {
    page?: string;
    page_size?: string;
    property_type_id?: string;
    listing_type_id?: string;
    search_longitude?: string;
    search_latitude?: string;
    bounding_box?: string;
    max_distance_km?: string;
    building_size_min?: string;
    building_size_max?: string;
    floor_size_min?: string;
    floor_size_max?: string;
    lot_size_min?: string;
    lot_size_max?: string;
    ceiling_height_min?: string;
    ceiling_height_max?: string;
    no_of_bedrooms_min?: string;
    no_of_bedrooms_max?: string;
    no_of_bathrooms_min?: string;
    no_of_bathrooms_max?: string;
    no_of_parking_spaces_min?: string;
    no_of_parking_spaces_max?: string;
    ai_generated_description?: string;
    sort_by?: string;
    sort_order?: string;
  };

  if (!query.page) {
    query.page = "1";
  }

  if (!query.page_size) {
    query.page_size = "10";
  }

  if (!query.property_type_id) {
    query.property_type_id = "1";
  }

  if (!query.listing_type_id) {
    query.listing_type_id = "1";
  }

  if (!query.sort_by) {
    query.sort_by = "created_at"; // Default sort field
  }

  if (!query.sort_order) {
    query.sort_order = "DESC"; // Default sort order
  }

  const offset = (parseInt(query.page) - 1) * parseInt(query.page_size);

  let boundingBoxCoords: number[] | null[] = [null, null, null, null];

  if (query?.bounding_box) {
    boundingBoxCoords = query.bounding_box.split("::").map((coord) =>
      parseFloat(coord)
    );
  }

  const searchLongitude = parseFloat(query?.search_longitude || "0");
  const searchLatitude = parseFloat(query?.search_latitude || "0");

  // Initialize the SQL WHERE clause with base conditions
  let sqlWhereClause = `
    pt.property_type_id = $1
    AND lt.listing_type_id = $2
  `;

  // Initialize the SQL parameters array with base parameters
  const sqlParams = [
    parseInt(query.property_type_id),
    parseInt(query.listing_type_id),
  ];

  // Initialize the parameter counter for dynamic parameter numbering
  let paramCounter = 3;

  // Function to add a new condition to the WHERE clause
  // deno-lint-ignore no-explicit-any
  const addWhereCondition = (condition: string, ...params: any[]) => {
    sqlWhereClause += ` AND ${condition}`;
    sqlParams.push(...params);
    paramCounter += params.length;
  };

  // Add bounding box condition if all coordinates are provided
  if (boundingBoxCoords.every((coord) => coord !== null)) {
    addWhereCondition(
      `
      p.latitude BETWEEN $${paramCounter} AND $${paramCounter + 1}
      AND p.longitude BETWEEN $${paramCounter + 2} AND $${paramCounter + 3}
    `,
      ...boundingBoxCoords as number[],
    );
  }

  // Add max distance condition if required parameters are provided
  if (query.max_distance_km && searchLongitude !== 0 && searchLatitude !== 0) {
    addWhereCondition(
      `
      ST_DWithin(p.geog, ST_SetSRID(ST_MakePoint($${paramCounter}, $${
        paramCounter + 1
      }), 4326)::geography, ${parseFloat(query.max_distance_km)} * 1000)
    `,
      searchLongitude,
      searchLatitude,
    );
  }

  // Add building size range condition if both min and max are provided
  if (query.building_size_min && query.building_size_max) {
    addWhereCondition(
      `p.building_size BETWEEN $${paramCounter} AND $${paramCounter + 1}`,
      parseFloat(query.building_size_min),
      parseFloat(query.building_size_max),
    );
  }

  // Add floor size range condition if both min and max are provided
  if (query.floor_size_min && query.floor_size_max) {
    addWhereCondition(
      `p.floor_size BETWEEN $${paramCounter} AND $${paramCounter + 1}`,
      parseFloat(query.floor_size_min),
      parseFloat(query.floor_size_max),
    );
  }

  // Add lot size range condition if both min and max are provided
  if (query.lot_size_min && query.lot_size_max) {
    addWhereCondition(
      `p.lot_size BETWEEN $${paramCounter} AND $${paramCounter + 1}`,
      parseFloat(query.lot_size_min),
      parseFloat(query.lot_size_max),
    );
  }

  // Add ceiling height range condition if both min and max are provided
  if (query.ceiling_height_min && query.ceiling_height_max) {
    addWhereCondition(
      `p.ceiling_height BETWEEN $${paramCounter} AND $${paramCounter + 1}`,
      parseFloat(query.ceiling_height_min),
      parseFloat(query.ceiling_height_max),
    );
  }

  // Add number of bedrooms range condition if both min and max are provided
  if (query.no_of_bedrooms_min && query.no_of_bedrooms_max) {
    addWhereCondition(
      `p.no_of_bedrooms BETWEEN $${paramCounter} AND $${paramCounter + 1}`,
      parseInt(query.no_of_bedrooms_min),
      parseInt(query.no_of_bedrooms_max),
    );
  }

  // Add number of bathrooms range condition if both min and max are provided
  if (query.no_of_bathrooms_min && query.no_of_bathrooms_max) {
    addWhereCondition(
      `p.no_of_bathrooms BETWEEN $${paramCounter} AND $${paramCounter + 1}`,
      parseInt(query.no_of_bathrooms_min),
      parseInt(query.no_of_bathrooms_max),
    );
  }

  // Add number of parking spaces range condition if both min and max are provided
  if (query.no_of_parking_spaces_min && query.no_of_parking_spaces_max) {
    addWhereCondition(
      `p.no_of_parking_spaces BETWEEN $${paramCounter} AND $${
        paramCounter + 1
      }`,
      parseInt(query.no_of_parking_spaces_min),
      parseInt(query.no_of_parking_spaces_max),
    );
  }

  // Add AI generated description filter if value is 1
  if (query.ai_generated_description === "1") {
    addWhereCondition(`p.ai_generated_description IS NOT NULL`);
  }

  // Validate and construct ORDER BY clause
  let orderByClause = "";
  const validSortFields = ["id", "created_at", "price"];
  const validSortOrders = ["ASC", "DESC"];

  if (
    validSortFields.includes(query.sort_by) &&
    validSortOrders.includes(query.sort_order.toUpperCase())
  ) {
    orderByClause =
      `ORDER BY l.${query.sort_by} ${query.sort_order.toUpperCase()}`;
  } else {
    orderByClause = "ORDER BY l.created_at DESC";
  }

  console.log({ sqlWhereClause, sqlParams, nextParamCounter: paramCounter });

  const postgres = await client.queryObject({
    args: [...sqlParams, parseInt(query.page_size), offset],
    text: `
          SELECT
              l.id AS listing_id,
              l.title,
              l.url,
              l.project_name,
              l.description,
              l.is_scraped,
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
              lt.type_name AS listing_type_name,
              wt.type_name AS warehouse_type_name,
              l.address AS listing_address,
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
              LEFT JOIN Listing_Type lt ON l.offer_type_id = lt.listing_type_id
              LEFT JOIN Warehouse_Type wt ON p.warehouse_type_id = wt.warehouse_type_id
              LEFT JOIN Listing_Region rg ON p.listing_region_id = rg.id
              LEFT JOIN Listing_City ct ON p.listing_city_id = ct.id
              LEFT JOIN Listing_Area ar ON p.listing_area_id = ar.id
          WHERE
              ${sqlWhereClause}
          ${orderByClause} LIMIT $${paramCounter} OFFSET $${paramCounter + 1};
    `,
  });

  const recordCount = await client.queryObject({
    args: sqlParams,
    text: `
      SELECT COUNT(*)::integer
      FROM
        Listing l
        JOIN Property p ON l.property_id = p.id
        LEFT JOIN Property_Type pt ON p.property_type_id = pt.property_type_id
        LEFT JOIN Listing_Type lt ON l.offer_type_id = lt.listing_type_id
        LEFT JOIN Warehouse_Type wt ON p.warehouse_type_id = wt.warehouse_type_id
        LEFT JOIN Listing_Region rg ON p.listing_region_id = rg.id
        LEFT JOIN Listing_City ct ON p.listing_city_id = ct.id
        LEFT JOIN Listing_Area ar ON p.listing_area_id = ar.id
      WHERE
        ${sqlWhereClause}
    `,
  });

  const counterResult = recordCount.rows[0] as { count: number };
  const totalListingRecords = counterResult.count;
  const pageNo = parseInt(query.page);
  const pageSize = parseInt(query.page_size);

  const totalPages = Math.ceil(totalListingRecords / pageSize);
  const nextPage = pageNo < totalPages ? pageNo + 1 : null;
  const previousPage = pageNo > 1 ? pageNo - 1 : null;

  return c.json({
    data: postgres.rows,
    pagination: {
      total: totalListingRecords,
      page: pageNo,
      page_size: pageSize,
      total_pages: totalPages,
      next_page: nextPage,
      previous_page: previousPage,
    },
  });
});

app.get("/api/properties/valuation", async (c: Context) => {
  const data = c.req.query();

  if (!data.property_type_id || !data.size_in_sqm) {
    return c.json({ error: "Property type and size are required" }, 400);
  }

  const propertyTypeId = parseInt(data.property_type_id);
  if (propertyTypeId < 1 || propertyTypeId > 4) {
    return c.json({
      error: "Invalid property type ID. Must be between 1 and 4",
    }, 400);
  }

  using client = await dbPool.connect();
  const sizeInSqm = parseFloat(data.size_in_sqm);
  if (isNaN(sizeInSqm) || sizeInSqm <= 0) {
    return c.json({ error: "Invalid size value" }, 400);
  }

  const queryParams: (number)[] = [propertyTypeId, sizeInSqm];
  let cityClause = "";
  let propertyFeaturesClause = "";
  let paramCounter = 3;

  if (data.city_id) {
    const cityId = parseInt(data.city_id);
    if (isNaN(cityId) || cityId < 1) {
      return c.json({ error: "Invalid city ID" }, 400);
    }
    queryParams.push(cityId);
    cityClause = `AND p.listing_city_id = $${paramCounter}`;
    paramCounter++;
  }

  // Only allow bedroom, bathroom, and parking filters for residential properties
  if (propertyTypeId === 1 || propertyTypeId === 2) {
    if (data.no_of_bedrooms) {
      const bedrooms = parseInt(data.no_of_bedrooms);
      if (isNaN(bedrooms) || bedrooms < 0) {
        return c.json({ error: "Invalid number of bedrooms" }, 400);
      }
      queryParams.push(bedrooms);
      propertyFeaturesClause += `AND p.no_of_bedrooms = $${paramCounter} `;
      paramCounter++;
    }

    if (data.no_of_bathrooms) {
      const bathrooms = parseInt(data.no_of_bathrooms);
      if (isNaN(bathrooms) || bathrooms < 0) {
        return c.json({ error: "Invalid number of bathrooms" }, 400);
      }
      queryParams.push(bathrooms);
      propertyFeaturesClause += `AND p.no_of_bathrooms = $${paramCounter} `;
      paramCounter++;
    }

    if (data.no_of_parking_spaces) {
      const parkingSpaces = parseInt(data.no_of_parking_spaces);
      if (isNaN(parkingSpaces) || parkingSpaces < 0) {
        return c.json({ error: "Invalid number of parking spaces" }, 400);
      }
      queryParams.push(parkingSpaces);
      propertyFeaturesClause +=
        `AND p.no_of_parking_spaces = $${paramCounter} `;
      paramCounter++;
    }
  }

  interface PropertyStats {
    offer_type_id: number;
    average_price: number;
    total_comparable_properties: number;
  }

  const properties = await client.queryObject<PropertyStats>({
    args: queryParams,
    text: `
      WITH PropertyStats AS (
        SELECT
          l.price,
          l.offer_type_id
        FROM Property p
        JOIN Listing l ON p.id = l.property_id
        WHERE 
          p.property_type_id = $1
          ${cityClause}
          ${propertyFeaturesClause}
          AND CASE 
            WHEN p.property_type_id IN (1, 3) THEN p.building_size BETWEEN $2 * 0.8 AND $2 * 1.2
            ELSE p.lot_size BETWEEN $2 * 0.8 AND $2 * 1.2
          END
          AND l.price > 0
      )
      SELECT
        l.offer_type_id,
        ROUND(AVG(price)::numeric, 2) as average_price,
        COUNT(*) as total_comparable_properties
      FROM PropertyStats l
      GROUP BY l.offer_type_id
    `,
  });

  if (!properties.rows.length) {
    return c.json({
      error: "Not enough data to generate valuation for the specified criteria",
    }, 404);
  }

  const valuationData = properties.rows.reduce(
    (acc, row) => {
      const type = row.offer_type_id === 1 ? "buy" : "rent";
      const formattedPrice = new Intl.NumberFormat("en-PH", {
        style: "currency",
        currency: "PHP",
        minimumFractionDigits: 2,
      }).format(row.average_price);

      acc[type] = {
        average_price: row.average_price.toString(),
        formatted_price: formattedPrice,
        total_comparable_properties: row.total_comparable_properties.toString(),
      };
      return acc;
    },
    {} as Record<
      string,
      {
        average_price: string;
        formatted_price: string;
        total_comparable_properties: string;
      }
    >,
  );

  return c.json({ data: valuationData });
});

app.get("/api/properties/cities", async (c: Context) => {
  using client = await dbPool.connect();
  const query = c.req.query();
  const search = query.search || "";

  const cities = await client.queryObject({
    args: [`%${search}%`],
    text: `
      SELECT DISTINCT 
        ct.id,
        ct.city,
        ct.listing_city_id,
        rg.region as region_name,
        rg.listing_region_id
      FROM Listing_City ct
      JOIN Listing_Region rg ON ct.listing_region_id = rg.id 
      WHERE LOWER(ct.city) LIKE LOWER($1)
      ORDER BY ct.city ASC
      LIMIT 10
    `,
  });

  return c.json({
    data: cities.rows,
  });
});

app.get("/api/properties/areas", async (c: Context) => {
let transaction: Transaction | null = null;
  let client: PoolClient | null = null;

  try {
    client = await dbPool.connect();
    using client2 = await dbPool.connect();
    transaction = client.createTransaction(
      "create_listing_from_raw_lamudi_data",
    );

    await transaction.begin();

    const rawProperties = await transaction.queryObject<RawLamudiData>(
      `
      SELECT
          id, json_data,
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
      LIMIT 25
      `,
    );

    for (const rawProperty of rawProperties.rows) {
      try {
        let region = await transaction.queryObject(`
                SELECT id, listing_region_id 
                FROM Listing_Region 
                WHERE listing_region_id = '${rawProperty.listing_region_id}'
              `);

        let city = await transaction.queryObject(`
                SELECT id, listing_city_id
                FROM Listing_City
                WHERE listing_city_id = '${rawProperty.listing_city_id}'
              `);

        let area = await transaction.queryObject(`
                SELECT id
                FROM Listing_Area
                WHERE listing_area_id = '${rawProperty.listing_area_id}'
              `);

        if (region.rowCount === 0) {
          region = await transaction.queryObject({
            args: [rawProperty.region, rawProperty.listing_region_id],
            text: `INSERT INTO Listing_Region (region, listing_region_id)
                         VALUES ($1, $2)
                         RETURNING id, listing_region_id`,
          });
        }

        if (city.rowCount === 0) {
          const createdRegion = region.rows[0] as {
            listing_region_id: number;
          };

          city = await transaction.queryObject({
            args: [
              rawProperty.city,
              rawProperty.listing_city_id,
              createdRegion.listing_region_id,
            ],
            text:
              `INSERT INTO Listing_City (city, listing_city_id, listing_region_id)
                         VALUES ($1, $2, $3)
                         RETURNING id, listing_city_id`,
          });
        }

        if (area.rowCount === 0 && rawProperty.listing_area_id) {
          area = await transaction.queryObject({
            args: [rawProperty.listing_area, rawProperty.listing_area_id],
            text: `INSERT INTO Listing_Area (area, listing_area_id)
                         VALUES ($1, $2)
                         RETURNING id`,
          });
        }

        rawProperty.listing_region_id = (region.rows[0] as { id: number }).id.toString();
        rawProperty.listing_city_id = (city.rows[0] as { id: number }).id.toString();
        rawProperty.listing_area_id = (area.rows[0] as { id: number }).id.toString();
      } catch (error) {
        throw error;
      }
    }

    if (rawProperties.rowCount && rawProperties.rowCount > 0) {
      for (const rawProperty of rawProperties.rows) {
        const images = rawProperty.images.map((image) => image.src);

        const listing = await transaction.queryObject<Listing>({
          args: [rawProperty.full_url],
          text: `SELECT url FROM Listing WHERE url = $1`,
        });

        if (listing.rowCount && listing.rowCount > 0) {
          console.info("Listing already exists");

          await transaction.queryObject({
            args: [rawProperty.id],
            text: `UPDATE lamudi_raw_data SET is_process = TRUE WHERE id = $1`,
          });

          await transaction.queryObject({
            args: [
              rawProperty.price,
              rawProperty.price_formatted,
              rawProperty.full_url,
            ],
            text: `UPDATE Listing 
                   SET price = $1, price_formatted = $2
                   WHERE url = $3`,
          });

          await transaction.queryObject({
            args: [
              JSON.stringify(images),
              rawProperty.agent_name,
              rawProperty.product_owner_name,
              rawProperty.project_name,
              rawProperty.full_url,
            ],
            text: `UPDATE Property p
                  SET images = $1,
                      agent_name = $2,
                      product_owner_name = $3,
                      project_name = $4
                  FROM Listing l 
                  WHERE l.property_id = p.id AND l.url = $5`,
          });

          continue;
        }
        let property;

        console.log({
          listing_region_id: rawProperty.listing_region_id,
          listing_city_id: rawProperty.listing_city_id,
          listing_area_id: rawProperty.listing_area_id,
        })

        try {
          property = await client2.queryObject<Property>({
            args: [
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
              rawProperty.property_type_id,
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
            text: `INSERT INTO Property 
                    (
                      floor_size, lot_size, building_size, no_of_bedrooms,
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
                    $26
                  )
                  ON CONFLICT DO NOTHING
                  RETURNING id`,
          });
        } catch (error) {
          throw error;
        }
        console.log(property.rowCount);

        // if (property) {
        //   try {
        //     await transaction.queryObject({
        //       args: [
        //         rawProperty.raw_title,
        //         rawProperty.full_url,
        //         rawProperty.project_name,
        //         rawProperty.description,
        //         true, // is_scraped
        //         rawProperty.address,
        //         rawProperty.price_formatted,
        //         rawProperty.price,
        //         rawProperty.offer_type_id,
        //         property.rows[0].id,
        //       ],
        //       text: `
        //             INSERT INTO Listing (
        //               title, url, project_name, description, is_scraped,
        //               address, price_formatted, price, offer_type_id, property_id
        //             ) VALUES (
        //               $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
        //             )
        //               `,
        //     });
        //   } catch (error) {
        //     throw error;
        //   }
        // }
      }
    }

    await transaction.commit();
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

  return c.json({ data: "success" });
});

app.get("/api/properties/:id", async (c: Context) => {
  using client = await dbPool.connect();
  const id = c.req.param("id");

  if (!id) {
    return c.json({ error: "Property ID is required" }, 400);
  }

  const property = await client.queryObject({
    args: [id],
    text: `
      SELECT
          l.id AS listing_id,
          l.title,
          l.url,
          l.project_name,
          l.description,
          l.is_scraped,
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
          lt.type_name AS listing_type_name,
          wt.type_name AS warehouse_type_name,
          l.address AS listing_address,
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
          LEFT JOIN Listing_Type lt ON l.offer_type_id = lt.listing_type_id
          LEFT JOIN Warehouse_Type wt ON p.warehouse_type_id = wt.warehouse_type_id
          LEFT JOIN Listing_Region rg ON p.listing_region_id = rg.id
          LEFT JOIN Listing_City ct ON p.listing_city_id = ct.id
          LEFT JOIN Listing_Area ar ON p.listing_area_id = ar.id
      WHERE
          l.id = $1
    `,
  });

  if (property.rowCount === 0) {
    return c.json({ data: null });
  }

  return c.json({ data: property.rows[0] });
});

app.post("/", async (c: Context) => {
  const data = await c.req.json();
  console.info("Received message:", data.type);
  await sendMessage({ kv, data, options: { delay: 5000 } });
  return c.text("Hono!");
});

listenQueue(kv).catch((error) => console.error(error));

Deno.serve({ port: parseInt(Deno.env.get("PORT") || "8000") }, app.fetch);
