import "jsr:@std/dotenv/load";
import { type Context, Hono } from "npm:hono";
import { cors } from "npm:hono/cors";

import { dbPool } from "./config/postgres.ts";
import { getKvInstance, listenQueue, sendMessage } from "./config/deno-kv.ts";
import { openaiAssistant } from "./services/openai-assistant.ts";

const app = new Hono();
const kv = await getKvInstance();

app.use(
  "*",
  cors({
    origin: "*",
  }),
);

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

  if (!data.property_type_id || !data.size_in_sqm || !data.city_id) {
    return c.json({ error: "Property type, size and city are required" }, 400);
  }

  const propertyTypeId = parseInt(data.property_type_id);
  if (propertyTypeId < 1 || propertyTypeId > 4) {
    return c.json({
      error: "Invalid property type ID. Must be between 1 and 4",
    }, 400);
  }

  const cityId = parseInt(data.city_id);
  if (isNaN(cityId) || cityId < 1) {
    return c.json({ error: "Invalid city ID" }, 400);
  }

  return c.json({ data: null });
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

app.get("/api/properties/:id", async (c: Context) => {
  using client = await dbPool.connect();
  const id = c.req.param("id");
  const query = c.req.query();

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

  const propertyData = property.rows[0] as any;

  if (query.regenerate_ai_description === "true") {
    const aiDescription = await openaiAssistant(JSON.stringify(propertyData));

    propertyData.ai_generated_description = aiDescription;

    await client.queryObject({
      args: [propertyData.id, JSON.stringify(aiDescription)],
      text: `UPDATE Property SET ai_generated_description = $2 WHERE id = $1`,
    });
  }

  return c.json({ data: propertyData });
});

app.post("/", async (c: Context) => {
  const data = await c.req.json();
  console.log(JSON.stringify(data));
  await sendMessage({ kv, data, options: { delay: 5000 } });
  return c.text("Hono!");
});

listenQueue(kv).catch((error) => console.error(error));

Deno.serve({ port: parseInt(Deno.env.get("PORT") || "8000") }, app.fetch);
