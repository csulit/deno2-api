import "jsr:@std/dotenv/load";
import { type Context, Hono, cors } from "jsr:@hono/hono";

import { dbPool } from "./config/postgres.ts";
import { getKvInstance, listenQueue, sendMessage } from "./config/deno-kv.ts";

const app = new Hono();
const kv = await getKvInstance();

app.use(cors({
    origin: "*",
  }));

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

  // Example of how to add a new condition in the future:
  // if (someNewCondition) {
  //   addWhereCondition(`new_column = $${paramCounter}`, newValue);
  // }

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
          ORDER BY l.id DESC LIMIT $${paramCounter} OFFSET $${paramCounter + 1};
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

  return c.json({ data: property.rows[0] });
});

app.post("/", async (c: Context) => {
  const data = await c.req.json();
  await sendMessage({ kv, data, options: { delay: 5000 } });
  return c.text("Hono!");
});

listenQueue(kv).catch((error) => console.error(error));

Deno.serve({ port: parseInt(Deno.env.get("PORT") || "8000") }, app.fetch);
