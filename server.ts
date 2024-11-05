import "jsr:@std/dotenv/load";
import { type Context, Hono } from "npm:hono";
import { cors } from "npm:hono/cors";
import { dbPool } from "./config/postgres.ts";
import { getKvInstance, listenQueue, sendMessage } from "./config/deno-kv.ts";
import { openaiAssistant } from "./services/openai-assistant.ts";

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
    listing_city_id?: string; // Added listing_city_id
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
    price_min?: string;
    price_max?: string;
    sort_by?: string;
    sort_order?: string;
    search?: string;
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

  // Add listing_city_id condition if provided
  if (query.listing_city_id) {
    addWhereCondition(
      `p.listing_city_id = $${paramCounter}`,
      parseInt(query.listing_city_id)
    );
  }

  // Add text search condition if search query is provided
  if (query.search) {
    addWhereCondition(
      `to_tsvector('english', l.title || ' ' || l.description) @@ plainto_tsquery($${paramCounter})`,
      query.search,
    );
  }

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

  // Add price range condition if both min and max are provided
  if (query.price_min && query.price_max) {
    addWhereCondition(
      `l.price BETWEEN $${paramCounter} AND $${paramCounter + 1}`,
      parseFloat(query.price_min),
      parseFloat(query.price_max),
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
          WITH User_Likes AS (
              SELECT 
                  uf.property_id
              FROM 
                  user_favorites uf
              WHERE
                  uf.user_id = 1  -- Replace with user's actual ID or parameter
          )
          SELECT
              l.id AS listing_id,
              l.title,
              l.url,
              l.description,
              l.is_scraped,
              l.price,
              l.price_formatted,
              l.price_not_shown,
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
              p.project_name AS property_project_name,
              p.agent_name,
              p.product_owner_name,
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
              ) AS price_change_log,
              CASE
                  WHEN ull.property_id IS NOT NULL THEN TRUE
                  ELSE FALSE
              END AS is_liked
          FROM
              Listing l
              JOIN Property p ON l.property_id = p.id
              LEFT JOIN Property_Type pt ON p.property_type_id = pt.property_type_id
              LEFT JOIN Listing_Type lt ON l.offer_type_id = lt.listing_type_id
              LEFT JOIN Warehouse_Type wt ON p.warehouse_type_id = wt.warehouse_type_id
              LEFT JOIN Listing_Region rg ON p.listing_region_id = rg.id
              LEFT JOIN Listing_City ct ON p.listing_city_id = ct.id
              LEFT JOIN Listing_Area ar ON p.listing_area_id = ar.id
              LEFT JOIN User_Likes ull ON p.id = ull.property_id
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

  if (!query.property_type_id) {
    return c.json({ error: "Property type ID is required" }, 400);
  }

  const cities = await client.queryObject({
    args: [`%${search}%`, query.property_type_id],
    text: `
      SELECT DISTINCT 
        ct.id::text,
        ct.city,
        ct.listing_city_id,
        rg.id::text as region_id,
        rg.region as region_name,
        rg.listing_region_id,
        pt.type_name as property_type_name,
        COUNT(DISTINCT CASE 
          WHEN p.property_type_id = $2::int 
          THEN p.id 
        END)::text as property_count
      FROM Listing_City ct
      JOIN Listing_Region rg ON ct.listing_region_id = rg.id
      LEFT JOIN Property p ON p.listing_city_id = ct.id
      LEFT JOIN Property_Type pt ON pt.property_type_id = $2::int
      WHERE LOWER(ct.city) LIKE LOWER($1)
      GROUP BY ct.id, ct.city, ct.listing_city_id, rg.id, rg.region, rg.listing_region_id, pt.type_name
      ORDER BY ct.city ASC
      LIMIT 5
    `,
  });

  return c.json({
    data: cities.rows,
  });
});

app.post("/api/properties/favorites/:propertyId", async (c: Context) => {
  using client = await dbPool.connect();
  const propertyId = c.req.param("propertyId");
  const { userId } = await c.req.json();

  if (!propertyId || !userId) {
    return c.json({ error: "Property ID and User ID are required" }, 400);
  }

  // First check if favorite already exists
  const existingFavorite = await client.queryObject({
    args: [userId, propertyId],
    text: `
      SELECT id, user_id, property_id, added_at 
      FROM User_Favorites
      WHERE user_id = $1 AND property_id = $2
    `,
  });

  if (existingFavorite?.rowCount && existingFavorite.rowCount > 0) {
    return c.json({
      data: existingFavorite.rows[0],
    });
  }

  // Create new favorite if it doesn't exist
  const result = await client.queryObject({
    args: [userId, propertyId],
    text: `
      INSERT INTO User_Favorites (user_id, property_id)
      VALUES ($1, $2)
      RETURNING id, user_id, property_id, added_at
    `,
  });

  return c.json({
    data: result.rows[0],
  });
});

app.delete("/api/properties/favorites/:propertyId", async (c: Context) => {
  using client = await dbPool.connect();
  const propertyId = c.req.param("propertyId");
  const { userId } = await c.req.json();

  if (!propertyId || !userId) {
    return c.json({ error: "Property ID and User ID are required" }, 400);
  }

  const result = await client.queryObject({
    args: [userId, propertyId],
    text: `
      DELETE FROM User_Favorites
      WHERE user_id = $1 AND property_id = $2
      RETURNING id
    `,
  });

  if (result.rowCount === 0) {
    return c.json({ error: "Favorite not found" }, 404);
  }

  return c.json({
    data: { success: true },
  });
});

app.get("/api/properties/:userId/favorites", async (c: Context) => {
  using client = await dbPool.connect();
  const userId = c.req.param("userId");

  if (!userId) {
    return c.json({ error: "User ID is required" }, 400);
  }

  const favorites = await client.queryObject({
    args: [userId],
    text: `
      SELECT
          pt.type_name AS property_type,
          json_agg(json_build_object(
              'listing_id', l.id,  -- Adding listing_id
              'formatted_price', l.price_formatted,
              'images', p.images,
              'title', l.title,
              'listing_address', json_build_object(
                  'address', l.address,
                  'region', r.region,
                  'city', c.city,
                  'area', a.area
              ),
              'offer_type', lt.type_name
          ) ORDER BY l.title) AS favorites
      FROM User_Favorites uf
      JOIN Property p ON uf.property_id = p.id
      JOIN Property_Type pt ON p.property_type_id = pt.property_type_id
      JOIN Listing l ON p.id = l.id
      JOIN Listing_Region r ON p.listing_region_id = r.id
      JOIN Listing_City c ON p.listing_city_id = c.id
      LEFT JOIN Listing_Area a ON p.listing_area_id = a.id
      JOIN Listing_Type lt ON l.offer_type_id = lt.listing_type_id
      WHERE uf.user_id = 1  -- Replace with the actual user ID
      GROUP BY pt.type_name
      ORDER BY pt.type_name;
    `,
  });

  return c.json({
    data: favorites.rows,
  });
});

app.patch("/api/properties/:id/generate-ai-description", async (c: Context) => {
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
          l.description,
          l.is_scraped,
          l.price,
          l.price_formatted,
          l.price_not_shown,
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
          p.project_name AS property_project_name,
          p.agent_name,
          p.product_owner_name,
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

  const aiGeneratedDescription = await openaiAssistant(
    JSON.stringify(property.rows[0]),
  );

  const propertyData = property.rows[0] as {
    property_id: number;
  };

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
        propertyData.property_id,
      ],
      text: `UPDATE Property SET ai_generated_description = $1 WHERE id = $2`,
    });
  }

  return c.json({
    property_id: propertyData.property_id,
    ai_generated_description: aiGeneratedDescription,
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
          l.description,
          l.is_scraped,
          l.price,
          l.price_formatted,
          l.price_not_shown,
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
          p.project_name AS property_project_name,
          p.agent_name,
          p.product_owner_name,
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
