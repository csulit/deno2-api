CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;

CREATE TABLE Property_Type (
    property_type_id SERIAL PRIMARY KEY,
    type_name VARCHAR(255) NOT NULL UNIQUE
);

CREATE TABLE Warehouse_Type (
    warehouse_type_id SERIAL PRIMARY KEY,
    type_name VARCHAR(255) NOT NULL UNIQUE
);

CREATE TABLE Listing_Type (
    listing_type_id SERIAL PRIMARY KEY,
    type_name VARCHAR(255) NOT NULL UNIQUE
);

CREATE TABLE Property_Status (
    status_id SERIAL PRIMARY KEY,
    status_name VARCHAR(255) NOT NULL UNIQUE
);

CREATE TABLE Listing_Region (
    id SERIAL PRIMARY KEY,
    listing_region_id VARCHAR(255) NOT NULL UNIQUE,
    region VARCHAR(255) NOT NULL UNIQUE
);

CREATE TABLE Listing_City (
    id SERIAL PRIMARY KEY,
    listing_city_id VARCHAR(255) NOT NULL UNIQUE,
    city VARCHAR(255) NOT NULL UNIQUE,
    listing_region_id INT NOT NULL REFERENCES Listing_Region(id) ON DELETE CASCADE
);

CREATE TABLE Listing_Area (
    id SERIAL PRIMARY KEY,
    listing_area_id VARCHAR(255) NOT NULL UNIQUE,
    area VARCHAR(255) NOT NULL
);

CREATE TABLE "User" (
    user_id SERIAL PRIMARY KEY,
    clerk_id VARCHAR(255) NOT NULL UNIQUE,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    email VARCHAR(255) UNIQUE,
    phone VARCHAR(20),
    role VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE Property (
    id SERIAL PRIMARY KEY,
    user_id INT REFERENCES "User"(user_id),
    floor_size DOUBLE PRECISION NOT NULL DEFAULT 0,
    lot_size DOUBLE PRECISION NOT NULL DEFAULT 0,
    building_size DOUBLE PRECISION NOT NULL DEFAULT 0,
    ceiling_height DOUBLE PRECISION NOT NULL DEFAULT 0,
    no_of_bedrooms INT NOT NULL DEFAULT 0,
    no_of_bathrooms INT NOT NULL DEFAULT 0,
    no_of_parking_spaces INT NOT NULL DEFAULT 0,
    longitude FLOAT NOT NULL,
    latitude FLOAT NOT NULL,
    year_built INT,
    primary_image_url VARCHAR(255),
    images JSONB,
    amenities JSONB,
    property_features JSONB,
    indoor_features JSONB,
    outdoor_features JSONB,
    ai_generated_description JSONB,
    ai_generated_basic_features JSONB,
    property_type_id INT NOT NULL REFERENCES Property_Type(property_type_id),
    warehouse_type_id INT REFERENCES Warehouse_Type(warehouse_type_id),
    address VARCHAR(255),
    listing_region_id INT NOT NULL REFERENCES Listing_Region(id) ON DELETE CASCADE,
    listing_city_id INT NOT NULL REFERENCES Listing_City(id) ON DELETE CASCADE,
    listing_area_id INT REFERENCES Listing_Area(id) ON DELETE CASCADE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    geog GEOGRAPHY(Point, 4326),
    project_name VARCHAR(100),
    agent_name VARCHAR(100),
    product_owner_name VARCHAR(100),
    CONSTRAINT check_floor_size CHECK (floor_size >= 0),
    CONSTRAINT check_lot_size CHECK (lot_size >= 0),
    CONSTRAINT check_building_size CHECK (building_size >= 0),
    CONSTRAINT check_ceiling_height CHECK (ceiling_height >= 0)
);

CREATE TABLE Listing (
    id SERIAL PRIMARY KEY,
    title VARCHAR(255) NOT NULL UNIQUE,
    url VARCHAR(255) NOT NULL UNIQUE,
    project_name VARCHAR(100),
    description TEXT NOT NULL,
    is_scraped BOOLEAN NOT NULL DEFAULT FALSE,
    address VARCHAR(255),
    price_formatted VARCHAR(255),
    price DOUBLE PRECISION NOT NULL CHECK (price >= 0),
    offer_type_id INT NOT NULL REFERENCES Listing_Type(listing_type_id),
    property_id INT NOT NULL REFERENCES Property(id) ON DELETE CASCADE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE Price_Change_Log (
   id SERIAL PRIMARY KEY,
   listing_id INTEGER NOT NULL,
   old_price DOUBLE PRECISION,
   new_price DOUBLE PRECISION NOT NULL,
   change_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
   FOREIGN KEY (listing_id) REFERENCES Listing(id)
);

CREATE TABLE Lamudi_raw_data (
    id SERIAL PRIMARY KEY,
    json_data JSONB,
    listingUrl TEXT,
    images JSONB,
    is_process BOOLEAN default false,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_property_type_id ON Property(property_type_id);
CREATE INDEX idx_listing_region_id ON Property(listing_region_id);
CREATE INDEX idx_listing_city_id ON Property(listing_city_id);
CREATE INDEX idx_listing_area_id ON Property(listing_area_id);
CREATE INDEX idx_listing_price ON Listing(price);
CREATE INDEX idx_listing_created_at ON Listing(created_at);
CREATE INDEX idx_property_geog ON Property USING GIST(geog);
CREATE INDEX idx_property_amenities ON Property USING GIN (amenities);
CREATE INDEX idx_property_location ON Property(longitude, latitude);
CREATE INDEX idx_property_property_features ON Property USING GIN (property_features);
CREATE INDEX idx_property_indoor_features ON Property USING GIN (indoor_features);
CREATE INDEX idx_property_outdoor_features ON Property USING GIN (outdoor_features);
CREATE INDEX idx_property_ai_generated_basic_features ON Property USING GIN (ai_generated_basic_features);
CREATE INDEX idx_title_trgm ON Listing USING gin (title gin_trgm_ops);
CREATE INDEX idx_description_trgm ON Listing USING gin (description gin_trgm_ops);

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_property_timestamp
BEFORE UPDATE ON Property
FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_listing_timestamp
BEFORE UPDATE ON Listing
FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE OR REPLACE FUNCTION update_geog()
RETURNS TRIGGER AS $$
BEGIN
    NEW.geog = ST_SetSRID(ST_MakePoint(NEW.longitude, NEW.latitude), 4326)::geography;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_geog_trigger
BEFORE INSERT OR UPDATE ON Property
FOR EACH ROW EXECUTE FUNCTION update_geog();

CREATE OR REPLACE FUNCTION log_price_change()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.price IS DISTINCT FROM NEW.price THEN
        INSERT INTO Price_change_log (listing_id, old_price, new_price, change_timestamp)
        VALUES (OLD.id, OLD.price, NEW.price, CURRENT_TIMESTAMP);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_price_change
AFTER UPDATE ON Listing
FOR EACH ROW
WHEN (OLD.price IS DISTINCT FROM NEW.price)
EXECUTE FUNCTION log_price_change();

CREATE OR REPLACE FUNCTION check_images_format()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.images IS NOT NULL THEN
        -- Raise an exception if the images are not an array
        IF jsonb_typeof(NEW.images) <> 'array' THEN
            RAISE EXCEPTION 'images must be a JSON array';
        END IF;

        -- Raise an exception if any element is not a valid URL
        PERFORM 1
        FROM jsonb_array_elements_text(NEW.images) AS elem
        WHERE elem::text = '' OR elem !~ '^https?://';

        IF FOUND THEN
            RAISE EXCEPTION 'All elements of images must be non-empty strings and valid URLs';
        END IF;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER check_images_trigger
BEFORE INSERT OR UPDATE ON property
FOR EACH ROW
EXECUTE FUNCTION check_images_format();

-- Step 1: Create the function that deletes rows with is_process = true
CREATE OR REPLACE FUNCTION delete_processed_rows() RETURNS TRIGGER AS $$
BEGIN
    IF NEW.is_process = TRUE THEN
        DELETE FROM Lamudi_raw_data WHERE id = NEW.id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Step 2: Create the trigger that fires the function after an update
CREATE TRIGGER trigger_delete_processed
AFTER UPDATE ON Lamudi_raw_data
FOR EACH ROW
WHEN (NEW.is_process = TRUE)
EXECUTE FUNCTION delete_processed_rows();

CREATE OR REPLACE FUNCTION listd_dev.public.clean_special_characters(text)
RETURNS text AS $$
BEGIN
    RETURN REGEXP_REPLACE($1, '[^a-zA-Z0-9 ]', '', 'g');
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION listd_dev.public.clean_property_description()
RETURNS TRIGGER AS $$
BEGIN
    NEW.ai_generated_description = listd_dev.public.clean_special_characters(NEW.ai_generated_description::text)::jsonb;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER clean_description_trigger
BEFORE INSERT OR UPDATE ON listd_dev.public.property
FOR EACH ROW
EXECUTE FUNCTION listd_dev.public.clean_property_description();

EXPLAIN ANALYZE
WITH params AS (
    SELECT
        -73.935242::numeric AS search_longitude,
        40.73061::numeric AS search_latitude,
        10::numeric AS max_distance_km
),
search_point AS (
    SELECT
        ST_SetSRID(ST_MakePoint(search_longitude, search_latitude), 4326)::geography AS geog,
        max_distance_km
    FROM params
)
SELECT
    p.id,
    p.longitude,
    p.latitude,
    ST_Distance(search_point.geog, p.geog) / 1000 AS distance_km
FROM
    Property p,
    search_point
WHERE
    ST_DWithin(p.geog, search_point.geog, search_point.max_distance_km * 1000)
ORDER BY
    distance_km;


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
    json_data->>'images' AS images,
    array(
        SELECT jsonb_array_elements(images) ->> 'src'
        FROM lamudi_raw_data
        WHERE id = lamudi_raw_data.id
    ) AS image_src_urls
FROM lamudi_raw_data
WHERE is_process = FALSE
    AND json_data->'dataLayer'->'location'->>'region' IS NOT NULL
    AND json_data->'dataLayer'->'location'->>'city' IS NOT NULL
    AND json_data->'dataLayer'->'attributes'->>'listing_area' IS NOT NULL
LIMIT 10
