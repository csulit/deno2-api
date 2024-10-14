-- Enable PostGIS extension
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;

-- Create ENUM types
CREATE TYPE offer_type AS ENUM ('Sale', 'Rent');

-- Create tables for lookup data
CREATE TABLE Property_Type (
    property_type_id SERIAL PRIMARY KEY,
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

-- Create main tables
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
    amenities JSONB,
    property_features JSONB,
    indoor_features JSONB,
    outdoor_features JSONB,
    ai_generated_description TEXT,
    ai_generated_basic_features JSONB,
    property_type_id INT NOT NULL REFERENCES Property_Type(property_type_id),
    warehouse_type VARCHAR(255),
    json_data JSONB,
    address VARCHAR(255),
    listing_region_id INT NOT NULL REFERENCES Listing_Region(id) ON DELETE CASCADE,
    listing_city_id INT NOT NULL REFERENCES Listing_City(id) ON DELETE CASCADE,
    listing_area_id INT NOT NULL REFERENCES Listing_Area(id) ON DELETE CASCADE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    geog GEOGRAPHY(Point, 4326),
    CONSTRAINT unique_location UNIQUE (latitude, longitude),
    CONSTRAINT check_longitude CHECK (longitude BETWEEN -180 AND 180),
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
    price DOUBLE PRECISION NOT NULL CHECK (price >= 0),
    offer_type offer_type NOT NULL,
    listing_city_id INT REFERENCES Listing_City(id) ON DELETE CASCADE,
    listing_region_id INT REFERENCES Listing_Region(id) ON DELETE CASCADE,
    listing_area_id INT REFERENCES Listing_Area(id) ON DELETE CASCADE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes
CREATE INDEX idx_property_type_id ON Property(property_type_id);
CREATE INDEX idx_listing_region_id ON Property(listing_region_id);
CREATE INDEX idx_listing_city_id ON Property(listing_city_id);
CREATE INDEX idx_listing_area_id ON Property(listing_area_id);
CREATE INDEX idx_listing_price ON Listing(price);
CREATE INDEX idx_listing_created_at ON Listing(created_at);
CREATE INDEX idx_property_geog ON Property USING GIST(geog);

-- Create functions
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_geog()
RETURNS TRIGGER AS $$
BEGIN
    NEW.geog = ST_SetSRID(ST_MakePoint(NEW.longitude, NEW.latitude), 4326)::geography;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create triggers
CREATE TRIGGER update_property_timestamp
BEFORE UPDATE ON Property
FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_listing_timestamp
BEFORE UPDATE ON Listing
FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_geog_trigger
BEFORE INSERT OR UPDATE ON Property
FOR EACH ROW EXECUTE FUNCTION update_geog();

-- Update existing data (if needed)
UPDATE Property
SET geog = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)::geography
WHERE id > 0;

-- Sample query (EXPLAIN ANALYZE)
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