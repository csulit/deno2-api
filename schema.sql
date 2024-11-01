-- Enable PostGIS extensions for geographical data handling
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Core lookup tables for property categorization
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

-- Location hierarchy tables
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

-- User management table
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

-- Main property table storing physical characteristics and metadata
CREATE TABLE Property (
    id SERIAL PRIMARY KEY,
    user_id INT REFERENCES "User"(user_id),

    -- Physical dimensions
    floor_size DOUBLE PRECISION NOT NULL DEFAULT 0,
    lot_size DOUBLE PRECISION NOT NULL DEFAULT 0,
    building_size DOUBLE PRECISION NOT NULL DEFAULT 0,
    ceiling_height DOUBLE PRECISION NOT NULL DEFAULT 0,

    -- Property features
    no_of_bedrooms INT NOT NULL DEFAULT 0,
    no_of_bathrooms INT NOT NULL DEFAULT 0,
    no_of_parking_spaces INT NOT NULL DEFAULT 0,

    -- Location data
    longitude FLOAT NOT NULL,
    latitude FLOAT NOT NULL,
    geog GEOGRAPHY(Point, 4326),

    -- Property details
    year_built INT,
    primary_image_url VARCHAR(255),
    images JSONB,

    -- Features and amenities (stored as JSON)
    amenities JSONB,
    property_features JSONB,
    indoor_features JSONB,
    outdoor_features JSONB,
    ai_generated_description JSONB,
    ai_generated_basic_features JSONB,

    -- Classification and location references
    property_type_id INT NOT NULL REFERENCES Property_Type(property_type_id),
    warehouse_type_id INT REFERENCES Warehouse_Type(warehouse_type_id),
    listing_region_id INT NOT NULL REFERENCES Listing_Region(id) ON DELETE CASCADE,
    listing_city_id INT NOT NULL REFERENCES Listing_City(id) ON DELETE CASCADE,
    listing_area_id INT REFERENCES Listing_Area(id) ON DELETE CASCADE,

    -- Additional metadata
    address VARCHAR(255),
    project_name VARCHAR(100),
    agent_name VARCHAR(100),
    product_owner_name VARCHAR(100),

    -- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Constraints
    CONSTRAINT check_floor_size CHECK (floor_size >= 0),
    CONSTRAINT check_lot_size CHECK (lot_size >= 0),
    CONSTRAINT check_building_size CHECK (building_size >= 0),
    CONSTRAINT check_ceiling_height CHECK (ceiling_height >= 0)
);

-- Listing information table
CREATE TABLE Listing (
    id SERIAL PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    url VARCHAR(255) NOT NULL,
    project_name VARCHAR(100),
    description TEXT NOT NULL,
    is_scraped BOOLEAN NOT NULL DEFAULT FALSE,
    address VARCHAR(255),
    price_formatted VARCHAR(255),
    price NUMERIC(15,2) NOT NULL CHECK (price >= 0),
    price_not_shown BOOLEAN default false,
    offer_type_id INT NOT NULL REFERENCES Listing_Type(listing_type_id),
    property_id INT NOT NULL REFERENCES Property(id) ON DELETE CASCADE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Price history tracking
CREATE TABLE Price_Change_Log (
   id SERIAL PRIMARY KEY,
   listing_id INTEGER NOT NULL,
   old_price DOUBLE PRECISION,
   new_price DOUBLE PRECISION NOT NULL,
   change_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
   FOREIGN KEY (listing_id) REFERENCES Listing(id)
);

-- Raw data storage for Lamudi scraping
CREATE TABLE Lamudi_raw_data (
    id SERIAL PRIMARY KEY,
    json_data JSONB,
    listingUrl TEXT,
    images JSONB,
    is_process BOOLEAN default false,
    price_not_shown_is_process BOOLEAN default false,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for optimizing common queries
CREATE INDEX idx_property_type_id ON Property(property_type_id);
CREATE INDEX idx_listing_region_id ON Property(listing_region_id);
CREATE INDEX idx_listing_city_id ON Property(listing_city_id);
CREATE INDEX idx_listing_area_id ON Property(listing_area_id);
CREATE INDEX idx_listing_price ON Listing(price);
CREATE INDEX idx_listing_title ON Listing(title);
CREATE INDEX idx_listing_url ON Listing(url);
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
CREATE INDEX idx_title_desc_tsvector ON listing USING gin (to_tsvector('english', title || ' ' || description));

-- Trigger function to auto-update timestamps
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

-- Trigger function to auto-update geography column
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

-- Trigger function to log price changes
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

-- Trigger function to validate image format
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

-- Auto-cleanup function for processed Lamudi data
CREATE OR REPLACE FUNCTION delete_processed_rows() RETURNS TRIGGER AS $$
BEGIN
    IF NEW.is_process = TRUE THEN
        DELETE FROM Lamudi_raw_data WHERE id = NEW.id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_delete_processed
AFTER UPDATE ON Lamudi_raw_data
FOR EACH ROW
WHEN (NEW.is_process = TRUE)
EXECUTE FUNCTION delete_processed_rows();
