import psycopg2

def loading_database():


    # Database connection details
    host = 'testtech.postgres.database.azure.com'
    port = '5432'
    database = 'postgres'
    user = 'testtech'
    password = 'George1234'

    # CSV file details
    csv_file_path = 'path_to_your_csv_file.csv'
    table_name = 'your_table_name'

    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password
    )

    # Open a cursor to perform database operations
    cursor = conn.cursor()

    # Create the table if it doesn't exist (skip this step if the table is already created)
    create_table_query = """
-- Table: countries
CREATE TABLE IF NOT EXISTS countries (
    country_code CHAR(2) NOT NULL,
    country_name VARCHAR(255) NOT NULL,
    continent VARCHAR(255) NOT NULL,
    PRIMARY KEY (country_code)
);

-- Table: regions
CREATE TABLE IF NOT EXISTS regions (
    region_code VARCHAR(4) NOT NULL,
    iso_country CHAR(2) NOT NULL,
    region_name VARCHAR(255) NOT NULL,
    continent VARCHAR(255) NOT NULL,
    PRIMARY KEY (region_code),
    FOREIGN KEY (iso_country) REFERENCES countries (country_code)
);

-- Table: airports
CREATE TABLE IF NOT EXISTS airports (
    airport_id VARCHAR(10) NOT NULL,
    type VARCHAR(255) NOT NULL,
    airport_name VARCHAR(255) NOT NULL,
    latitude_deg FLOAT NOT NULL,
    longitude_deg FLOAT NOT NULL,
    elevation_ft INT,
    iso_country CHAR(2) NOT NULL,
    iso_region VARCHAR(4) NOT NULL,
    municipality VARCHAR(255),
    scheduled_service VARCHAR(10),
    gps_code VARCHAR(10),
    iata_code VARCHAR(3),
    local_code VARCHAR(10),
    PRIMARY KEY (airport_id),
    FOREIGN KEY (iso_country) REFERENCES countries (country_code),
    FOREIGN KEY (iso_region) REFERENCES regions (region_code)
);

-- Table: airport_frequencies
CREATE TABLE IF NOT EXISTS airport_frequencies (
    airport_frequencies_id INT NOT NULL,
    airport_id VARCHAR(10) NOT NULL,
    freq_type VARCHAR(10),
    description VARCHAR(255),
    frequency_mhz DECIMAL(9,6),
    PRIMARY KEY (airport_frequencies_id),
    FOREIGN KEY (airport_id) REFERENCES airports (airport_id)
);

-- Table: runways
CREATE TABLE IF NOT EXISTS runways (
    runway_id INT NOT NULL,
    airport_id VARCHAR(10) NOT NULL,
    length_ft INT,
    width_ft INT,
    surface VARCHAR(255),
    lighted INT,
    closed INT,
    PRIMARY KEY (runway_id),
    FOREIGN KEY (airport_id) REFERENCES airports (airport_id)
);

-- Table: runways_lower_elevation
CREATE TABLE IF NOT EXISTS runways_le (
    le_id INT NOT NULL,
    runway_id INT NOT NULL,
    le_latitude_deg FLOAT,
    le_longitude_deg FLOAT,
    le_elevation_ft INT,
    le_heading_degT INT,
    le_displaced_threshold_ft INT,
    PRIMARY KEY (le_id),
    FOREIGN KEY (runway_id) REFERENCES runways (runway_id)
);

-- Table: runways_higher_elevation
CREATE TABLE IF NOT EXISTS runways_he (
    he_id INT NOT NULL,
    runway_id INT NOT NULL,
    he_latitude_deg FLOAT,
    he_longitude_deg FLOAT,
    he_elevation_ft INT,
    he_heading_degT INT,
    he_displaced_threshold_ft INT,
    PRIMARY KEY (he_id),
    FOREIGN KEY (runway_id) REFERENCES runways (runway_id)
);

-- Table: navaids
CREATE TABLE IF NOT EXISTS navaids (
    navaid_id INT PRIMARY KEY ,
    filename VARCHAR(255),
    ident VARCHAR(10),
    type VARCHAR(255),
    name VARCHAR(255),
    frequency_khz INT,
    latitude_deg FLOAT,
    longitude_deg FLOAT,
    elevation_ft INT,
    iso_country CHAR(2),
    slaved_variation FLOAT,
    magnetic_variation FLOAT,
    usage_type VARCHAR(255),
    power FLOAT,
    associated_airport_ident VARCHAR(10),
    FOREIGN KEY (associated_airport_ident) REFERENCES airports (airport_id),
    FOREIGN KEY (iso_country) REFERENCES countries (country_code)
);

--Table: distance-measuring equipment
CREATE TABLE IF NOT EXISTS DME(
    dme_id SERIAL PRIMARY KEY,
    navaid_id INT,
    dme_frequency DECIMAL(9,6),
    dme_channel VARCHAR(255),
    dme_latitude_deg FLOAT,
    dme_longitude_deg FLOAT,
    dme_elevation_ft INT,
    FOREIGN KEY (navaid_id) REFERENCES navaids (navaid_id)
);

--Table: airport_comments
CREATE TABLE IF NOT EXISTS airport_comments(
    airport_comments_id INT PRIMARY KEY,
    airport_id VARCHAR(10),
    threadRef INT,
    comment_date DATE,
    memberNickname VARCHAR,
    subject VARCHAR,
    body VARCHAR,
    FOREIGN KEY (airport_id) REFERENCES airports (airport_id)
);

--Table: GEO countries
CREATE TABLE IF NOT EXISTS geo_countries(
    geo_countries_id INT PRIMARY KEY,
    areas INT,
    country VARCHAR,
    country_code VARCHAR ,
    Area_in_kmsq VARCHAR,
    Names_per_kmsq VARCHAR,
    population INT,
    Names_per1000_Inhabitants FLOAT,
    FOREIGN KEY (country_code) REFERENCES countries (country_code)
);

-- Table: Geoname
CREATE TABLE IF NOT EXISTS geoname (
    geoname_id INT PRIMARY KEY,
    name VARCHAR(255),
    asciiname VARCHAR(255),
    alternatenames VARCHAR(255),
    latitude FLOAT,
    longitude FLOAT,
    feature_class CHAR(1),
    feature_code VARCHAR(10),
    country_code CHAR(2),
    altcountry_code CHAR(2),
    admin1_code VARCHAR(20),
    admin2_code VARCHAR(80),
    admin3_code VARCHAR(20),
    admin4_code VARCHAR(20),
    population BIGINT,
    elevation INT,
    DEM INT,
    timezone VARCHAR(40),
    modification_date DATE
);
    """
    cursor.execute(create_table_query)
    conn.commit()
    print('done')
    # Import the CSV file into the table
    import_query = f"""
        COPY {table_name}
        FROM '{csv_file_path}'
        DELIMITER ','
        CSV HEADER;
    """
    #cursor.execute(import_query)
    #conn.commit()

    # Close the cursor and database connection
    cursor.close()
    conn.close()

loading_database()