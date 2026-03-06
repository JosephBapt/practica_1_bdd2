CREATE TABLE IF NOT EXISTS dim_time (
  time_key SERIAL PRIMARY KEY,
  fecha_reporte DATE UNIQUE,
  anio INTEGER,
  mes INTEGER
);

CREATE TABLE IF NOT EXISTS dim_location (
  location_key SERIAL PRIMARY KEY,
  address TEXT,
  city TEXT,
  state TEXT,
  postal_code TEXT,
  country TEXT,
  region TEXT,
  UNIQUE(address, city, country)
);

CREATE TABLE IF NOT EXISTS dim_supplier (
  supplier_key SERIAL PRIMARY KEY,
  supplier_group TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_factory (
  factory_key SERIAL PRIMARY KEY,
  factory_name TEXT UNIQUE,
  factory_type TEXT,
  product_type TEXT
);

CREATE TABLE IF NOT EXISTS dim_brand (
  brand_key SERIAL PRIMARY KEY,
  brand_name TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS fact_manufacturing (
  fact_id SERIAL PRIMARY KEY,
  factory_key INTEGER REFERENCES dim_factory(factory_key),
  location_key INTEGER REFERENCES dim_location(location_key),
  supplier_key INTEGER REFERENCES dim_supplier(supplier_key),
  time_key INTEGER REFERENCES dim_time(time_key),
  brand_key INTEGER REFERENCES dim_brand(brand_key),
  events TEXT,
  total_workers INTEGER,
  line_workers INTEGER,
  pct_female REAL,
  pct_migrant REAL,
  UNIQUE(factory_key, time_key) 
);
