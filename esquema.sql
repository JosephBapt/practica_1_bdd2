-- 1. Dimensión de Tiempo (¡La clave para no sobrescribir meses!)
CREATE TABLE dim_time (
    time_key INTEGER PRIMARY KEY AUTOINCREMENT,
    fecha_reporte DATE UNIQUE, -- Ej: '2025-12-01'
    anio INTEGER,
    mes INTEGER
);

-- 2. Dimensión de Ubicación
CREATE TABLE dim_location (
    location_key INTEGER PRIMARY KEY AUTOINCREMENT,
    address TEXT,
    city TEXT,
    state TEXT,
    postal_code TEXT,
    country TEXT,
    region TEXT,
    UNIQUE(address, city, country)
);

-- 3. Dimensión de Proveedor
CREATE TABLE dim_supplier (
    supplier_key INTEGER PRIMARY KEY AUTOINCREMENT,
    supplier_group TEXT UNIQUE
);

-- 4. Dimensión de Fábrica
CREATE TABLE dim_factory (
    factory_key INTEGER PRIMARY KEY AUTOINCREMENT,
    factory_name TEXT UNIQUE,
    factory_type TEXT,
    product_type TEXT
);

-- 5. Tabla de Hechos (Donde ocurre la magia mensual)
CREATE TABLE fact_manufacturing (
    fact_id INTEGER PRIMARY KEY AUTOINCREMENT,
    factory_key INTEGER REFERENCES dim_factory(factory_key),
    location_key INTEGER REFERENCES dim_location(location_key),
    supplier_key INTEGER REFERENCES dim_supplier(supplier_key),
    time_key INTEGER REFERENCES dim_time(time_key),
    
    -- Metadatos adicionales del mes
    brands TEXT,
    events TEXT,
    
    -- Métricas (Hechos)
    total_workers INTEGER,
    line_workers INTEGER,
    pct_female REAL,
    pct_migrant REAL,
    
    -- Esta restricción asegura que si corres diciembre dos veces, no se dupliquen los datos
    UNIQUE(factory_key, time_key) 
);
