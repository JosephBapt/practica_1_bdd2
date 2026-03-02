const fs = require("node:fs");
const csv = require("csv-parser");
const { DatabaseSync } = require("node:sqlite");

const mesesMap = {
  JAN: "01", FEB: "02", MAR: "03", APR: "04", MAY: "05", JUN: "06",
  JUL: "07", AUG: "08", SEP: "09", OCT: "10", NOV: "11", DEC: "12",
};

function inicializarBaseDeDatos() {
  const db = new DatabaseSync("./datawarehouse_nike.db");

  // Creación del Esquema de Estrella (Dimensiones y Hechos)
  db.exec(`
    CREATE TABLE IF NOT EXISTS dim_time (
      time_key INTEGER PRIMARY KEY AUTOINCREMENT,
      fecha_reporte DATE UNIQUE,
      anio INTEGER,
      mes INTEGER
    );

    CREATE TABLE IF NOT EXISTS dim_location (
      location_key INTEGER PRIMARY KEY AUTOINCREMENT,
      address TEXT,
      city TEXT,
      state TEXT,
      postal_code TEXT,
      country TEXT,
      region TEXT,
      UNIQUE(address, city, country)
    );

    CREATE TABLE IF NOT EXISTS dim_supplier (
      supplier_key INTEGER PRIMARY KEY AUTOINCREMENT,
      supplier_group TEXT UNIQUE
    );

    CREATE TABLE IF NOT EXISTS dim_factory (
      factory_key INTEGER PRIMARY KEY AUTOINCREMENT,
      factory_name TEXT UNIQUE,
      factory_type TEXT,
      product_type TEXT
    );

    CREATE TABLE IF NOT EXISTS fact_manufacturing (
      fact_id INTEGER PRIMARY KEY AUTOINCREMENT,
      factory_key INTEGER REFERENCES dim_factory(factory_key),
      location_key INTEGER REFERENCES dim_location(location_key),
      supplier_key INTEGER REFERENCES dim_supplier(supplier_key),
      time_key INTEGER REFERENCES dim_time(time_key),
      brands TEXT,
      events TEXT,
      total_workers INTEGER,
      line_workers INTEGER,
      pct_female REAL,
      pct_migrant REAL,
      UNIQUE(factory_key, time_key) 
    );
  `);

  return db;
}

function procesarArchivoMensual(rutaArchivo) {
  // 1. Extracción robusta de la fecha leyendo todo el archivo como texto
  const contenido = fs.readFileSync(rutaArchivo, "utf-8");
  
  const matchFecha = contenido.match(/Data As Of\s+([A-Z]{3})\s+(\d{4})/i);
  if (!matchFecha) {
    throw new Error("No se pudo extraer la fecha. Verifica que el archivo sea un CSV de texto plano y no un Excel binario.");
  }

  const mesStr = matchFecha[1].toUpperCase();
  const anioStr = matchFecha[2];
  const fechaReporte = `${anioStr}-${mesesMap[mesStr]}-01`;

  console.log(`\n📅 Iniciando carga al Data Warehouse para el periodo: ${fechaReporte}`);

  const db = inicializarBaseDeDatos();
  const registros = [];

  // 2. Precompilar sentencias SQL para máxima velocidad
  // Dimensión Tiempo
  const insertTime = db.prepare(`INSERT OR IGNORE INTO dim_time (fecha_reporte, anio, mes) VALUES (?, ?, ?)`);
  const getTime = db.prepare(`SELECT time_key FROM dim_time WHERE fecha_reporte = ?`);
  
  // Dimensión Ubicación
  const insertLocation = db.prepare(`INSERT OR IGNORE INTO dim_location (address, city, state, postal_code, country, region) VALUES (?, ?, ?, ?, ?, ?)`);
  const getLocation = db.prepare(`SELECT location_key FROM dim_location WHERE address = ? AND city = ? AND country = ?`);
  
  // Dimensión Proveedor
  const insertSupplier = db.prepare(`INSERT OR IGNORE INTO dim_supplier (supplier_group) VALUES (?)`);
  const getSupplier = db.prepare(`SELECT supplier_key FROM dim_supplier WHERE supplier_group = ?`);
  
  // Dimensión Fábrica
  const insertFactory = db.prepare(`INSERT OR IGNORE INTO dim_factory (factory_name, factory_type, product_type) VALUES (?, ?, ?)`);
  const getFactory = db.prepare(`SELECT factory_key FROM dim_factory WHERE factory_name = ?`);
  
  // Tabla de Hechos
  const insertFact = db.prepare(`
    INSERT OR IGNORE INTO fact_manufacturing 
    (factory_key, location_key, supplier_key, time_key, brands, events, total_workers, line_workers, pct_female, pct_migrant) 
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  // 3. Insertar la fecha actual en la dimensión de tiempo
  insertTime.run(fechaReporte, parseInt(anioStr), parseInt(mesesMap[mesStr]));
  const timeKey = getTime.get(fechaReporte).time_key;

  // 4. Leer y parsear el CSV
  fs.createReadStream(rutaArchivo)
    .pipe(csv({ skipLines: 1 }))
    .on("data", (row) => {
      if (row["Factory Name"]) registros.push(row);
    })
    .on("end", () => {
      console.log(`🚀 Se encontraron ${registros.length} registros válidos. Procesando ETL...`);

      db.exec("BEGIN TRANSACTION");

      try {
        for (const fila of registros) {
          // --- A. DIMENSIÓN UBICACIÓN ---
          insertLocation.run(fila["Address"], fila["City"], fila["State"], fila["Postal Code"], fila["Country / Region"], fila["Region"]);
          const locationData = getLocation.get(fila["Address"], fila["City"], fila["Country / Region"]);
          const locationKey = locationData ? locationData.location_key : null;

          // --- B. DIMENSIÓN PROVEEDOR ---
          const nombreProveedor = fila["Supplier Group"] || "Desconocido";
          insertSupplier.run(nombreProveedor);
          const supplierData = getSupplier.get(nombreProveedor);
          const supplierKey = supplierData ? supplierData.supplier_key : null;

          // --- C. DIMENSIÓN FÁBRICA ---
          insertFactory.run(fila["Factory Name"], fila["Factory Type"], fila["Product Type Type"]);
          const factoryData = getFactory.get(fila["Factory Name"]);
          const factoryKey = factoryData ? factoryData.factory_key : null;

          // --- D. TABLA DE HECHOS ---
          const totalWorkers = parseInt(fila["Total Workers"]) || 0;
          const lineWorkers = parseInt(fila["Line Workers"]) || 0;
          const pctFemale = parseFloat(fila["% Female Workers"]) || 0;
          const pctMigrant = parseFloat(fila["% Migrant Workers"]) || 0;

          insertFact.run(
            factoryKey, locationKey, supplierKey, timeKey,
            fila["Nike, Inc. Brand(s)"], fila["Events"],
            totalWorkers, lineWorkers, pctFemale, pctMigrant
          );
        }

        db.exec("COMMIT");
        console.log("✅ ¡ETL completado! Data Warehouse actualizado con éxito.");
      } catch (error) {
        db.exec("ROLLBACK");
        console.error("❌ Error en la transformación/carga. Cambios revertidos:", error.message);
      } finally {
        db.close();
      }
    });
}

// Pasamos el CSV (Asegúrate de que la ruta y el nombre sean correctos)
const nombreArchivo = "./imap_export.csv";
procesarArchivoMensual(nombreArchivo);
