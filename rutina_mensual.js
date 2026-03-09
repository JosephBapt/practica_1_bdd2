const fs = require("node:fs");
const csv = require("csv-parser");
const { Client } = require("pg");

const mesesMap = {
  JAN: "01",
  FEB: "02",
  MAR: "03",
  APR: "04",
  MAY: "05",
  JUN: "06",
  JUL: "07",
  AUG: "08",
  SEP: "09",
  OCT: "10",
  NOV: "11",
  DEC: "12",
};

const dbConfig = {
  database: "datawarehouse",
  user: "postgres",
  password: "password",
  host: "localhost",
  port: 5432,
};

async function inicializarBaseDeDatos(client) {
  await client.query(`
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
  `);
}

async function procesarArchivoMensual(rutaArchivo) {
  const contenido = fs.readFileSync(rutaArchivo, "utf-8");
  const matchFecha = contenido.match(/Data As Of\s+([A-Z]{3})\s+(\d{4})/i);

  if (!matchFecha) {
    throw new Error(
      "No se pudo extraer la fecha.",
    );
  }

  const mesStr = matchFecha[1].toUpperCase();
  const anioStr = matchFecha[2];
  const fechaReporte = `${anioStr}-${mesesMap[mesStr]}-01`;

  console.log(
    `\nCargando el Data Warehouse para el periodo: ${fechaReporte}`,
  );

  const client = new Client(dbConfig);
  await client.connect();

  await inicializarBaseDeDatos(client);
  const registros = [];

  await client.query(
    `
    INSERT INTO dim_time (fecha_reporte, anio, mes) 
    VALUES ($1, $2, $3) 
    ON CONFLICT (fecha_reporte) DO NOTHING
  `,
    [fechaReporte, parseInt(anioStr), parseInt(mesesMap[mesStr])],
  );

  const timeRes = await client.query(
    `SELECT time_key FROM dim_time WHERE fecha_reporte = $1`,
    [fechaReporte],
  );
  const timeKey = timeRes.rows[0].time_key;

  fs.createReadStream(rutaArchivo)
    .pipe(csv({ skipLines: 2, headers: false }))
    .on("data", (row) => {
      if (row[0]) registros.push(row);
    })
    .on("end", async () => {
      console.log(
        `Se encontraron ${registros.length} registros.`,
      );

      await client.query("BEGIN");

      try {
        for (const fila of registros) {
          await client.query(
            `
            INSERT INTO dim_location (address, city, state, postal_code, country, region) 
            VALUES ($1, $2, $3, $4, $5, $6) 
            ON CONFLICT (address, city, country) DO NOTHING
          `,
            [
              fila[6], //adress
              fila[7], //city
              fila[8], //state
              fila[9], //postal code
              fila[10], //country/region
              fila[11], //region
            ],
          );

          const locRes = await client.query(
            `SELECT location_key FROM dim_location WHERE address = $1 AND city = $2 AND country = $3`,
            [fila[6], fila[7], fila[10]],
          );
          const locationKey = locRes.rows[0]
            ? locRes.rows[0].location_key
            : null;

          const nombreProveedor = fila[5] || "Desconocido";
          await client.query(
            `
            INSERT INTO dim_supplier (supplier_group) VALUES ($1) ON CONFLICT (supplier_group) DO NOTHING
          `,
            [nombreProveedor],
          );

          const suppRes = await client.query(
            `SELECT supplier_key FROM dim_supplier WHERE supplier_group = $1`,
            [nombreProveedor],
          );
          const supplierKey = suppRes.rows[0]
            ? suppRes.rows[0].supplier_key
            : null;

          await client.query(
            `
            INSERT INTO dim_factory (factory_name, factory_type, product_type) VALUES ($1, $2, $3) ON CONFLICT (factory_name) DO NOTHING
          `,
            [
              fila[0], // factory name
              fila[1], // factory type
              fila[2], // product type
            ],
          );

          const factRes = await client.query(
            `SELECT factory_key FROM dim_factory WHERE factory_name = $1`,
            [fila[0]],
          );
          const factoryKey = factRes.rows[0]
            ? factRes.rows[0].factory_key
            : null;

          const nombreMarca = fila[3] || "Sin Marca";
          await client.query(
            `
            INSERT INTO dim_brand (brand_name) VALUES ($1) ON CONFLICT (brand_name) DO NOTHING
          `,
            [nombreMarca],
          );

          const brandRes = await client.query(
            `SELECT brand_key FROM dim_brand WHERE brand_name = $1`,
            [nombreMarca],
          );
          console.log(brandRes.rows[0]);
          const brandKey = brandRes.rows[0] ? brandRes.rows[0].brand_key : null;

          const totalWorkers = parseInt(fila[12]) || 0;
          const lineWorkers = parseInt(fila[13]) || 0;
          const pctFemale = parseFloat(fila[14]) || 0;
          const pctMigrant = parseFloat(fila[15]) || 0;

          await client.query(
            `
            INSERT INTO fact_manufacturing 
            (factory_key, location_key, supplier_key, time_key, brand_key, events, total_workers, line_workers, pct_female, pct_migrant) 
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (factory_key, time_key) DO NOTHING
          `,
            [
              factoryKey,
              locationKey,
              supplierKey,
              timeKey,
              brandKey,
              fila[4] || "N/A", //events
              totalWorkers,
              lineWorkers,
              pctFemale,
              pctMigrant,
            ],
          );
        }

        await client.query("COMMIT");
        console.log(
          "Data Warehouse actualizado con éxito.",
        );
      } catch (error) {
        await client.query("ROLLBACK");
        console.error(
          "Error en la carga",
          error.message,
        );
      } finally {
        await client.end();
      }
    });
}

const nombreArchivo = "./imap_export.csv";
procesarArchivoMensual(nombreArchivo);
