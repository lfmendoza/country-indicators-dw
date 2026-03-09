CREATE TABLE IF NOT EXISTS pais_poblacion (
    _id                 VARCHAR(24) NOT NULL,
    continente          VARCHAR(80),
    pais                VARCHAR(100) NOT NULL,
    poblacion           BIGINT,
    costo_bajo_hospedaje    NUMERIC(10,2),
    costo_promedio_comida   NUMERIC(10,2),
    costo_bajo_transporte   NUMERIC(10,2),
    costo_promedio_entretenimiento NUMERIC(10,2),
    created_at          TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_pais_poblacion PRIMARY KEY (_id)
);

CREATE TABLE IF NOT EXISTS pais_envejecimiento (
    id_pais                 INTEGER NOT NULL,
    nombre_pais             VARCHAR(100) NOT NULL,
    capital                 VARCHAR(100),
    continente              VARCHAR(80),
    region                  VARCHAR(100),
    poblacion               NUMERIC(18,2),
    tasa_de_envejecimiento  NUMERIC(6,2),
    created_at              TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_pais_envejecimiento PRIMARY KEY (id_pais)
);
