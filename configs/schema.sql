-- Esquema de tablas destino para el pipeline de normatividad ANI

CREATE TABLE IF NOT EXISTS regulations (
    id SERIAL PRIMARY KEY,
    title VARCHAR(128) NOT NULL,
    created_at DATE NOT NULL,
    update_at TIMESTAMP NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    gtype VARCHAR(32),
    entity VARCHAR(128) NOT NULL,
    external_link TEXT,
    rtype_id INTEGER NOT NULL,
    summary TEXT,
    classification_id INTEGER
);

-- Un índice único basado en expresión evita duplicados incluso con external_link nulo.
CREATE UNIQUE INDEX IF NOT EXISTS regulations_unique_idx
ON regulations (title, created_at, (COALESCE(external_link, '')));

CREATE TABLE IF NOT EXISTS regulations_component (
    id SERIAL PRIMARY KEY,
    regulations_id INTEGER NOT NULL REFERENCES regulations(id) ON DELETE CASCADE,
    components_id INTEGER NOT NULL
);
