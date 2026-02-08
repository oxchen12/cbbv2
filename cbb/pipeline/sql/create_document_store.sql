CREATE TABLE IF NOT EXISTS Documents (
   key VARCHAR,
   record_type VARCHAR,
   timestamp TIMESTAMP,
   up_to_date BOOLEAN,
   payload JSON,
);

CREATE TABLE IF NOT EXISTS DiscoveryManifest (
   key VARCHAR,
   record_type VARCHAR,
   PRIMARY KEY (key, record_type)
);