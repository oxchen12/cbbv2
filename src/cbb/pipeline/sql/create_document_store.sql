CREATE TABLE IF NOT EXISTS Documents (
   document_key VARCHAR,
   record_type VARCHAR,
   document_timestamp TIMESTAMP,
   up_to_date BOOLEAN,
   payload JSON,
);

CREATE TABLE IF NOT EXISTS DiscoveryManifest (
   document_key VARCHAR,
   record_type VARCHAR,
   PRIMARY KEY (document_key, record_type)
);