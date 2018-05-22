/*
 * Script for loading the core metrics tables in an empty database.
 */

/*
 * db_version -- table to store the version history of this database
 */
CREATE SEQUENCE db_version_id_seq;
CREATE TABLE db_version (
  db_version_id   SERIAL,           -- the identifier for the version
  version         VARCHAR(250),     -- the version number
  status          INT8,             -- status of the version, 1 if its the current version
  CONSTRAINT db_version_pk PRIMARY KEY (db_version_id)
);
INSERT into db_version (version, status) VALUES ('0.0.2', 1);
CREATE TABLE metrics(
    serial_no SERIAL,           -- the identifier for the record
    dataset_id TEXT,           -- the identifier for the dataset
    user_id TEXT,           -- the identifier for the user
    repository TEXT,           -- the identifier for the repository (member node)
    funding_number TEXT,           -- the funding number under which the dataset was published
    award_number TEXT,           -- the award number under which the dataset was published
    day INTEGER,           -- day of the occurence of the event
    month INTEGER,           -- month of the occurence of the event
    year INTEGER,           -- year of the occurence of the event
    location TEXT,           -- location of the user accessing the dataset
    metrics_name TEXT,           -- metrics name for given dataset
    metrics_value INTEGER           -- count of metric event for that dataset
);

/*
 * citations table, which stores citations information from the crossref endpoint
 */
create table citations (
    id serial,               -- identifier of the record
    target_id text,          -- target(dataset that was cited) identifier
    source_id text,          -- source id
    relation_type text,      -- relation type between source and target
    source_id_scheme text,   -- type of source id (DOI for now)
    source_id_url text,      -- resolving url of source identifier
    source_type_name text,   -- type of source (e.g. literature)
    source_sub_type text,    -- sub type (e.g. journal, book chapter)
    source_sub_type_schema text,    -- sub type schema (e.g. crossref)
    primary key(target_id, source_id, relation_type)
);

/*
 * db_metadata -- table to store arbitrary key, value pairs for application state and configuration
 */
CREATE TABLE db_metadata (
    key TEXT PRIMARY KEY,
    value TEXT
)