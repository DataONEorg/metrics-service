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
    dataset_id TEXT,            -- the identifier for the dataset, source: pid
    user_id TEXT,               -- the identifier for the user, source: rights_holder
    repository TEXT,            -- the identifier for the repository, source: member node
    funding_number TEXT,        -- the funding number under which the dataset was published, source: not available
    award_number TEXT,          -- the award number under which the dataset was published, source: not available
    day INTEGER,                -- day of the occurence of the event, source: dateLogged
    month INTEGER,              -- month of the occurence of the event, source: dateLogged
    year INTEGER,               -- year of the occurence of the event, source: dateLogged
    country_code TEXT,          -- location of the user accessing the dataset
    geohash4 TEXT,              -- Geohash value at level 4.
    metrics_name TEXT,          -- metrics name for given dataset
    metrics_value INTEGER       -- count of metric event for that dataset
);

/*
 * citations table, which stores citations information from the crossref endpoint
 */
create table citations (
    id SERIAL,                    -- identifier of the record
    report  JSON,                 -- the json report object
    metadata  JSON,               -- the json metadata object
    target_id TEXT,               -- target(dataset that was cited) identifier
    source_id TEXT,               -- source identifier
    source_url TEXT,              -- source url
    link_publication_date TEXT,   -- the date when this relation was first recorded by CROSSREF.
    origin TEXT,                  -- authors of the source dataset
    title TEXT,                   -- title of the dataset
    publisher TEXT,               -- publisher e.g. dataONE
    journal TEXT,                 -- journal that published the dataset
    volume TEXT,                  -- volume of the publication containing this document
    page TEXT,                    -- page numbers of the publication containing this document
    year_of_publishing INTEGER,   -- YOP for the source dataset
    primary key(target_id, source_id, link_publication_date)
);


/*
 * Citations SOLR metadata - which will be used for different facets
 */
 create table citation_metadata (
    id SERIAL,                    -- identifier of the record. (not null)
    target_id TEXT NOT NULL ,     -- target(dataset that was cited) identifier. DataONE pid. (not null)
    origin TEXT[],                -- author names associated with the citation.
    node_id TEXT[],               -- Corresponding authoritativeMn for the given target_id (pid)
    awards TEXT[]                 -- Award numbers associated with the pid
    primary key(target_id)
);

/*
 * db_metadata -- table to store arbitrary key, value pairs for application state and configuration
 */
CREATE TABLE db_metadata (
    key TEXT PRIMARY KEY,
    value TEXT
)