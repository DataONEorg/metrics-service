/*
 * Script for loading the core metrics tables in an empty database.
 */

/*
 * db_version -- table to store the version history of this database
 */
CREATE SEQUENCE db_version_id_seq;
CREATE TABLE db_version (
  db_version_id   INT8 default nextval ('db_version_id_seq'), -- the identifier for the version
  version         VARCHAR(250),     -- the version number
  status          INT8,             -- status of the version, 1 if its the current version
  CONSTRAINT db_version_pk PRIMARY KEY (db_version_id)
);
INSERT into db_version (version, status) VALUES ('0.0.1', 1);
