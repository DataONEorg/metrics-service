ALTER TABLE CITATIONS
  ADD COLUMN citation_status TEXT,
  ADD COLUMN reporter TEXT,
  ADD COLUMN reviewer TEXT,
  ADD COLUMN relation_type TEXT;

ALTER TABLE CITATION_METADATA
  ADD COLUMN portal_id TEXT[];
  ADD COLUMN title TEXT,
  ADD COLUMN datePublished TEXT,
  ADD COLUMN dateUploaded TEXT,
  ADD COLUMN dateModified TEXT;
