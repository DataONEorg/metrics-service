'''
This package implements a tool to persist CN log aggregation records to json records on disk.

The basic workflow is:

session_states = loadSessionStatesFromPreviousRecords()
records = loadRecordsFromSolrIndex( start_date )
for record in records:
  record = correctKnownErrorsInRecord( record )
  record = computeSessionInformation( session_states )
  json_record = json.dumps(record)
  writeJsonToLogFileWithRotation( json_record )

session_states is a data structure that tracks a session, which is the interactions by a user
with the system for a particular type of event. Sessions may be computed for different
lengths of time and different types of event. Six types of session are tracked:

             Event Type
Duration     Any         Read
      10     sess_a10    sess_r10
      30     sess_a30    sess_r30
      60     sess_a60    sess_r60

Only sess_r60 is needed for counter compliance.

'''