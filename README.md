# metrics-service
# DataONE Metrics Service
An efficient database and REST API for delivering aggregated data set metrics to clients.

[![Build Status](https://travis-ci.org/DataONEorg/metrics-service.svg)](https://travis-ci.org/DataONEorg/metrics-service)
[![metrics-service](https://img.shields.io/badge/metrics--service-0.0.1-blue.svg)](http://github.com/DataONEorg/metrics-service)

- **Contributors**: Rushiraj Nenuji (nenuji@nceas.ucsb.edu), Chris Jones (cjones@nceas.ucsb.edu), Lauren Walker (walker@nceas.ucsb.edu), Matthew B. Jones (jones@nceas.ucsb.edu), Dave Vieglais (vieglais@nceas.ucsb.edu)
- **Version**: 0.0.1 (**In development, not yet released**)
- **Bug reports**: http://github.com/DataONEorg/metrics-service/issues
- **Task Board**: https://waffle.io/DataONEorg/metrics-service
- **Source code**: http://github.com/DataONEorg/metrics-service
- **Slack Discussion channel**: #ci on http://slack.dataone.org

The DataONE Metrics Service represents a facility for storing, aggregating,
and accessing metrics about the data held in the DataONE Federation of data
repositories.  The service consists of two main components:

- Database of aggregated metrics
- REST API for accessing the database

The database is currently implemented as a PostgresQL database and can
be installed in any recent postgres system.

The REST API service provides web-accessible methods for querying and downloading
metrics data in aggregated form.

## Copyright and License
Copyright: 2018 Regents of the University of California

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

## Funding and Acknowledgements

This material is based upon work supported by the Alfred P. Sloan Foundation
as part of the Make Data Count Project. https://makedatacount.org
