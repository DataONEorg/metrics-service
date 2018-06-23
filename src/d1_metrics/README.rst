Metrics Service Library and Tools
=================================

Implements libraries for working with the metrics service and command line tools
for administering and inspecting metrics content.


Installation
------------

Needs Python3, postgresql, elasticsearch.

Install the python bits::

  pip install -e .


Elastic Search Actions
----------------------

Show the amount of work to do for session calculations::

   d1metricses -l compute -Y


Compute session info::

   d1metricses -l compute -Y



Database Actions
----------------

Create a database::

  sudo -u postgres createuser -E -P metrics_user
  sudo -u postgres createdb -E UTF8 metrics
  sudo -u postgres psql metrics
    psql=# GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO metrics_user;


Initialize the database::

  d1metricsdb -c config.ini --sqlinit ../sql/*.sql initialize


Check the status of the database::

  d1metricsdb -c config.ini check
  { 'awardcharts': 0,
    'awardmetrics': 0,
    'landingpages': 0,
    'repocharts': 0,
    'repometrics': 0,
    'rows': 0,
    'userprofilecharts': 0,
    'userprofilemetrics': 0,
    'version': '0.0.1'}


Scripts installed are listed in setup.py under ``entry_points.console_scripts``.

