Log Formatting
==============


Apache2 log formatting for the search UI.


Logging search events::

  LogFormat "{ \"time\":\"%t\", \"remoteIP\":\"%a\", \"host\":\"%V\", \"request\":\"%U\", \"query\":\"%q\", \"method\":\"%m\", \"status\":\"%>s\", \"userAgent\":\"%{User-agent}i\", \"referer\":\"%{Referer}i\" }" leapache


Target output is a JSON object per log entry with contents:

.. code-block:: json

  {
    "time":"",
    "ipAddress":"",
    "method":"",
    "request":"",
    "query":"",
    "userAgent":"",
    "remoteUser":"",

  }


See also:

* `Apache log files`_ for general information on configuring Apache logs
* `mod_log_config`_ for log format options


.. _Apache log files: https://httpd.apache.org/docs/2.4/logs.html
.. _mod_log_config: https://httpd.apache.org/docs/2.4/mod/mod_log_config.html
