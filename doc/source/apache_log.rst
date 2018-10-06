Log Formatting
==============


Apache2 log formatting for the search UI.


Logging search events::

  LogFormat "{ \"time\":\"%{%Y-%m-%d}tT%{%T}t.%{msec_frac}tZ\", \"remoteIP\":\"%a\",
               \"request\":\"%U\", \"query\":\"%q\", \"method\":\"%m\", \"status\":\"%>s\",
               \"userAgent\":\"%{User-agent}i\", \"accessToken\":\"%{Authorization}i\",
               \"referer\":\"%{Referer}i\", \"remoteuser\":\"%u\"}" searchstats


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
    "accessToken": "",
    "status": ""
  }

Time formatting::

  %{%Y-%m-%d}tT%{%T}t.%{msec_frac}tZ

ipAddress::

  %a

method::

  %m

request::

  %U

query::

  %q

userAgent::

  %{User-agent}i

remoteUser::

  %u

status::

  %>s

accessToken::

  %{Authorization}i


The accessToken can be decoded using pyjwt for example::

  accessToken = "Bearer eyJhbGciOiJSUzI1NiJ ..."
  junk, token = accessToken.split(" ")
  print( jwt.decode(token, verify=False) )
  {'sub': 'http://orcid.org/0000-...',
    'fullName': 'Dave Vieglais',
    'issuedAt': '2018-10-06T12:28:44.156+00:00',
    'consumerKey': '...',
    'exp': 1538893724,
    'userId': 'http://orcid.org/0000-...',
    'ttl': 64800,
    'iat': 1538...}

See also:

* `Apache log files`_ for general information on configuring Apache logs
* `mod_log_config`_ for log format options


.. _Apache log files: https://httpd.apache.org/docs/2.4/logs.html
.. _mod_log_config: https://httpd.apache.org/docs/2.4/mod/mod_log_config.html
