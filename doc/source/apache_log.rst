Log Formatting
==============


Apache2 log formatting for the search UI. The DataONE `Search UI`_ is a single page application served from an Apache
web server. All requests issued by the Search UI are proxied back through the apache server, hence the logs for that
server provide a good location for logging requests issued by the service.

.. list-table:: Apache search log properties
   :widths: 10 10 30
   :header-rows: 1

   * - Property
     - Entry
     - Notes
   * - ``ver``
     - ``1.0``
     - Version flag, indicating the revision of the log information. Must be incremented when format changes.
   * - ``time``
     - ``%{%Y-%m-%d}tT%{%T}t.%{msec_frac}tZ``
     - Time that the request started
   * - ``remoteIP``
     - ``%a``
     - IP address of the requestor
   * - ``method``
     - ``%m``
     - HTTP method used for the request
   * - ``request``
     - ``%U``
     - The request portion of the URL, i.e. after host and before query
   * - ``query``
     - ``%q``
     - The query portion of the URL, i.e. after the "?"
   * - ``userAgent``
     - ``%{User-agent}i``
     - Name of the client user agent
   * - ``remoteUser``
     - ``%u``
     - Remote user identity, only if HTTP authentication used.
   * - ``referer``
     - ``%{Referer}i``
     - URL of the page that requested the resource
   * - ``status``
     - ``%>s``
     - HTTP status of the response
   * - ``responseTime``
     - ``%D``
     - The time in microseconds taken by the server to respond
   * - ``accessToken``
     - ``%{Authorization}i``
     - The accessToken can be decoded using pyjwt for example::

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
   * - ``ga_cookie``
     - ``%{_ga}C``
     - The value of the Google Analytics _ga cookie. `More details <https://stackoverflow.com/questions/16102436/what-are-the-values-in-ga-cookie>`_


Apache configuration for logging search events::

  LogFormat "{ \"ver\":\"1.0\", \"time\":\"%{%Y-%m-%d}tT%{%T}t.%{msec_frac}tZ\", \"remoteIP\":\"%a\",
               \"request\":\"%U\", \"query\":\"%q\", \"method\":\"%m\", \"status\":\"%>s\",
               \"responseTime\":\"%T\", \"userAgent\":\"%{User-agent}i\",
               \"accessToken\":\"%{Authorization}i\", \"referer\":\"%{Referer}i\",
               \"remoteuser\":\"%u\", \"ga_cookie\":\"%{_ga}Ci\"}" searchstats


Example of output, reformatted for readability. Each log message appears as a single line in the log file.

.. code-block:: json

    {
      "ver": 1.0,
      "time": "2018-10-08T07:56:41.600Z",
      "remoteIP": "73.128.224.157",
      "request": "/cn/v2/meta/solson.18.1",
      "query": "",
      "method": "GET",
      "status": "200",
      "responseTime": "7504774",
      "userAgent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36",
      "accessToken": "-",
      "referer": "https://search.dataone.org/view/doi:10.5063/F1HT2M7Q",
      "remoteUser": "-"
    }



See also:

* `Apache log files`_ for general information on configuring Apache logs
* `mod_log_config`_ for log format options


.. _Search UI: https://search.dataone.org/
.. _Apache log files: https://httpd.apache.org/docs/2.4/logs.html
.. _mod_log_config: https://httpd.apache.org/docs/2.4/mod/mod_log_config.html
