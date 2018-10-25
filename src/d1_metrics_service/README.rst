d1_metrics_service
==================

Implements a REST API for access to aggregated metrics for DataONE.


Deployment
----------


The server stack is basically::

                    Internet
                        |
                    https @ 443
                        |
             [apache2.4 + mod_wsgi]
                        |
               [d1-metrics-service]
              /         |        \
        http @ 8200    5432   cn.dataone.org @443
            /           |          \
    [Elastic Search] [Postgres]   [Solr]

Python and d1-metrics-service are deployed in a virtual environment using Python 3.5+.


The Virtual Environment
.......................

Create the virtual environment::

  cd /var/local/metrics-service-asyncio
  virtualenv venv -p /usr/bin/python3

Activate the virtual environment::

  . /var/local/metrics-service-asyncio/venv/bin/activate

To deactivate the virtual environment::

  deactivate

To install the metrics service and dependencies into the virtual environment, first activate
the virtual environment, then::

  cd /var/local/metrics-service-asyncio/metrics-service/src/
  pip install -U -e d1_metrics
  pip install -U -e d1_metrics_service

Note that this installs the ``d1_metrics`` and ``d1_metrics_service`` packages in developer mode, so that
any changes to the source are immediately reflected in the installed packages. Leave out the ``-e`` switch
in a production system to provide some isolation between the source and the installed packages. In that case,
it will be necessary to ``pip install -U d1_metrics_service`` in the activated environment after source changes.


Apache mod_wsgi Configuration
.............................

``/etc/apache2/mods-enabled/wsgi.conf``::

  WSGIPythonOptimize 1

In the Apache site configuration::

  WSGIPythonHome /var/local/metrics-service-asyncio/venv
  <VirtualHost *:443>
  ...

    Header always set Access-Control-Allow-Origin *
    WSGIDaemonProcess d1_metrics_service processes=4 python-path=/var/local/metrics-service-asyncio/venv/lib/python3.5/site-packages
	WSGIProcessGroup d1_metrics_service
	WSGIApplicationGroup %{GLOBAL}
	WSGIScriptAlias / /var/local/metrics-service-asyncio/venv/lib/python3.5/site-packages/d1_metrics_service/wsgi.py

  	<Location /metrics>
		AuthType None
		Require all granted
		# Always set these headers.
        #Header always set Access-Control-Allow-Origin *
        Header always set Access-Control-Allow-Methods "POST, GET, OPTIONS, DELETE, PUT"
        Header always set Access-Control-Max-Age "1000"
        Header always set Access-Control-Allow-Headers "x-requested-with, Content-Type, origin, authorization, accept, client-security-token"
    </Location>
    ...
  </VirtualHost>

