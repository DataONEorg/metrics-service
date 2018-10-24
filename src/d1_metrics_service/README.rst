d1_metrics_service
==================

Implements a REST API for access to aggregated metrics for DataONE.

#TODO: describe the application, installation, and operation.


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

Deactivate the virtual environment::

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

The Gunicorn server can be configured to `auto reload <http://docs.gunicorn.org/en/stable/settings.html>`_ after source
changes, which can be convenient in a dynamic development environment.


Apache mod_wsgi Configuration
.............................

``/etc/apache2/mods-enabled/wsgi.conf``::

  WSGIPythonOptimize 1


In the Apache site configuration::

  WSGIPythonHome /var/local/metrics-service/src/d1_metrics_service/.venv
  <VirtualHost *:443>
  ...

    Header always set Access-Control-Allow-Origin *
    WSGIDaemonProcess d1_metrics_service processes=4 python-path=/var/local/metrics-service/src/d1_metrics_service/.venv/lib/python3.5/site-packages
	WSGIProcessGroup d1_metrics_service
	WSGIApplicationGroup %{GLOBAL}
	WSGIScriptAlias / /var/local/metrics-service/src/d1_metrics_service/.venv/lib/python3.5/site-packages/d1_metrics_service/wsgi.py

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



Gunicorn Systemd Configuration
..............................

/etc/systemd/system/d1-metrics-service.service::

    [Unit]
    Description=Gunicorn instance serving d1-metrics-service
    After=network.target

    [Service]
    User=www-data
    Group=www-data
    Restart=on-failure
    WorkingDirectory=/var/local/metrics-service-asyncio
    Environment="PATH=/var/local/metrics-service-asyncio/venv/bin"
    ExecStart=/var/local/metrics-service-asyncio/venv/bin/gunicorn -c /var/local/metrics-service-asyncio/metrics-service/src/d1_metrics_service/gunicorn.conf d1_metrics_service.app:api

    [Install]
    WantedBy=multi-user.target

Activate ``d1-metrics-service.service``::

  sudo systemctl daemon-reload

Start the service::

  sudo systemctl start d1-metrics-service

Top the service::

  sudo systemctl stop d1-metrics-service

Enable the service at boot::

  sudo systemctl enable d1-metrics-service

Verify the service is running::

  sudo systemctl status d1-metrics-service
  ● d1-metrics-service.service - Gunicorn instance serving d1-metrics-service
     Loaded: loaded (/etc/systemd/system/d1-metrics-service.service; enabled; vendor preset: enabled)
     Active: active (running) since Thu 2018-10-18 05:48:10 PDT; 6s ago
   Main PID: 245164 (gunicorn)
      Tasks: 2
     Memory: 41.0M
        CPU: 536ms
     CGroup: /system.slice/d1-metrics-service.service
             ├─245164 /var/local/metrics-service-asyncio/venv/bin/python3 /var/local/metrics-service-asyncio/venv/bin/gunicorn -c /var/local/metrics-service-asyncio/metrics-service/src/d1_metrics_service/gunicorn.conf d1_metrics_service.app:api
             └─245166 /var/local/metrics-service-asyncio/venv/bin/python3 /var/local/metrics-service-asyncio/venv/bin/gunicorn -c /var/local/metrics-service-asyncio/metrics-service/src/d1_metrics_service/gunicorn.conf d1_metrics_service.app:api

  Oct 18 05:48:10 logproc-stage-ucsb-1 systemd[1]: Started Gunicorn instance serving d1-metrics-service.
  Oct 18 05:48:10 logproc-stage-ucsb-1 gunicorn[245164]: [2018-10-18 05:48:10 -0700] [245164] [INFO] Starting gunicorn 19.9.0
  Oct 18 05:48:10 logproc-stage-ucsb-1 gunicorn[245164]: [2018-10-18 05:48:10 -0700] [245164] [INFO] Listening at: http://127.0.0.1:8010 (245164)
  Oct 18 05:48:10 logproc-stage-ucsb-1 gunicorn[245164]: [2018-10-18 05:48:10 -0700] [245164] [INFO] Using worker: sync
  Oct 18 05:48:10 logproc-stage-ucsb-1 gunicorn[245164]: [2018-10-18 05:48:10 -0700] [245166] [INFO] Booting worker with pid: 245166


Apache Configuration
....................

Apache is configured to proxy the Gunicorn wsgi http service listening on port 8010::

    ProxyPass /metrics http://127.0.0.1:8010/metrics
    ProxyPassReverse /metrics http://127.0.0.1:8010/metrics
    <Location /metrics>
      AuthType None
      Require all granted
      # Always set these headers.
      #Header always set Access-Control-Allow-Origin *
      Header always set Access-Control-Allow-Methods "POST, GET, OPTIONS, DELETE, PUT"
      Header always set Access-Control-Max-Age "1000"
      Header always set Access-Control-Allow-Headers "x-requested-with, Content-Type, origin, authorization, accept, client-security-token"
    </Location>
