Notes on d1_metrics_service
===========================


Example request, from the search UI browse page::

  https://logproc-stage-ucsb-1.test.dataone.org/metrics?metricsRequest={%22metricsPage%22:{%22total%22:0,%22start%22:0,%22count%22:0},%22metrics%22:[%22citations%22,%22downloads%22,%22views%22],%22filterBy%22:[{%22filterType%22:%22catalog%22,%22values%22:[%22p1161.ds2423_20181010_0300%22,%22p1151.ds2412_20181010_0300%22,%22urn:uuid:2e9143a6-2623-46be-9cc5-788c238f27ea%22,%22PPBioMA.50.4%22,%22https://pasta.lternet.edu/package/metadata/eml/knb-lter-nwt/93/1%22,%22https://pasta.lternet.edu/package/metadata/eml/knb-lter-nwt/45/1%22,%22doi:10.6067:XCV8446794_meta$v=1538938553701%22,%22doi:10.6067:XCV8446793_meta$v=1538934411225%22,%22p10.ds237_20181007_0300%22,%22p17.ds2553_20181006_0302%22,%22p1284.ds2551_20181006_0302%22,%22p1284.ds2550_20181006_0302%22,%22p17.ds2547_20181006_0302%22,%22p17.ds2546_20181006_0301%22,%22p17.ds2545_20181006_0301%22,%22p1229.ds2543_20181006_0301%22,%22p1279.ds2539_20181006_0301%22,%22p1279.ds2538_20181006_0301%22,%22p1278.ds2537_20181006_0301%22,%22p1278.ds2536_20181006_0301%22,%22p1278.ds2535_20181006_0301%22,%22p1278.ds2534_20181006_0301%22,%22p1278.ds2533_20181006_0301%22,%22p1278.ds2532_20181006_0301%22,%22p43.ds2520_20181006_0301%22],%22interpretAs%22:%22list%22},{%22filterType%22:%22month%22,%22values%22:[%2201/01/2000%22,%2210/16/2018%22],%22interpretAs%22:%22range%22}],%22groupBy%22:[%22month%22]}
  https://logproc-stage-ucsb-1.test.dataone.org/metricsa?metricsRequest={%22metricsPage%22:{%22total%22:0,%22start%22:0,%22count%22:0},%22metrics%22:[%22citations%22,%22downloads%22,%22views%22],%22filterBy%22:[{%22filterType%22:%22catalog%22,%22values%22:[%22p1161.ds2423_20181010_0300%22,%22p1151.ds2412_20181010_0300%22,%22urn:uuid:2e9143a6-2623-46be-9cc5-788c238f27ea%22,%22PPBioMA.50.4%22,%22https://pasta.lternet.edu/package/metadata/eml/knb-lter-nwt/93/1%22,%22https://pasta.lternet.edu/package/metadata/eml/knb-lter-nwt/45/1%22,%22doi:10.6067:XCV8446794_meta$v=1538938553701%22,%22doi:10.6067:XCV8446793_meta$v=1538934411225%22,%22p10.ds237_20181007_0300%22,%22p17.ds2553_20181006_0302%22,%22p1284.ds2551_20181006_0302%22,%22p1284.ds2550_20181006_0302%22,%22p17.ds2547_20181006_0302%22,%22p17.ds2546_20181006_0301%22,%22p17.ds2545_20181006_0301%22,%22p1229.ds2543_20181006_0301%22,%22p1279.ds2539_20181006_0301%22,%22p1279.ds2538_20181006_0301%22,%22p1278.ds2537_20181006_0301%22,%22p1278.ds2536_20181006_0301%22,%22p1278.ds2535_20181006_0301%22,%22p1278.ds2534_20181006_0301%22,%22p1278.ds2533_20181006_0301%22,%22p1278.ds2532_20181006_0301%22,%22p43.ds2520_20181006_0301%22],%22interpretAs%22:%22list%22},{%22filterType%22:%22month%22,%22values%22:[%2201/01/2000%22,%2210/16/2018%22],%22interpretAs%22:%22range%22}],%22groupBy%22:[%22month%22]}
  http://localhost:8010/metrics?metricsRequest={%22metricsPage%22:{%22total%22:0,%22start%22:0,%22count%22:0},%22metrics%22:[%22citations%22,%22downloads%22,%22views%22],%22filterBy%22:[{%22filterType%22:%22catalog%22,%22values%22:[%22p1161.ds2423_20181010_0300%22,%22p1151.ds2412_20181010_0300%22,%22urn:uuid:2e9143a6-2623-46be-9cc5-788c238f27ea%22,%22PPBioMA.50.4%22,%22https://pasta.lternet.edu/package/metadata/eml/knb-lter-nwt/93/1%22,%22https://pasta.lternet.edu/package/metadata/eml/knb-lter-nwt/45/1%22,%22doi:10.6067:XCV8446794_meta$v=1538938553701%22,%22doi:10.6067:XCV8446793_meta$v=1538934411225%22,%22p10.ds237_20181007_0300%22,%22p17.ds2553_20181006_0302%22,%22p1284.ds2551_20181006_0302%22,%22p1284.ds2550_20181006_0302%22,%22p17.ds2547_20181006_0302%22,%22p17.ds2546_20181006_0301%22,%22p17.ds2545_20181006_0301%22,%22p1229.ds2543_20181006_0301%22,%22p1279.ds2539_20181006_0301%22,%22p1279.ds2538_20181006_0301%22,%22p1278.ds2537_20181006_0301%22,%22p1278.ds2536_20181006_0301%22,%22p1278.ds2535_20181006_0301%22,%22p1278.ds2534_20181006_0301%22,%22p1278.ds2533_20181006_0301%22,%22p1278.ds2532_20181006_0301%22,%22p43.ds2520_20181006_0301%22],%22interpretAs%22:%22list%22},{%22filterType%22:%22month%22,%22values%22:[%2201/01/2000%22,%2210/16/2018%22],%22interpretAs%22:%22range%22}],%22groupBy%22:[%22month%22]}
  http://localhost:8000/metrics?metricsRequest={%22metricsPage%22:{%22total%22:0,%22start%22:0,%22count%22:0},%22metrics%22:[%22citations%22,%22downloads%22,%22views%22],%22filterBy%22:[{%22filterType%22:%22catalog%22,%22values%22:[%22p1161.ds2423_20181010_0300%22,%22p1151.ds2412_20181010_0300%22,%22urn:uuid:2e9143a6-2623-46be-9cc5-788c238f27ea%22,%22PPBioMA.50.4%22,%22https://pasta.lternet.edu/package/metadata/eml/knb-lter-nwt/93/1%22,%22https://pasta.lternet.edu/package/metadata/eml/knb-lter-nwt/45/1%22,%22doi:10.6067:XCV8446794_meta$v=1538938553701%22,%22doi:10.6067:XCV8446793_meta$v=1538934411225%22,%22p10.ds237_20181007_0300%22,%22p17.ds2553_20181006_0302%22,%22p1284.ds2551_20181006_0302%22,%22p1284.ds2550_20181006_0302%22,%22p17.ds2547_20181006_0302%22,%22p17.ds2546_20181006_0301%22,%22p17.ds2545_20181006_0301%22,%22p1229.ds2543_20181006_0301%22,%22p1279.ds2539_20181006_0301%22,%22p1279.ds2538_20181006_0301%22,%22p1278.ds2537_20181006_0301%22,%22p1278.ds2536_20181006_0301%22,%22p1278.ds2535_20181006_0301%22,%22p1278.ds2534_20181006_0301%22,%22p1278.ds2533_20181006_0301%22,%22p1278.ds2532_20181006_0301%22,%22p43.ds2520_20181006_0301%22],%22interpretAs%22:%22list%22},{%22filterType%22:%22month%22,%22values%22:[%2201/01/2000%22,%2210/16/2018%22],%22interpretAs%22:%22range%22}],%22groupBy%22:[%22month%22]}

decoded and formatted::

  {
      "metricsPage": {
        "total": 0,
        "start": 0,
        "count": 0
      },
      "metrics": [
        "citations",
        "downloads",
        "views"
      ],
      "filterBy": [
        {
          "filterType": "catalog",
          "values": [
            "p1161.ds2423_20181010_0300",
            "p1151.ds2412_20181010_0300",
            "urn:uuid:2e9143a6-2623-46be-9cc5-788c238f27ea",
            "PPBioMA.50.4",
            "https://pasta.lternet.edu/package/metadata/eml/knb-lter-nwt/93/1",
            "https://pasta.lternet.edu/package/metadata/eml/knb-lter-nwt/45/1",
            "doi:10.6067:XCV8446794_meta$v=1538938553701",
            "doi:10.6067:XCV8446793_meta$v=1538934411225",
            "p10.ds237_20181007_0300",
            "p17.ds2553_20181006_0302",
            "p1284.ds2551_20181006_0302",
            "p1284.ds2550_20181006_0302",
            "p17.ds2547_20181006_0302",
            "p17.ds2546_20181006_0301",
            "p17.ds2545_20181006_0301",
            "p1229.ds2543_20181006_0301",
            "p1279.ds2539_20181006_0301",
            "p1279.ds2538_20181006_0301",
            "p1278.ds2537_20181006_0301",
            "p1278.ds2536_20181006_0301",
            "p1278.ds2535_20181006_0301",
            "p1278.ds2534_20181006_0301",
            "p1278.ds2533_20181006_0301",
            "p1278.ds2532_20181006_0301",
            "p43.ds2520_20181006_0301"
          ],
          "interpretAs": "list"
        },
        {
          "filterType": "month",
          "values": [
            "01/01/2000",
            "10/16/2018"
          ],
          "interpretAs": "range"
        }
      ],
      "groupBy": [
        "month"
      ]
    }

The response::

  {
      "results": {
        "views": [
          3,
          3,
          3,
          5,
          13,
          0,
          3,
          3,
          3,
          3,
          2,
          3,
          5,
          3,
          3,
          3,
          3,
          3,
          3,
          0,
          3,
          4,
          3,
          3,
          3
        ],
        "country": [],
        "citations": [
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0
        ],
        "datasets": [
          "p1278.ds2534_20181006_0301",
          "p17.ds2546_20181006_0301",
          "p1278.ds2537_20181006_0301",
          "p10.ds237_20181007_0300",
          "urn:uuid:2e9143a6-2623-46be-9cc5-788c238f27ea",
          "doi:10.6067:XCV8446794_meta$v=1538938553701",
          "p1279.ds2538_20181006_0301",
          "https://pasta.lternet.edu/package/metadata/eml/knb-lter-nwt/93/1",
          "p1278.ds2535_20181006_0301",
          "p17.ds2545_20181006_0301",
          "p1151.ds2412_20181010_0300",
          "p1278.ds2533_20181006_0301",
          "p17.ds2553_20181006_0302",
          "p1284.ds2551_20181006_0302",
          "p43.ds2520_20181006_0301",
          "p1284.ds2550_20181006_0302",
          "p1279.ds2539_20181006_0301",
          "p1229.ds2543_20181006_0301",
          "https://pasta.lternet.edu/package/metadata/eml/knb-lter-nwt/45/1",
          "doi:10.6067:XCV8446793_meta$v=1538934411225",
          "PPBioMA.50.4",
          "p1161.ds2423_20181010_0300",
          "p1278.ds2536_20181006_0301",
          "p1278.ds2532_20181006_0301",
          "p17.ds2547_20181006_0302"
        ],
        "downloads": [
          0,
          0,
          0,
          0,
          15,
          0,
          0,
          3,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          4,
          0,
          0,
          0,
          0,
          0,
          0
        ],
        "months": []
      },
      "metricsRequest": {
        "metrics": [
          "citations",
          "downloads",
          "views"
        ],
        "groupBy": [
          "month"
        ],
        "metricsPage": {
          "count": 0,
          "total": 0,
          "start": 0
        },
        "filterBy": [
          {
            "interpretAs": "list",
            "values": [
              "p1161.ds2423_20181010_0300",
              "p1151.ds2412_20181010_0300",
              "urn:uuid:2e9143a6-2623-46be-9cc5-788c238f27ea",
              "PPBioMA.50.4",
              "https://pasta.lternet.edu/package/metadata/eml/knb-lter-nwt/93/1",
              "https://pasta.lternet.edu/package/metadata/eml/knb-lter-nwt/45/1",
              "doi:10.6067:XCV8446794_meta$v=1538938553701",
              "doi:10.6067:XCV8446793_meta$v=1538934411225",
              "p10.ds237_20181007_0300",
              "p17.ds2553_20181006_0302",
              "p1284.ds2551_20181006_0302",
              "p1284.ds2550_20181006_0302",
              "p17.ds2547_20181006_0302",
              "p17.ds2546_20181006_0301",
              "p17.ds2545_20181006_0301",
              "p1229.ds2543_20181006_0301",
              "p1279.ds2539_20181006_0301",
              "p1279.ds2538_20181006_0301",
              "p1278.ds2537_20181006_0301",
              "p1278.ds2536_20181006_0301",
              "p1278.ds2535_20181006_0301",
              "p1278.ds2534_20181006_0301",
              "p1278.ds2533_20181006_0301",
              "p1278.ds2532_20181006_0301",
              "p43.ds2520_20181006_0301"
            ],
            "filterType": "catalog"
          },
          {
            "interpretAs": "range",
            "values": [
              "01/01/2000",
              "10/16/2018"
            ],
            "filterType": "month"
          }
        ]
      },
      "resultDetails": {}
    }

A typical solr request within::

  https://cn.dataone.org/cn/v2/query/solr/?q=%7B!join%20from=resourceMap%20to=resourceMap%7Did:%22p1284.ds2550_20181006_0302%22&fl=id&wt=json

  q={!join from=resourceMap to=resourceMap}id:"p1284.ds2550_20181006_0302"
  &fl=id
  &wt=json



Looking at a single record in the search UI, the request::

  https://logproc-stage-ucsb-1.test.dataone.org/metrics?metricsRequest={%22metricsPage%22:{%22total%22:0,%22start%22:0,%22count%22:0},%22metrics%22:[%22citations%22,%22downloads%22,%22views%22],%22filterBy%22:[{%22filterType%22:%22dataset%22,%22values%22:[%22PPBioMA.50.4%22],%22interpretAs%22:%22list%22},{%22filterType%22:%22month%22,%22values%22:[%2201/01/2000%22,%2210/16/2018%22],%22interpretAs%22:%22range%22}],%22groupBy%22:[%22month%22]}

  {
  "metricsPage": {
    "total": 0,
    "start": 0,
    "count": 0
  },
  "metrics": [
    "citations",
    "downloads",
    "views"
  ],
  "filterBy": [
    {
      "filterType": "dataset",
      "values": [
        "PPBioMA.50.4"
      ],
      "interpretAs": "list"
    },
    {
      "filterType": "month",
      "values": [
        "01/01/2000",
        "10/16/2018"
      ],
      "interpretAs": "range"
    }
  ],
  "groupBy": [
    "month"
  ]
  }


====

[Wed Oct 17 14:37:55.647561 2018] [wsgi:error] [pid 73342:tid 140325136893696] [client 73.128.224.157:55052] mod_wsgi (pid=73342): Exception occurred processing WSGI script '/var/local/metrics-service/src/d1_metrics_service/.venv/lib/python3.5/site-packages/d1_metrics_service/wsgi.py'., referer: https://search.dataone.org/view/https://pasta.lternet.edu/package/metadata/eml/edi/107/1
[Wed Oct 17 14:37:55.647746 2018] [wsgi:error] [pid 73342:tid 140325136893696] [client 73.128.224.157:55052] Traceback (most recent call last):, referer: https://search.dataone.org/view/https://pasta.lternet.edu/package/metadata/eml/edi/107/1
[Wed Oct 17 14:37:55.647802 2018] [wsgi:error] [pid 73342:tid 140325136893696] [client 73.128.224.157:55052]   File "/var/local/metrics-service/src/d1_metrics_service/.venv/lib/python3.5/site-packages/falcon/api.py", line 244, in __call__, referer: https://search.dataone.org/view/https://pasta.lternet.edu/package/metadata/eml/edi/107/1
[Wed Oct 17 14:37:55.647815 2018] [wsgi:error] [pid 73342:tid 140325136893696] [client 73.128.224.157:55052]     responder(req, resp, **params), referer: https://search.dataone.org/view/https://pasta.lternet.edu/package/metadata/eml/edi/107/1
[Wed Oct 17 14:37:55.647840 2018] [wsgi:error] [pid 73342:tid 140325136893696] [client 73.128.224.157:55052]   File "/var/local/metrics-service/src/d1_metrics_service/.venv/lib/python3.5/site-packages/d1_metrics_service/metricsreader.py", line 59, in on_get, referer: https://search.dataone.org/view/https://pasta.lternet.edu/package/metadata/eml/edi/107/1
[Wed Oct 17 14:37:55.647858 2018] [wsgi:error] [pid 73342:tid 140325136893696] [client 73.128.224.157:55052]     resp.body = json.dumps(self.process_request(metrics_request), ensure_ascii=False), referer: https://search.dataone.org/view/https://pasta.lternet.edu/package/metadata/eml/edi/107/1
[Wed Oct 17 14:37:55.647878 2018] [wsgi:error] [pid 73342:tid 140325136893696] [client 73.128.224.157:55052]   File "/var/local/metrics-service/src/d1_metrics_service/.venv/lib/python3.5/site-packages/d1_metrics_service/metricsreader.py", line 117, in process_request, referer: https://search.dataone.org/view/https://pasta.lternet.edu/package/metadata/eml/edi/107/1
[Wed Oct 17 14:37:55.647891 2018] [wsgi:error] [pid 73342:tid 140325136893696] [client 73.128.224.157:55052]     results, resultDetails = self.getSummaryMetricsPerDataset(filter_by[0]["values"]), referer: https://search.dataone.org/view/https://pasta.lternet.edu/package/metadata/eml/edi/107/1
[Wed Oct 17 14:37:55.647911 2018] [wsgi:error] [pid 73342:tid 140325136893696] [client 73.128.224.157:55052]   File "/var/local/metrics-service/src/d1_metrics_service/.venv/lib/python3.5/site-packages/d1_metrics_service/metricsreader.py", line 155, in getSummaryMetricsPerDataset, referer: https://search.dataone.org/view/https://pasta.lternet.edu/package/metadata/eml/edi/107/1
[Wed Oct 17 14:37:55.647928 2018] [wsgi:error] [pid 73342:tid 140325136893696] [client 73.128.224.157:55052]     obsoletes_dict = pid_resolution.getObsolescenceChain( PIDs, max_depth=1 ), referer: https://search.dataone.org/view/https://pasta.lternet.edu/package/metadata/eml/edi/107/1
[Wed Oct 17 14:37:55.647949 2018] [wsgi:error] [pid 73342:tid 140325136893696] [client 73.128.224.157:55052]   File "/var/local/metrics-service/src/d1_metrics_service/.venv/lib/python3.5/site-packages/d1_metrics_service/pid_resolution.py", line 242, in getObsolescenceChain, referer: https://search.dataone.org/view/https://pasta.lternet.edu/package/metadata/eml/edi/107/1
[Wed Oct 17 14:37:55.647961 2018] [wsgi:error] [pid 73342:tid 140325136893696] [client 73.128.224.157:55052]     loop = asyncio.get_event_loop(), referer: https://search.dataone.org/view/https://pasta.lternet.edu/package/metadata/eml/edi/107/1
[Wed Oct 17 14:37:55.647980 2018] [wsgi:error] [pid 73342:tid 140325136893696] [client 73.128.224.157:55052]   File "/usr/lib/python3.5/asyncio/events.py", line 632, in get_event_loop, referer: https://search.dataone.org/view/https://pasta.lternet.edu/package/metadata/eml/edi/107/1
[Wed Oct 17 14:37:55.647997 2018] [wsgi:error] [pid 73342:tid 140325136893696] [client 73.128.224.157:55052]     return get_event_loop_policy().get_event_loop(), referer: https://search.dataone.org/view/https://pasta.lternet.edu/package/metadata/eml/edi/107/1
[Wed Oct 17 14:37:55.648018 2018] [wsgi:error] [pid 73342:tid 140325136893696] [client 73.128.224.157:55052]   File "/usr/lib/python3.5/asyncio/events.py", line 578, in get_event_loop, referer: https://search.dataone.org/view/https://pasta.lternet.edu/package/metadata/eml/edi/107/1
[Wed Oct 17 14:37:55.648035 2018] [wsgi:error] [pid 73342:tid 140325136893696] [client 73.128.224.157:55052]     % threading.current_thread().name), referer: https://search.dataone.org/view/https://pasta.lternet.edu/package/metadata/eml/edi/107/1
[Wed Oct 17 14:37:55.648064 2018] [wsgi:error] [pid 73342:tid 140325136893696] [client 73.128.224.157:55052] RuntimeError: There is no current event loop in thread 'Dummy-5'., referer: https://search.dataone.org/view/https://pasta.lternet.edu/package/metadata/eml/edi/107/1
[Wed Oct 17 14:38:01.243106 2018] [wsgi:error] [pid 73342:tid 140325037274880] DEBUG:metrics_service.d1_metrics_service.metricsreader:enter on_get
[Wed Oct 17 14:38:01.243442 2018] [wsgi:error] [pid 73342:tid 140325037274880] DEBUG:metrics_service.d1_metrics_service.metricsreader:enter process_request. metrics_request={'filterBy': [{'values': ['https://pasta.lternet.edu/package/metadata/eml/knb-lter-ntl/324/16', 'doi:10.6067:XCV8930SQG_meta$v=1539726372987', 'https://pasta.lternet.edu/package/metadata/eml/edi/234/1', 'doi:10.6067:XCV8HM5803_meta$v=1539726149456', 'doi:10.6067:XCV84T6N9T_meta$v=1539710961211', 'doi:10.6067:XCV8NS0VSJ_meta$v=1539708438522', 'https://pasta.lternet.edu/package/metadata/eml/knb-lter-fce/1080/8', 'doi:10.6067:XCV8KK9CNH_meta$v=1539670544238', 'urn:uuid:f46dafac-91e4-4f5f-aaff-b53eab9fe863', 'https://pasta.lternet.edu/package/metadata/eml/knb-lter-fce/1079/9', 'https://pasta.lternet.edu/package/metadata/eml/knb-lter-fce/1075/8', 'doi:10.5063/F1QC01QK', 'doi:10.6067:XCV8G73D27_meta$v=1539556859394', 'https://pasta.lternet.edu/package/metadata/eml/knb-lter-mcm/63/9', 'urn:uuid:ebe9b67f-7a2b-44b6-9762-ed650573adde', 'https://pasta.lternet.edu/package/metadata/eml/knb-lter-jrn/210338008/1', 'https://pasta.lternet.edu/package/metadata/eml/knb-lter-jrn/210338007/1', 'https://pasta.lternet.edu/package/metadata/eml/knb-lter-fce/1074/10', 'https://pasta.lternet.edu/package/metadata/eml/knb-lter-jrn/210338006/1', 'https://pasta.lternet.edu/package/metadata/eml/knb-lter-jrn/210338005/1', 'https://pasta.lternet.edu/package/metadata/eml/edi/108/1', 'https://pasta.lternet.edu/package/metadata/eml/edi/107/1', 'doi:10.6067:XCV8CN75VC_meta$v=1539358000027', 'urn:uuid:0beca8b9-7fcb-468f-9118-2bcc9f641f90', 'https://pasta.lternet.edu/package/metadata/eml/edi/244/2'], 'interpretAs': 'list', 'filterType': 'catalog'}, {'values': ['01/01/2000', '10/17/2018'], 'interpretAs': 'range', 'filterType': 'month'}], 'groupBy': ['month'], 'metricsPage': {'total': 0, 'count': 0, 'start': 0}, 'metrics': ['citations', 'downloads', 'views']}
[Wed Oct 17 14:38:01.243507 2018] [wsgi:error] [pid 73342:tid 140325037274880] DEBUG:metrics_service.d1_metrics_service.metricsreader:process_request: filter_type=catalog, interpret_as=list, n_filter_values=25
[Wed Oct 17 14:38:01.243549 2018] [wsgi:error] [pid 73342:tid 140325037274880] DEBUG:metrics_service.d1_metrics_service.metricsreader:enter getSummaryMetricsPerCatalog
[Wed Oct 17 14:38:01.243623 2018] [wsgi:error] [pid 73342:tid 140325037274880] DEBUG:metrics_service.d1_metrics_service.metricsreader:getSummaryMetricsPerCatalog #004
[Wed Oct 17 14:38:01.243684 2018] [wsgi:error] [pid 73342:tid 140325037274880] [client 73.128.224.157:55080] mod_wsgi (pid=73342): Exception occurred processing WSGI script '/var/local/metrics-service/src/d1_metrics_service/.venv/lib/python3.5/site-packages/d1_metrics_service/wsgi.py'., referer: https://search.dataone.org/data
[Wed Oct 17 14:38:01.243846 2018] [wsgi:error] [pid 73342:tid 140325037274880] [client 73.128.224.157:55080] Traceback (most recent call last):, referer: https://search.dataone.org/data
[Wed Oct 17 14:38:01.243891 2018] [wsgi:error] [pid 73342:tid 140325037274880] [client 73.128.224.157:55080]   File "/var/local/metrics-service/src/d1_metrics_service/.venv/lib/python3.5/site-packages/falcon/api.py", line 244, in __call__, referer: https://search.dataone.org/data
[Wed Oct 17 14:38:01.243902 2018] [wsgi:error] [pid 73342:tid 140325037274880] [client 73.128.224.157:55080]     responder(req, resp, **params), referer: https://search.dataone.org/data
[Wed Oct 17 14:38:01.243927 2018] [wsgi:error] [pid 73342:tid 140325037274880] [client 73.128.224.157:55080]   File "/var/local/metrics-service/src/d1_metrics_service/.venv/lib/python3.5/site-packages/d1_metrics_service/metricsreader.py", line 59, in on_get, referer: https://search.dataone.org/data
[Wed Oct 17 14:38:01.243944 2018] [wsgi:error] [pid 73342:tid 140325037274880] [client 73.128.224.157:55080]     resp.body = json.dumps(self.process_request(metrics_request), ensure_ascii=False), referer: https://search.dataone.org/data
[Wed Oct 17 14:38:01.243965 2018] [wsgi:error] [pid 73342:tid 140325037274880] [client 73.128.224.157:55080]   File "/var/local/metrics-service/src/d1_metrics_service/.venv/lib/python3.5/site-packages/d1_metrics_service/metricsreader.py", line 128, in process_request, referer: https://search.dataone.org/data
[Wed Oct 17 14:38:01.243981 2018] [wsgi:error] [pid 73342:tid 140325037274880] [client 73.128.224.157:55080]     results, resultDetails = self.getSummaryMetricsPerCatalog(filter_by[0]["values"], filter_type), referer: https://search.dataone.org/data
[Wed Oct 17 14:38:01.244000 2018] [wsgi:error] [pid 73342:tid 140325037274880] [client 73.128.224.157:55080]   File "/var/local/metrics-service/src/d1_metrics_service/.venv/lib/python3.5/site-packages/d1_metrics_service/metricsreader.py", line 558, in getSummaryMetricsPerCatalog, referer: https://search.dataone.org/data
[Wed Oct 17 14:38:01.244017 2018] [wsgi:error] [pid 73342:tid 140325037274880] [client 73.128.224.157:55080]     return_dict = pid_resolution.getResolvePIDs(catalogPIDs), referer: https://search.dataone.org/data
[Wed Oct 17 14:38:01.244037 2018] [wsgi:error] [pid 73342:tid 140325037274880] [client 73.128.224.157:55080]   File "/var/local/metrics-service/src/d1_metrics_service/.venv/lib/python3.5/site-packages/d1_metrics_service/pid_resolution.py", line 311, in getResolvePIDs, referer: https://search.dataone.org/data
[Wed Oct 17 14:38:01.244053 2018] [wsgi:error] [pid 73342:tid 140325037274880] [client 73.128.224.157:55080]     loop = asyncio.get_event_loop(), referer: https://search.dataone.org/data
[Wed Oct 17 14:38:01.244072 2018] [wsgi:error] [pid 73342:tid 140325037274880] [client 73.128.224.157:55080]   File "/usr/lib/python3.5/asyncio/events.py", line 632, in get_event_loop, referer: https://search.dataone.org/data
[Wed Oct 17 14:38:01.245847 2018] [wsgi:error] [pid 73342:tid 140325037274880] [client 73.128.224.157:55080]     return get_event_loop_policy().get_event_loop(), referer: https://search.dataone.org/data
[Wed Oct 17 14:38:01.245872 2018] [wsgi:error] [pid 73342:tid 140325037274880] [client 73.128.224.157:55080]   File "/usr/lib/python3.5/asyncio/events.py", line 578, in get_event_loop, referer: https://search.dataone.org/data
[Wed Oct 17 14:38:01.245888 2018] [wsgi:error] [pid 73342:tid 140325037274880] [client 73.128.224.157:55080]     % threading.current_thread().name), referer: https://search.dataone.org/data
[Wed Oct 17 14:38:01.245919 2018] [wsgi:error] [pid 73342:tid 140325037274880] [client 73.128.224.157:55080] RuntimeError: There is no current event loop in thread 'Dummy-6'., referer: https://search.dataone.org/data
[Wed Oct 17 14:38:07.405875 2018] [wsgi:error] [pid 73341:tid 140325136893696] DEBUG:metrics_service.d1_metrics_service.metricsreader:enter on_get
[Wed Oct 17 14:38:07.406162 2018] [wsgi:error] [pid 73341:tid 140325136893696] DEBUG:metrics_service.d1_metrics_service.metricsreader:enter process_request. metrics_request={'groupBy': ['month'], 'metricsPage': {'count': 0, 'start': 0, 'total': 0}, 'filterBy': [{'values': ['https://pasta.lternet.edu/package/metadata/eml/edi/234/1'], 'interpretAs': 'list', 'filterType': 'dataset'}, {'values': ['01/01/2000', '10/17/2018'], 'interpretAs': 'range', 'filterType': 'month'}], 'metrics': ['citations', 'downloads', 'views']}
[Wed Oct 17 14:38:07.406222 2018] [wsgi:error] [pid 73341:tid 140325136893696] DEBUG:metrics_service.d1_metrics_service.metricsreader:process_request: filter_type=dataset, interpret_as=list, n_filter_values=1
[Wed Oct 17 14:38:07.406508 2018] [wsgi:error] [pid 73341:tid 140325136893696] DEBUG:resolvePIDs:enter resolvePIDs
[Wed Oct 17 14:38:07.407595 2018] [wsgi:error] [pid 73341:tid 140325136893696] DEBUG:urllib3.connectionpool:Starting new HTTPS connection (1): cn.dataone.org:443
[Wed Oct 17 14:38:07.431562 2018] [wsgi:error] [pid 73341:tid 140325136893696] DEBUG:urllib3.connectionpool:https://cn.dataone.org:443 "GET /cn/v2/query/solr/?q=%7B!join%20from=resourceMap%20to=resourceMap%7Did:%22https://pasta.lternet.edu/package/metadata/eml/edi/234/1%22&fl=id&wt=json HTTP/1.1" 200 None
[Wed Oct 17 14:38:08.118108 2018] [wsgi:error] [pid 73341:tid 140325136893696] DEBUG:urllib3.connectionpool:https://cn.dataone.org:443 "POST /cn/v2/query/solr/ HTTP/1.1" 200 None
[Wed Oct 17 14:38:08.118764 2018] [wsgi:error] [pid 73341:tid 140325136893696] DEBUG:resolvePIDs:resolvePIDs response = ["https://pasta.lternet.edu/package/metadata/eml/edi/234/1"]
[Wed Oct 17 14:38:08.118821 2018] [wsgi:error] [pid 73341:tid 140325136893696] DEBUG:resolvePIDs:exit resolvePIDs
[Wed Oct 17 14:38:08.119976 2018] [wsgi:error] [pid 73341:tid 140325136893696] DEBUG:metrics_service.d1_metrics_service.metricsreader:getSummaryMetricsPerDataset:t1=0.7137
[Wed Oct 17 14:38:08.120409 2018] [wsgi:error] [pid 73341:tid 140325136893696] [client 73.128.224.157:55113] mod_wsgi (pid=73341): Exception occurred processing WSGI script '/var/local/metrics-service/src/d1_metrics_service/.venv/lib/python3.5/site-packages/d1_metrics_service/wsgi.py'., referer: https://search.dataone.org/view/https://pasta.lternet.edu/package/metadata/eml/edi/234/1
[Wed Oct 17 14:38:08.121172 2018] [wsgi:error] [pid 73341:tid 140325136893696] [client 73.128.224.157:55113] Traceback (most recent call last):, referer: https://search.dataone.org/view/https://pasta.lternet.edu/package/metadata/eml/edi/234/1
[Wed Oct 17 14:38:08.121222 2018] [wsgi:error] [pid 73341:tid 140325136893696] [client 73.128.224.157:55113]   File "/var/local/metrics-service/src/d1_metrics_service/.venv/lib/python3.5/site-packages/falcon/api.py", line 244, in __call__, referer: https://search.dataone.org/view/https://pasta.lternet.edu/package/metadata/eml/edi/234/1
[Wed Oct 17 14:38:08.121234 2018] [wsgi:error] [pid 73341:tid 140325136893696] [client 73.128.224.157:55113]     responder(req, resp, **params), referer: https://search.dataone.org/view/https://pasta.lternet.edu/package/metadata/eml/edi/234/1
[Wed Oct 17 14:38:08.121259 2018] [wsgi:error] [pid 73341:tid 140325136893696] [client 73.128.224.157:55113]   File "/var/local/metrics-service/src/d1_metrics_service/.venv/lib/python3.5/site-packages/d1_metrics_service/metricsreader.py", line 59, in on_get, referer: https://search.dataone.org/view/https://pasta.lternet.edu/package/metadata/eml/edi/234/1
[Wed Oct 17 14:38:08.121276 2018] [wsgi:error] [pid 73341:tid 140325136893696] [client 73.128.224.157:55113]     resp.body = json.dumps(self.process_request(metrics_request), ensure_ascii=False), referer: https://search.dataone.org/view/https://pasta.lternet.edu/package/metadata/eml/edi/234/1
[Wed Oct 17 14:38:08.121294 2018] [wsgi:error] [pid 73341:tid 140325136893696] [client 73.128.224.157:55113]   File "/var/local/metrics-service/src/d1_metrics_service/.venv/lib/python3.5/site-packages/d1_metrics_service/metricsreader.py", line 117, in process_request, referer: https://search.dataone.org/view/https://pasta.lternet.edu/package/metadata/eml/edi/234/1
[Wed Oct 17 14:38:08.121310 2018] [wsgi:error] [pid 73341:tid 140325136893696] [client 73.128.224.157:55113]     results, resultDetails = self.getSummaryMetricsPerDataset(filter_by[0]["values"]), referer: https://search.dataone.org/view/https://pasta.lternet.edu/package/metadata/eml/edi/234/1
[Wed Oct 17 14:38:08.121329 2018] [wsgi:error] [pid 73341:tid 140325136893696] [client 73.128.224.157:55113]   File "/var/local/metrics-service/src/d1_metrics_service/.venv/lib/python3.5/site-packages/d1_metrics_service/metricsreader.py", line 155, in getSummaryMetricsPerDataset, referer: https://search.dataone.org/view/https://pasta.lternet.edu/package/metadata/eml/edi/234/1
[Wed Oct 17 14:38:08.121345 2018] [wsgi:error] [pid 73341:tid 140325136893696] [client 73.128.224.157:55113]     obsoletes_dict = pid_resolution.getObsolescenceChain( PIDs, max_depth=1 ), referer: https://search.dataone.org/view/https://pasta.lternet.edu/package/metadata/eml/edi/234/1
[Wed Oct 17 14:38:08.121363 2018] [wsgi:error] [pid 73341:tid 140325136893696] [client 73.128.224.157:55113]   File "/var/local/metrics-service/src/d1_metrics_service/.venv/lib/python3.5/site-packages/d1_metrics_service/pid_resolution.py", line 242, in getObsolescenceChain, referer: https://search.dataone.org/view/https://pasta.lternet.edu/package/metadata/eml/edi/234/1
[Wed Oct 17 14:38:08.121380 2018] [wsgi:error] [pid 73341:tid 140325136893696] [client 73.128.224.157:55113]     loop = asyncio.get_event_loop(), referer: https://search.dataone.org/view/https://pasta.lternet.edu/package/metadata/eml/edi/234/1
[Wed Oct 17 14:38:08.121397 2018] [wsgi:error] [pid 73341:tid 140325136893696] [client 73.128.224.157:55113]   File "/usr/lib/python3.5/asyncio/events.py", line 632, in get_event_loop, referer: https://search.dataone.org/view/https://pasta.lternet.edu/package/metadata/eml/edi/234/1
[Wed Oct 17 14:38:08.121413 2018] [wsgi:error] [pid 73341:tid 140325136893696] [client 73.128.224.157:55113]     return get_event_loop_policy().get_event_loop(), referer: https://search.dataone.org/view/https://pasta.lternet.edu/package/metadata/eml/edi/234/1
[Wed Oct 17 14:38:08.121431 2018] [wsgi:error] [pid 73341:tid 140325136893696] [client 73.128.224.157:55113]   File "/usr/lib/python3.5/asyncio/events.py", line 578, in get_event_loop, referer: https://search.dataone.org/view/https://pasta.lternet.edu/package/metadata/eml/edi/234/1
[Wed Oct 17 14:38:08.121447 2018] [wsgi:error] [pid 73341:tid 140325136893696] [client 73.128.224.157:55113]     % threading.current_thread().name), referer: https://search.dataone.org/view/https://pasta.lternet.edu/package/metadata/eml/edi/234/1
[Wed Oct 17 14:38:08.121476 2018] [wsgi:error] [pid 73341:tid 140325136893696] [client 73.128.224.157:55113] RuntimeError: There is no current event loop in thread 'Dummy-21'., referer: https://search.dataone.org/view/https://pasta.lternet.edu/package/metadata/eml/edi/234/1



[Wed Oct 17 14:40:57.970722 2018] [wsgi:error] [pid 73953:tid 140254958835456] DEBUG:urllib3.connectionpool:Starting new HTTP connection (1): localhost:9200
[Wed Oct 17 14:40:57.977304 2018] [wsgi:error] [pid 73953:tid 140254958835456] DEBUG:urllib3.connectionpool:http://localhost:9200 "GET /_search HTTP/1.1" 200 481
[Wed Oct 17 14:40:57.977648 2018] [wsgi:error] [pid 73953:tid 140254958835456] INFO:elasticsearch:GET http://localhost:9200/_search [status:200 request:0.007s]
[Wed Oct 17 14:40:57.977709 2018] [wsgi:error] [pid 73953:tid 140254958835456] DEBUG:elasticsearch:> {"query":{"bool":{"must":[[{"term":{"event.key":"read"}},{"terms":{"pid.key":["https://pasta.lternet.edu/package/metadata/eml/edi/234/1"]}},{"exists":{"field":"geoip.country_code2.keyword"}},{"exists":{"field":"sessionId"}},{"terms":{"formatType":["DATA","METADATA"]}}]],"filter":{"range":{"dateLogged":{"gte":"2000-01-01T00:00:00","lte":"2018-10-17T00:00:00"}}}}},"size":0,"aggs":{"pid_list":{"composite":{"sources":[{"country":{"terms":{"field":"geoip.country_code2.keyword"}}},{"format":{"terms":{"field":"formatType"}}},{"month":{"date_histogram":{"interval":"month","field":"dateLogged"}}}],"size":100},"aggs":{"https://pasta.lternet.edu/package/metadata/eml/edi/234/1":{"filters":{"filters":{"pid.key":{"term":{"pid.key":"https://pasta.lternet.edu/package/metadata/eml/edi/234/1"}}}}}}},"package_pid_list":{"composite":{"sources":[{"format":{"terms":{"field":"formatType"}}}]},"aggs":{"https://pasta.lternet.edu/package/metadata/eml/edi/234/1":{"filters":{"filters":{"pid.key":{"term":{"pid.key":"https://pasta.lternet.edu/package/metadata/eml/edi/234/1"}}}}}}}}}
[Wed Oct 17 14:40:57.977756 2018] [wsgi:error] [pid 73953:tid 140254958835456] DEBUG:elasticsearch:< {"took":5,"timed_out":false,"_shards":{"total":3,"successful":2,"skipped":0,"failed":1,"failures":[{"shard":0,"index":".kibana-6","node":"rVjAhCSgRM6vZw-qtvcDcg","reason":{"type":"query_shard_exception","reason":"failed to find field [geoip.country_code2.keyword] and [missing_bucket] is not set","index_uuid":"re8pCUHOTBasYmWH8X9log","index":".kibana-6"}}]},"hits":{"total":0,"max_score":0.0,"hits":[]},"aggregations":{"pid_list":{"buckets":[]},"package_pid_list":{"buckets":[]}}}
[Wed Oct 17 14:40:57.977911 2018] [wsgi:error] [pid 73953:tid 140254958835456] DEBUG:metrics_service.d1_metrics_service.metricsreader:getSummaryMetricsPerDataset:t3=0.9754
[Wed Oct 17 14:40:57.977959 2018] [wsgi:error] [pid 73953:tid 140254958835456] DEBUG:metrics_service.d1_metrics_service.metricsreader:enter gatherCitations
[Wed Oct 17 14:40:57.978031 2018] [wsgi:error] [pid 73953:tid 140254958835456] INFO:MetricsDatabase:Connecting to metrics@localhost:5432/metrics
[Wed Oct 17 14:40:57.985395 2018] [wsgi:error] [pid 73953:tid 140254958835456] INFO:MetricsDatabase:Connection to database already established.
[Wed Oct 17 14:40:57.989896 2018] [wsgi:error] [pid 73953:tid 140254958835456] DEBUG:metrics_service.d1_metrics_service.metricsreader:exit gatherCitations, elapsed=0.011849sec
[Wed Oct 17 14:40:57.990222 2018] [wsgi:error] [pid 73953:tid 140254958835456] DEBUG:metrics_service.d1_metrics_service.metricsreader:exit process_request, duration=0.987974sec
[Wed Oct 17 14:40:57.990330 2018] [wsgi:error] [pid 73953:tid 140254958835456] DEBUG:metrics_service.d1_metrics_service.metricsreader:exit on_get
[Wed Oct 17 14:40:58.133987 2018] [wsgi:error] [pid 73953:tid 140254933657344] DEBUG:metrics_service.d1_metrics_service.metricsreader:enter on_get
[Wed Oct 17 14:40:58.134448 2018] [wsgi:error] [pid 73953:tid 140254933657344] DEBUG:metrics_service.d1_metrics_service.metricsreader:enter process_request. metrics_request={'metrics': ['citations', 'downloads', 'views'], 'metricsPage': {'total': 0, 'start': 0, 'count': 0}, 'groupBy': ['month'], 'filterBy': [{'interpretAs': 'list', 'filterType': 'catalog', 'values': ['https://pasta.lternet.edu/package/metadata/eml/knb-lter-ntl/324/16', 'doi:10.6067:XCV8930SQG_meta$v=1539726372987', 'https://pasta.lternet.edu/package/metadata/eml/edi/234/1', 'doi:10.6067:XCV8HM5803_meta$v=1539726149456', 'doi:10.6067:XCV84T6N9T_meta$v=1539710961211', 'doi:10.6067:XCV8NS0VSJ_meta$v=1539708438522', 'https://pasta.lternet.edu/package/metadata/eml/knb-lter-fce/1080/8', 'doi:10.6067:XCV8KK9CNH_meta$v=1539670544238', 'urn:uuid:f46dafac-91e4-4f5f-aaff-b53eab9fe863', 'https://pasta.lternet.edu/package/metadata/eml/knb-lter-fce/1079/9', 'https://pasta.lternet.edu/package/metadata/eml/knb-lter-fce/1075/8', 'doi:10.5063/F1QC01QK', 'doi:10.6067:XCV8G73D27_meta$v=1539556859394', 'https://pasta.lternet.edu/package/metadata/eml/knb-lter-mcm/63/9', 'urn:uuid:ebe9b67f-7a2b-44b6-9762-ed650573adde', 'https://pasta.lternet.edu/package/metadata/eml/knb-lter-jrn/210338008/1', 'https://pasta.lternet.edu/package/metadata/eml/knb-lter-jrn/210338007/1', 'https://pasta.lternet.edu/package/metadata/eml/knb-lter-fce/1074/10', 'https://pasta.lternet.edu/package/metadata/eml/knb-lter-jrn/210338006/1', 'https://pasta.lternet.edu/package/metadata/eml/knb-lter-jrn/210338005/1', 'https://pasta.lternet.edu/package/metadata/eml/edi/108/1', 'https://pasta.lternet.edu/package/metadata/eml/edi/107/1', 'doi:10.6067:XCV8CN75VC_meta$v=1539358000027', 'urn:uuid:0beca8b9-7fcb-468f-9118-2bcc9f641f90', 'https://pasta.lternet.edu/package/metadata/eml/edi/244/2']}, {'interpretAs': 'range', 'filterType': 'month', 'values': ['01/01/2000', '10/17/2018']}]}
[Wed Oct 17 14:40:58.134556 2018] [wsgi:error] [pid 73953:tid 140254933657344] DEBUG:metrics_service.d1_metrics_service.metricsreader:process_request: filter_type=catalog, interpret_as=list, n_filter_values=25
[Wed Oct 17 14:40:58.134629 2018] [wsgi:error] [pid 73953:tid 140254933657344] DEBUG:metrics_service.d1_metrics_service.metricsreader:enter getSummaryMetricsPerCatalog
[Wed Oct 17 14:40:58.134721 2018] [wsgi:error] [pid 73953:tid 140254933657344] DEBUG:metrics_service.d1_metrics_service.metricsreader:getSummaryMetricsPerCatalog #004
[Wed Oct 17 14:40:58.134810 2018] [wsgi:error] [pid 73953:tid 140254933657344] [client 73.128.224.157:55858] mod_wsgi (pid=73953): Exception occurred processing WSGI script '/var/local/metrics-service/src/d1_metrics_service/.venv/lib/python3.5/site-packages/d1_metrics_service/wsgi.py'., referer: https://search.dataone.org/data
[Wed Oct 17 14:40:58.136001 2018] [wsgi:error] [pid 73953:tid 140254933657344] [client 73.128.224.157:55858] Traceback (most recent call last):, referer: https://search.dataone.org/data
[Wed Oct 17 14:40:58.136081 2018] [wsgi:error] [pid 73953:tid 140254933657344] [client 73.128.224.157:55858]   File "/var/local/metrics-service/src/d1_metrics_service/.venv/lib/python3.5/site-packages/falcon/api.py", line 244, in __call__, referer: https://search.dataone.org/data
[Wed Oct 17 14:40:58.136100 2018] [wsgi:error] [pid 73953:tid 140254933657344] [client 73.128.224.157:55858]     responder(req, resp, **params), referer: https://search.dataone.org/data
[Wed Oct 17 14:40:58.136166 2018] [wsgi:error] [pid 73953:tid 140254933657344] [client 73.128.224.157:55858]   File "/var/local/metrics-service/src/d1_metrics_service/.venv/lib/python3.5/site-packages/d1_metrics_service/metricsreader.py", line 59, in on_get, referer: https://search.dataone.org/data
[Wed Oct 17 14:40:58.136185 2018] [wsgi:error] [pid 73953:tid 140254933657344] [client 73.128.224.157:55858]     resp.body = json.dumps(self.process_request(metrics_request), ensure_ascii=False), referer: https://search.dataone.org/data
[Wed Oct 17 14:40:58.136215 2018] [wsgi:error] [pid 73953:tid 140254933657344] [client 73.128.224.157:55858]   File "/var/local/metrics-service/src/d1_metrics_service/.venv/lib/python3.5/site-packages/d1_metrics_service/metricsreader.py", line 128, in process_request, referer: https://search.dataone.org/data
[Wed Oct 17 14:40:58.136233 2018] [wsgi:error] [pid 73953:tid 140254933657344] [client 73.128.224.157:55858]     results, resultDetails = self.getSummaryMetricsPerCatalog(filter_by[0]["values"], filter_type), referer: https://search.dataone.org/data
[Wed Oct 17 14:40:58.136261 2018] [wsgi:error] [pid 73953:tid 140254933657344] [client 73.128.224.157:55858]   File "/var/local/metrics-service/src/d1_metrics_service/.venv/lib/python3.5/site-packages/d1_metrics_service/metricsreader.py", line 558, in getSummaryMetricsPerCatalog, referer: https://search.dataone.org/data
[Wed Oct 17 14:40:58.136279 2018] [wsgi:error] [pid 73953:tid 140254933657344] [client 73.128.224.157:55858]     return_dict = pid_resolution.getResolvePIDs(catalogPIDs), referer: https://search.dataone.org/data
[Wed Oct 17 14:40:58.136307 2018] [wsgi:error] [pid 73953:tid 140254933657344] [client 73.128.224.157:55858]   File "/var/local/metrics-service/src/d1_metrics_service/.venv/lib/python3.5/site-packages/d1_metrics_service/pid_resolution.py", line 311, in getResolvePIDs, referer: https://search.dataone.org/data
[Wed Oct 17 14:40:58.136324 2018] [wsgi:error] [pid 73953:tid 140254933657344] [client 73.128.224.157:55858]     loop = asyncio.get_event_loop(), referer: https://search.dataone.org/data
[Wed Oct 17 14:40:58.136348 2018] [wsgi:error] [pid 73953:tid 140254933657344] [client 73.128.224.157:55858]   File "/usr/lib/python3.5/asyncio/events.py", line 632, in get_event_loop, referer: https://search.dataone.org/data
[Wed Oct 17 14:40:58.136365 2018] [wsgi:error] [pid 73953:tid 140254933657344] [client 73.128.224.157:55858]     return get_event_loop_policy().get_event_loop(), referer: https://search.dataone.org/data
[Wed Oct 17 14:40:58.136417 2018] [wsgi:error] [pid 73953:tid 140254933657344] [client 73.128.224.157:55858]   File "/usr/lib/python3.5/asyncio/events.py", line 578, in get_event_loop, referer: https://search.dataone.org/data
[Wed Oct 17 14:40:58.136434 2018] [wsgi:error] [pid 73953:tid 140254933657344] [client 73.128.224.157:55858]     % threading.current_thread().name), referer: https://search.dataone.org/data
[Wed Oct 17 14:40:58.136474 2018] [wsgi:error] [pid 73953:tid 140254933657344] [client 73.128.224.157:55858] RuntimeError: There is no current event loop in thread 'Dummy-2'., referer: https://search.dataone.org/data
Exception ignored in: <bound method BaseEventLoop.__del__ of <_UnixSelectorEventLoop running=False closed=False debug=False>>
Traceback (most recent call last):
  File "/usr/lib/python3.5/asyncio/base_events.py", line 429, in __del__
NameError: name 'ResourceWarning' is not defined


process_request. metrics_request={'groupBy': ['month'], 'filterBy': [{'values': ['https://pasta.lternet.edu/package/metadata/eml/knb-lter-ntl/324/16', 'doi:10.6067:XCV8930SQG_meta$v=1539726372987', 'https://pasta.lternet.edu/package/metadata/eml/edi/234/1', 'doi:10.6067:XCV8HM5803_meta$v=1539726149456', 'doi:10.6067:XCV84T6N9T_meta$v=1539710961211', 'doi:10.6067:XCV8NS0VSJ_meta$v=1539708438522', 'https://pasta.lternet.edu/package/metadata/eml/knb-lter-fce/1080/8', 'doi:10.6067:XCV8KK9CNH_meta$v=1539670544238', 'urn:uuid:f46dafac-91e4-4f5f-aaff-b53eab9fe863', 'https://pasta.lternet.edu/package/metadata/eml/knb-lter-fce/1079/9', 'https://pasta.lternet.edu/package/metadata/eml/knb-lter-fce/1075/8', 'doi:10.5063/F1QC01QK', 'doi:10.6067:XCV8G73D27_meta$v=1539556859394', 'https://pasta.lternet.edu/package/metadata/eml/knb-lter-mcm/63/9', 'urn:uuid:ebe9b67f-7a2b-44b6-9762-ed650573adde', 'https://pasta.lternet.edu/package/metadata/eml/knb-lter-jrn/210338008/1', 'https://pasta.lternet.edu/package/metadata/eml/knb-lter-jrn/210338007/1', 'https://pasta.lternet.edu/package/metadata/eml/knb-lter-fce/1074/10', 'https://pasta.lternet.edu/package/metadata/eml/knb-lter-jrn/210338006/1', 'https://pasta.lternet.edu/package/metadata/eml/knb-lter-jrn/210338005/1', 'https://pasta.lternet.edu/package/metadata/eml/edi/108/1', 'https://pasta.lternet.edu/package/metadata/eml/edi/107/1', 'doi:10.6067:XCV8CN75VC_meta$v=1539358000027', 'urn:uuid:0beca8b9-7fcb-468f-9118-2bcc9f641f90', 'https://pasta.lternet.edu/package/metadata/eml/edi/244/2'], 'interpretAs': 'list', 'filterType': 'catalog'}, {'values': ['01/01/2000', '10/17/2018'], 'interpretAs': 'range', 'filterType': 'month'}], 'metrics': ['citations', 'downloads', 'views'], 'metricsPage': {'total': 0, 'count': 0, 'start': 0}}
[Wed Oct 17 14:59:57.627450 2018] [wsgi:error] [pid 77768:tid 140491503208192] DEBUG:metrics_service.d1_metrics_service.metricsreader:process_request: filter_type=catalog, interpret_as=list, n_filter_values=25
[Wed Oct 17 14:59:57.627492 2018] [wsgi:error] [pid 77768:tid 140491503208192] DEBUG:metrics_service.d1_metrics_service.metricsreader:enter getSummaryMetricsPerCatalog
[Wed Oct 17 14:59:57.627564 2018] [wsgi:error] [pid 77768:tid 140491503208192] DEBUG:metrics_service.d1_metrics_service.metricsreader:getSummaryMetricsPerCatalog #004
[Wed Oct 17 14:59:57.627630 2018] [wsgi:error] [pid 77768:tid 140491503208192] WARNING:getResolvePIDs:There is no current event loop in thread 'Dummy-21'.
[Wed Oct 17 14:59:57.627732 2018] [wsgi:error] [pid 77768:tid 140491503208192] DEBUG:asyncio:Using selector: EpollSelector
[Wed Oct 17 14:59:57.628002 2018] [wsgi:error] [pid 77768:tid 140491503208192] [client 73.128.224.157:60919] mod_wsgi (pid=77768): Exception occurred processing WSGI script '/var/local/metrics-service/src/d1_metrics_service/.venv/lib/python3.5/site-packages/d1_metrics_service/wsgi.py'., referer: https://search.dataone.org/data
[Wed Oct 17 14:59:57.672356 2018] [wsgi:error] [pid 77768:tid 140491503208192] [client 73.128.224.157:60919] Traceback (most recent call last):, referer: https://search.dataone.org/data
[Wed Oct 17 14:59:57.672437 2018] [wsgi:error] [pid 77768:tid 140491503208192] [client 73.128.224.157:60919]   File "/var/local/metrics-service/src/d1_metrics_service/.venv/lib/python3.5/site-packages/falcon/api.py", line 244, in __call__, referer: https://search.dataone.org/data
[Wed Oct 17 14:59:57.672449 2018] [wsgi:error] [pid 77768:tid 140491503208192] [client 73.128.224.157:60919]     responder(req, resp, **params), referer: https://search.dataone.org/data
[Wed Oct 17 14:59:57.672473 2018] [wsgi:error] [pid 77768:tid 140491503208192] [client 73.128.224.157:60919]   File "/var/local/metrics-service/src/d1_metrics_service/.venv/lib/python3.5/site-packages/d1_metrics_service/metricsreader.py", line 58, in on_get, referer: https://search.dataone.org/data
[Wed Oct 17 14:59:57.672490 2018] [wsgi:error] [pid 77768:tid 140491503208192] [client 73.128.224.157:60919]     resp.body = json.dumps(self.process_request(metrics_request), ensure_ascii=False), referer: https://search.dataone.org/data
[Wed Oct 17 14:59:57.672509 2018] [wsgi:error] [pid 77768:tid 140491503208192] [client 73.128.224.157:60919]   File "/var/local/metrics-service/src/d1_metrics_service/.venv/lib/python3.5/site-packages/d1_metrics_service/metricsreader.py", line 127, in process_request, referer: https://search.dataone.org/data
[Wed Oct 17 14:59:57.672525 2018] [wsgi:error] [pid 77768:tid 140491503208192] [client 73.128.224.157:60919]     results, resultDetails = self.getSummaryMetricsPerCatalog(filter_by[0]["values"], filter_type), referer: https://search.dataone.org/data
[Wed Oct 17 14:59:57.672544 2018] [wsgi:error] [pid 77768:tid 140491503208192] [client 73.128.224.157:60919]   File "/var/local/metrics-service/src/d1_metrics_service/.venv/lib/python3.5/site-packages/d1_metrics_service/metricsreader.py", line 557, in getSummaryMetricsPerCatalog, referer: https://search.dataone.org/data
[Wed Oct 17 14:59:57.672561 2018] [wsgi:error] [pid 77768:tid 140491503208192] [client 73.128.224.157:60919]     return_dict = pid_resolution.getResolvePIDs(catalogPIDs), referer: https://search.dataone.org/data
[Wed Oct 17 14:59:57.672580 2018] [wsgi:error] [pid 77768:tid 140491503208192] [client 73.128.224.157:60919]   File "/var/local/metrics-service/src/d1_metrics_service/.venv/lib/python3.5/site-packages/d1_metrics_service/pid_resolution.py", line 327, in getResolvePIDs, referer: https://search.dataone.org/data
[Wed Oct 17 14:59:57.672596 2018] [wsgi:error] [pid 77768:tid 140491503208192] [client 73.128.224.157:60919]     loop.run_until_complete( _work(PIDs) ), referer: https://search.dataone.org/data
[Wed Oct 17 14:59:57.672615 2018] [wsgi:error] [pid 77768:tid 140491503208192] [client 73.128.224.157:60919]   File "/usr/lib/python3.5/asyncio/base_events.py", line 387, in run_until_complete, referer: https://search.dataone.org/data
[Wed Oct 17 14:59:57.672631 2018] [wsgi:error] [pid 77768:tid 140491503208192] [client 73.128.224.157:60919]     return future.result(), referer: https://search.dataone.org/data
[Wed Oct 17 14:59:57.672649 2018] [wsgi:error] [pid 77768:tid 140491503208192] [client 73.128.224.157:60919]   File "/usr/lib/python3.5/asyncio/futures.py", line 274, in result, referer: https://search.dataone.org/data
[Wed Oct 17 14:59:57.672666 2018] [wsgi:error] [pid 77768:tid 140491503208192] [client 73.128.224.157:60919]     raise self._exception, referer: https://search.dataone.org/data
[Wed Oct 17 14:59:57.672685 2018] [wsgi:error] [pid 77768:tid 140491503208192] [client 73.128.224.157:60919]   File "/usr/lib/python3.5/asyncio/tasks.py", line 239, in _step, referer: https://search.dataone.org/data
[Wed Oct 17 14:59:57.672701 2018] [wsgi:error] [pid 77768:tid 140491503208192] [client 73.128.224.157:60919]     result = coro.send(None), referer: https://search.dataone.org/data
[Wed Oct 17 14:59:57.672720 2018] [wsgi:error] [pid 77768:tid 140491503208192] [client 73.128.224.157:60919]   File "/var/local/metrics-service/src/d1_metrics_service/.venv/lib/python3.5/site-packages/d1_metrics_service/pid_resolution.py", line 313, in _work, referer: https://search.dataone.org/data
[Wed Oct 17 14:59:57.672735 2018] [wsgi:error] [pid 77768:tid 140491503208192] [client 73.128.224.157:60919]     loop = asyncio.get_event_loop(), referer: https://search.dataone.org/data
[Wed Oct 17 14:59:57.672754 2018] [wsgi:error] [pid 77768:tid 140491503208192] [client 73.128.224.157:60919]   File "/usr/lib/python3.5/asyncio/events.py", line 632, in get_event_loop, referer: https://search.dataone.org/data
[Wed Oct 17 14:59:57.672770 2018] [wsgi:error] [pid 77768:tid 140491503208192] [client 73.128.224.157:60919]     return get_event_loop_policy().get_event_loop(), referer: https://search.dataone.org/data
[Wed Oct 17 14:59:57.672788 2018] [wsgi:error] [pid 77768:tid 140491503208192] [client 73.128.224.157:60919]   File "/usr/lib/python3.5/asyncio/events.py", line 578, in get_event_loop, referer: https://search.dataone.org/data
[Wed Oct 17 14:59:57.672804 2018] [wsgi:error] [pid 77768:tid 140491503208192] [client 73.128.224.157:60919]     % threading.current_thread().name), referer: https://search.dataone.org/data
[Wed Oct 17 14:59:57.672835 2018] [wsgi:error] [pid 77768:tid 140491503208192] [client 73.128.224.157:60919] RuntimeError: There is no current event loop in thread 'Dummy-21'., referer: https://search.dataone.org/data



[Wed Oct 17 15:20:04.144445 2018] [wsgi:error] [pid 81989:tid 140337318196992] DEBUG:urllib3.connectionpool:https://cn.dataone.org:443 "GET /cn/v2/query/solr/?fl=id%2Cdocuments%2Cobsoletes%2CresourceMap&wt=json&q.op=OR&q=resourceMap%3A%28%22https%5C%3A%2F%2Fpasta.lternet.edu%2Fpackage%2Fmetadata%2Feml%2Fknb%5C-lter%5C-jrn%2F210338007%2F1%22+%22https%5C%3A%2F%2Fpasta.lternet.edu%2Fpackage%2Fdata%2Feml%2Fknb%5C-lter%5C-jrn%2F210338007%2F1%2F6d806a026da732cfeda8f587e6176857%22+%22https%5C%3A%2F%2Fpasta.lternet.edu%2Fpackage%2Freport%2Feml%2Fknb%5C-lter%5C-jrn%2F210338007%2F1%22%29 HTTP/1.1" 200 None
[Wed Oct 17 15:20:04.149880 2018] [wsgi:error] [pid 81989:tid 140337813104384] DEBUG:urllib3.connectionpool:https://cn.dataone.org:443 "GET /cn/v2/query/solr/?fl=id%2Cdocuments%2Cobsoletes%2CresourceMap&wt=json&q.op=OR&q=resourceMap%3A%28%22https%5C%3A%2F%2Fpasta.lternet.edu%2Fpackage%2Fmetadata%2Feml%2Fedi%2F108%2F1%22+%22https%5C%3A%2F%2Fpasta.lternet.edu%2Fpackage%2Fdata%2Feml%2Fedi%2F108%2F1%2F732b856b8fe1cf4ebde6b11ed8ed234f%22+%22https%5C%3A%2F%2Fpasta.lternet.edu%2Fpackage%2Freport%2Feml%2Fedi%2F108%2F1%22%29 HTTP/1.1" 200 None
[Wed Oct 17 15:20:04.151833 2018] [wsgi:error] [pid 81989:tid 140337838282496] DEBUG:urllib3.connectionpool:https://cn.dataone.org:443 "GET /cn/v2/query/solr/?fl=id&wt=json&q.op=OR&q=%7B%21join+from%3DresourceMap+to%3DresourceMap%7Did%3A%22https%5C%3A%2F%2Fpasta.lternet.edu%2Fpackage%2Fmetadata%2Feml%2Fknb%5C-lter%5C-jrn%2F210338008%2F1%22 HTTP/1.1" 200 None
[Wed Oct 17 15:20:04.158173 2018] [wsgi:error] [pid 81989:tid 140337838282496] DEBUG:urllib3.connectionpool:https://cn.dataone.org:443 "GET /cn/v2/query/solr/?fl=id%2Cdocuments%2Cobsoletes%2CresourceMap&wt=json&q.op=OR&q=resourceMap%3A%28%22https%5C%3A%2F%2Fpasta.lternet.edu%2Fpackage%2Fmetadata%2Feml%2Fknb%5C-lter%5C-jrn%2F210338008%2F1%22%29 HTTP/1.1" 200 None
[Wed Oct 17 15:20:04.160554 2018] [wsgi:error] [pid 81989:tid 140338342467328] DEBUG:urllib3.connectionpool:https://cn.dataone.org:443 "GET /cn/v2/query/solr/?fl=id%2Cdocuments%2Cobsoletes%2CresourceMap&wt=json&q.op=OR&q=resourceMap%3A%28%22doi%5C%3A10.6067%5C%3AXCV8930SQG_meta%24v%3D1539726372987%22%29 HTTP/1.1" 200 None
[Wed Oct 17 15:20:04.162105 2018] [wsgi:error] [pid 81989:tid 140338334074624] DEBUG:urllib3.connectionpool:https://cn.dataone.org:443 "GET /cn/v2/query/solr/?fl=id%2Cdocuments%2Cobsoletes%2CresourceMap&wt=json&q.op=OR&q=resourceMap%3A%28%22doi%5C%3A10.6067%5C%3AXCV8HM5803_meta%24v%3D1539726149456%22%29 HTTP/1.1" 200 None
[Wed Oct 17 15:20:04.163550 2018] [wsgi:error] [pid 81989:tid 140338808350464] [client 73.128.224.157:49760] mod_wsgi (pid=81989): Exception occurred processing WSGI script '/var/local/metrics-service/src/d1_metrics_service/.venv/lib/python3.5/site-packages/d1_metrics_service/wsgi.py'., referer: https://search.dataone.org/data
[Wed Oct 17 15:20:04.164820 2018] [wsgi:error] [pid 81989:tid 140338808350464] [client 73.128.224.157:49760] Traceback (most recent call last):, referer: https://search.dataone.org/data
[Wed Oct 17 15:20:04.164878 2018] [wsgi:error] [pid 81989:tid 140338808350464] [client 73.128.224.157:49760]   File "/var/local/metrics-service/src/d1_metrics_service/.venv/lib/python3.5/site-packages/falcon/api.py", line 244, in __call__, referer: https://search.dataone.org/data
[Wed Oct 17 15:20:04.164900 2018] [wsgi:error] [pid 81989:tid 140338808350464] [client 73.128.224.157:49760]     responder(req, resp, **params), referer: https://search.dataone.org/data
[Wed Oct 17 15:20:04.164959 2018] [wsgi:error] [pid 81989:tid 140338808350464] [client 73.128.224.157:49760]   File "/var/local/metrics-service/src/d1_metrics_service/.venv/lib/python3.5/site-packages/d1_metrics_service/metricsreader.py", line 57, in on_get, referer: https://search.dataone.org/data
[Wed Oct 17 15:20:04.164979 2018] [wsgi:error] [pid 81989:tid 140338808350464] [client 73.128.224.157:49760]     resp.body = json.dumps(self.process_request(metrics_request), ensure_ascii=False), referer: https://search.dataone.org/data
[Wed Oct 17 15:20:04.165001 2018] [wsgi:error] [pid 81989:tid 140338808350464] [client 73.128.224.157:49760]   File "/var/local/metrics-service/src/d1_metrics_service/.venv/lib/python3.5/site-packages/d1_metrics_service/metricsreader.py", line 126, in process_request, referer: https://search.dataone.org/data
[Wed Oct 17 15:20:04.165020 2018] [wsgi:error] [pid 81989:tid 140338808350464] [client 73.128.224.157:49760]     results, resultDetails = self.getSummaryMetricsPerCatalog(filter_by[0]["values"], filter_type), referer: https://search.dataone.org/data
[Wed Oct 17 15:20:04.165041 2018] [wsgi:error] [pid 81989:tid 140338808350464] [client 73.128.224.157:49760]   File "/var/local/metrics-service/src/d1_metrics_service/.venv/lib/python3.5/site-packages/d1_metrics_service/metricsreader.py", line 556, in getSummaryMetricsPerCatalog, referer: https://search.dataone.org/data
[Wed Oct 17 15:20:04.165060 2018] [wsgi:error] [pid 81989:tid 140338808350464] [client 73.128.224.157:49760]     return_dict = pid_resolution.getResolvePIDs(catalogPIDs), referer: https://search.dataone.org/data
[Wed Oct 17 15:20:04.165072 2018] [wsgi:error] [pid 81989:tid 140338808350464] [client 73.128.224.157:49760]   File "/var/local/metrics-service/src/d1_metrics_service/.venv/lib/python3.5/site-packages/d1_metrics_service/pid_resolution.py", line 342, in getResolvePIDs, referer: https://search.dataone.org/data
[Wed Oct 17 15:20:04.165089 2018] [wsgi:error] [pid 81989:tid 140338808350464] [client 73.128.224.157:49760]     loop.run_until_complete( _work(PIDs) ), referer: https://search.dataone.org/data
[Wed Oct 17 15:20:04.165109 2018] [wsgi:error] [pid 81989:tid 140338808350464] [client 73.128.224.157:49760]   File "/usr/lib/python3.5/asyncio/base_events.py", line 387, in run_until_complete, referer: https://search.dataone.org/data
[Wed Oct 17 15:20:04.165126 2018] [wsgi:error] [pid 81989:tid 140338808350464] [client 73.128.224.157:49760]     return future.result(), referer: https://search.dataone.org/data
[Wed Oct 17 15:20:04.165145 2018] [wsgi:error] [pid 81989:tid 140338808350464] [client 73.128.224.157:49760]   File "/usr/lib/python3.5/asyncio/futures.py", line 274, in result, referer: https://search.dataone.org/data
[Wed Oct 17 15:20:04.165165 2018] [wsgi:error] [pid 81989:tid 140338808350464] [client 73.128.224.157:49760]     raise self._exception, referer: https://search.dataone.org/data
[Wed Oct 17 15:20:04.165185 2018] [wsgi:error] [pid 81989:tid 140338808350464] [client 73.128.224.157:49760]   File "/usr/lib/python3.5/asyncio/tasks.py", line 241, in _step, referer: https://search.dataone.org/data
[Wed Oct 17 15:20:04.165204 2018] [wsgi:error] [pid 81989:tid 140338808350464] [client 73.128.224.157:49760]     result = coro.throw(exc), referer: https://search.dataone.org/data
[Wed Oct 17 15:20:04.165215 2018] [wsgi:error] [pid 81989:tid 140338808350464] [client 73.128.224.157:49760]   File "/var/local/metrics-service/src/d1_metrics_service/.venv/lib/python3.5/site-packages/d1_metrics_service/pid_resolution.py", line 333, in _work, referer: https://search.dataone.org/data
[Wed Oct 17 15:20:04.165232 2018] [wsgi:error] [pid 81989:tid 140338808350464] [client 73.128.224.157:49760]     for response in await asyncio.gather(*tasks):, referer: https://search.dataone.org/data
[Wed Oct 17 15:20:04.165252 2018] [wsgi:error] [pid 81989:tid 140338808350464] [client 73.128.224.157:49760]   File "/usr/lib/python3.5/asyncio/futures.py", line 361, in __iter__, referer: https://search.dataone.org/data
[Wed Oct 17 15:20:04.165278 2018] [wsgi:error] [pid 81989:tid 140338808350464] [client 73.128.224.157:49760]     yield self  # This tells Task to wait for completion., referer: https://search.dataone.org/data
[Wed Oct 17 15:20:04.165322 2018] [wsgi:error] [pid 81989:tid 140338808350464] [client 73.128.224.157:49760] RuntimeError: Task <Task pending coro=<getResolvePIDs.<locals>._work() running at /var/local/metrics-service/src/d1_metrics_service/.venv/lib/python3.5/site-packages/d1_metrics_service/pid_resolution.py:333> cb=[_run_until_complete_cb() at /usr/lib/python3.5/asyncio/base_events.py:164]> got Future <_GatheringFuture pending> attached to a different loop, referer: https://search.dataone.org/data
Exception ignored in: <bound method BaseEventLoop.__del__ of <_UnixSelectorEventLoop running=False closed=False debug=False>>
Traceback (most recent call last):
  File "/usr/lib/python3.5/asyncio/base_events.py", line 429, in __del__
NameError: name 'ResourceWarning' is not defined
Exception ignored in: <bound method BaseEventLoop.__del__ of <_UnixSelectorEventLoop running=False closed=False debug=False>>
Traceback (most recent call last):
  File "/usr/lib/python3.5/asyncio/base_events.py", line 429, in __del__
NameError: name 'ResourceWarning' is not defined
Exception ignored in: <bound method BaseEventLoop.__del__ of <_UnixSelectorEventLoop running=False closed=False debug=False>>
Traceback (most recent call last):
  File "/usr/lib/python3.5/asyncio/base_events.py", line 429, in __del__
NameError: name 'ResourceWarning' is not defined
[Wed Oct 17 15:21:05.999354 2018] [mpm_event:notice] [pid 81985:tid 140339023230848] AH00491: caught SIGTERM, shutting down