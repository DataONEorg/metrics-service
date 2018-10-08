DataONE Metrics Service implementation
==============================

.. figure:: ../images/dataone-implementation.png
    :align: center
    :alt: High level DataONE Metrics implementation
    :figclass: align-center

This figure shows a high level implementation of the DataONE Metrics Service.
Whenever a read event occurs at the Member Node (MN) or the Co-ordinating Node (CN) level, the
DataONE API logs that event and writes it to the disk for further processing.
This processing of the logs is done based on the `COUNTER Code of Practice <https://peerj.com/preprints/26505>`_
at the logprocessor. The Metrics Reporting service generates reports based on the
`DataCite SUSHI API <https://www.niso.org/schemas/sushi>`_
and send them to the DataCite HUB. The DataONE Metrics API uses these processed logs
to drive the metrics on the MetacatUI - a client-side web interface for querying Metacat
servers and other servers that implement the DataONE REST API.