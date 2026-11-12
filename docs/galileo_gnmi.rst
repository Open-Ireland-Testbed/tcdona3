Galileo gNMI API
================

Capabilities
**************************

The Galileo optical transport system exposes an OpenConfig data model over gNMI. The ``galileo_gnmi`` subpackage provides Python APIs for monitoring and configuring the Terra Flex line terminals.

The monitoring API is read-only by design: it invokes only ``gnmic get`` and exposes no Set, update, replace, delete, or subscribe operation. Configuration is handled by a separate write-query API that builds typed ``gnmic set`` requests.

.. Caution::

   The write-query API changes device configuration. Both ends of a link must use matching operational mode and frequency.

Environment
**************************

Export the endpoint settings before use:

.. code-block:: bash

   export GALILEO=192.0.2.10 GNMI_USER=admin
   read -rs -p 'gNMI password: ' GNMI_PASS; export GNMI_PASS

``GNMI_PORT`` defaults to ``51052``.

Monitoring
**************************

.. code-block:: python

   from galileo_gnmi import GalileoTerraFlexAPI, ReadOnlyGNMISession

   with ReadOnlyGNMISession.from_environment() as gnmi:
       api = GalileoTerraFlexAPI(gnmi)
       status = api.get_line_status("linecard-1-line-opt-1-1", 111040101)
       print(status)

Available monitoring methods: ``get_system``, ``get_component``, ``list_components``, ``get_logical_channel``, ``list_logical_channels``, ``list_optical_channels``, ``get_line_status``, ``poll_line_status``, and ``get_alarms``.

Configuration
**************************

``GalileoTerraFlexWriteQueries`` builds typed ``gnmic set`` requests for line configuration. Call ``execute(query)`` to send one to the configured device.

.. automodule:: galileo_gnmi.terra_flex
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: galileo_gnmi.terra_flex_config
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: galileo_gnmi.nec_nos
   :members:
   :undoc-members:
   :show-inheritance:
