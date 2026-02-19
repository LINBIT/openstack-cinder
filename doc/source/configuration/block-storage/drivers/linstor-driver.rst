==============
LINSTOR driver
==============

The LINSTOR driver allows Cinder to use DRBD/LINSTOR instances.

Requirements
~~~~~~~~~~~~

- LINSTOR 1.4.0 or later
- ``python-linstor`` package with ``MultiLinstor`` support

External package installation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The driver requires the ``python-linstor`` package for communication with the
LINSTOR Controller. Install the package from PYPI using the following command:

.. code-block:: console

   $ python -m pip install python-linstor

Configuration
~~~~~~~~~~~~~

Set the following option in the ``cinder.conf`` file to use DRBD direct
attach (requires the Cinder host to be part of the LINSTOR cluster):

.. code-block:: ini

   volume_driver = cinder.volume.drivers.linstordrv.LinstorDrbdDriver

Or use the following for iSCSI transport:

.. code-block:: ini

   volume_driver = cinder.volume.drivers.linstordrv.LinstorIscsiDriver

Both ``LinstorDrbdDriver`` and ``LinstorIscsiDriver`` are aliases for the
unified ``LinstorDriver`` class with the ``linstor_direct`` option set
appropriately.

Volume types and resource groups
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Cinder volume types map directly to LINSTOR Resource Groups. When a volume
is created with a given volume type, the driver creates or reuses a resource
group whose name is derived from the volume type ID (prefixed with
``cinder-``). Volumes created without a volume type use the resource group
specified by ``linstor_default_resource_group_name`` (default:
``DfltRscGrp``).

Resource group settings (replica count, storage pool, DRBD options, etc.)
can be configured either directly in LINSTOR or via ``linstor:*`` extra
specs on the Cinder volume type. For example, to create a volume type
with three replicas spread across different availability zones:

.. code-block:: console

   $ openstack volume type create linstor-3x-az
   $ openstack volume type set linstor-3x-az \
       --property linstor:redundancy=3 \
       --property linstor:replicas_on_different=Aux/zone

Supported ``linstor:*`` extra specs:

.. list-table::
   :header-rows: 1
   :widths: 30 10 60

   * - Key
     - Type
     - Description
   * - ``linstor:storage_pool``
     - str
     - Storage pool to use when auto-placing. Defaults to
       ``linstor_default_storage_pool_name``.
   * - ``linstor:redundancy``
     - int
     - Number of replicas to create. Defaults to 2.
   * - ``linstor:replicas_on_same``
     - str
     - Comma-separated list of ``key`` or ``key=value`` autoplacement labels
       requiring replicas to be placed on nodes sharing the same property.
   * - ``linstor:replicas_on_different``
     - str
     - Comma-separated list of ``key`` or ``key=value`` autoplacement labels
       requiring replicas to be placed on nodes with differing property values.
   * - ``linstor:diskless_on_remaining``
     - bool
     - Create diskless replicas on non-selected nodes after auto-placing.
       Defaults to ``False``.
   * - ``linstor:layer_list``
     - str
     - Comma-separated list of layers to apply. Defaults to ``DRBD,Storage``.
   * - ``linstor:provider_list``
     - str
     - Comma-separated list of storage providers to use.
   * - ``linstor:do_not_place_with_regex``
     - str
     - Do not place the resource on nodes that have a resource whose name
       matches this regex.

In addition, arbitrary LINSTOR resource group properties can be set using
the ``linstor:property:<name>`` pattern:

.. code-block:: console

   $ openstack volume type set linstor-type \
       --property linstor:property:DrbdOptions/PeerDevice/c-max-rate=100M


The following table contains the configuration options supported by the
LINSTOR driver:

.. config-table::
   :config-target: LINSTOR

   cinder.volume.drivers.linstordrv
