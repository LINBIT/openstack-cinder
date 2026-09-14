==============
LINSTOR driver
==============

The LINSTOR driver allows Cinder to use DRBD/LINSTOR instances.

Requirements
~~~~~~~~~~~~

- LINSTOR 1.35.0 or later (REST API 1.29.0)
- ``python-linstor`` 1.29.0 or later

External package installation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The driver requires the ``python-linstor`` package for communication with the
LINSTOR Controller. Install the package from PYPI using the following command:

.. code-block:: console

   $ python -m pip install 'python-linstor>=1.29.0'

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

Host names
~~~~~~~~~~

The driver refers to LINSTOR nodes by their LINSTOR node name in two places:

* The node the ``cinder-volume`` service itself runs on. The driver attaches
  volumes there temporarily, for example to copy an image into a volume, and
  permanently when a transport such as iSCSI is used. The driver looks the
  node up by the ``host`` option of the volume service (without the backend
  suffix) and by the system host name, each first as node name and then as
  the host name the satellite reported (see below), and finally by a network
  interface on the ``my_ip`` option. If none of these finds the node, for
  example because ``cinder-volume`` runs in a container, write the LINSTOR
  node name into a file and point ``linstor_hostname_file`` at it; the file
  then replaces all lookups. The service refuses to start if no node is
  found.

* With direct attach (``LinstorDrbdDriver`` or ``linstor_direct = True``),
  the compute node a volume is attached to. Nova reports its own ``host``
  option in the connector, and the driver resolves it to a LINSTOR node by
  trying these lookups in order until exactly one node matches:

  #. the node whose property named by ``linstor_connector_host_property``
     equals the connector host, if that option is set,
  #. the node with the connector host as its name,
  #. the node whose satellite reported the connector host as its host name
     (``uname -n``, stored by LINSTOR in the node property ``NodeUname``),
  #. the node with a network interface on the IP address Nova reports in
     the connector (its ``my_block_storage_ip`` option, which defaults to
     ``my_ip``).

  Host names are compared ignoring case. A lookup that matches more than one
  node is an error, as is a connector that matches no node at all.

  The name and host name lookups cover deployments where the Nova host name
  is either the LINSTOR node name or the host name of the compute node. The
  address lookup covers deployments that use an unrelated identifier, such
  as a UUID, as the Nova host name; it requires the Nova block storage IP
  address to be one of the node's LINSTOR network interfaces. Where that is
  not the case, set ``linstor_connector_host_property`` to the name of a
  node property, and set that property on every compute node to its Nova
  host name:

  .. code-block:: console

     $ linstor node set-property compute-1 Aux/openstack-host a30aa14d-56ac-49b6-adcd-f8027baf2da2

  .. code-block:: ini

     linstor_connector_host_property = Aux/openstack-host

Live migration
~~~~~~~~~~~~~~

With direct attach, a live migration attaches the volume on the destination
host while it is still in use on the source host, so both hosts have to be
DRBD primary until the migration completes. The driver attaches with
``make-available`` and its ``auto_manage_dual_primary`` option and detaches
with ``unmake-available``. When the resource is in use on another host,
LINSTOR sets ``allow-two-primaries`` (and protocol C if needed) between
source and destination, and reverts it when the source detaches. Detaching
keeps diskful replicas and tiebreakers in place. On a force detach the
driver reverts the make-available on every attached host as far as
possible.

The driver attaches volumes on its own host the same way, for example to
copy an image into a volume or to export it via iSCSI, but without the
``auto_manage_dual_primary`` option: such an attach is never a live
migration, and a volume in use on a compute node must not become primary on
the Cinder host as well.

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
