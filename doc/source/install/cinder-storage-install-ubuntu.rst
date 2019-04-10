Install and configure a storage node
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Prerequisites
-------------

Before you install and configure the Block Storage service on the
storage node, you must prepare the storage device.

.. note::

   Perform these steps on the storage node.

#. Install the supporting utility packages:

   .. code-block:: console

      # apt install lvm2 thin-provisioning-tools

   .. end

   .. note::

      Some distributions include LVM by default.

#. Create the LVM physical volume ``/dev/sdb``:

   .. code-block:: console

      # pvcreate /dev/sdb

      Physical volume "/dev/sdb" successfully created

   .. end

#. Create the LVM volume group ``cinder-volumes``:

   .. code-block:: console

      # vgcreate cinder-volumes /dev/sdb

      Volume group "cinder-volumes" successfully created

   .. end

   The Block Storage service creates logical volumes in this volume group.

#. Only instances can access Block Storage volumes. However, the
   underlying operating system manages the devices associated with
   the volumes. By default, the LVM volume scanning tool scans the
   ``/dev`` directory for block storage devices that
   contain volumes. If projects use LVM on their volumes, the scanning
   tool detects these volumes and attempts to cache them which can cause
   a variety of problems with both the underlying operating system
   and project volumes. You must reconfigure LVM to scan only the devices
   that contain the ``cinder-volumes`` volume group. Edit the
   ``/etc/lvm/lvm.conf`` file and complete the following actions:

   * In the ``devices`` section, add a filter that accepts the
     ``/dev/sdb`` device and rejects all other devices:

     .. path /etc/lvm/lvm.conf
     .. code-block:: bash

        devices {
        ...
        filter = [ "a/sdb/", "r/.*/"]

     .. end

     Each item in the filter array begins with ``a`` for **accept** or
     ``r`` for **reject** and includes a regular expression for the
     device name. The array must end with ``r/.*/`` to reject any
     remaining devices. You can use the :command:`vgs -vvvv` command
     to test filters.

     .. warning::

        If your storage nodes use LVM on the operating system disk, you
        must also add the associated device to the filter. For example,
        if the ``/dev/sda`` device contains the operating system:

        .. ignore_path /etc/lvm/lvm.conf
        .. code-block:: ini

           filter = [ "a/sda/", "a/sdb/", "r/.*/"]

        .. end

        Similarly, if your compute nodes use LVM on the operating
        system disk, you must also modify the filter in the
        ``/etc/lvm/lvm.conf`` file on those nodes to include only
        the operating system disk. For example, if the ``/dev/sda``
        device contains the operating system:

        .. path /etc/openstack-dashboard/local_settings.py
        .. code-block:: ini

           filter = [ "a/sda/", "r/.*/"]

        .. end

Install and configure components
--------------------------------

#. Install the packages:

   .. code-block:: console

     # apt install cinder-volume

   .. end


#. Edit the ``/etc/cinder/cinder.conf`` file
   and complete the following actions:

   * In the ``[database]`` section, configure database access:

     .. path /etc/cinder/cinder.conf
     .. code-block:: ini

        [database]
        # ...
        connection = mysql+pymysql://cinder:CINDER_DBPASS@controller/cinder

     .. end

     Replace ``CINDER_DBPASS`` with the password you chose for
     the Block Storage database.

   * In the ``[DEFAULT]`` section, configure ``RabbitMQ``
     message queue access:

     .. path /etc/cinder/cinder.conf
     .. code-block:: ini

        [DEFAULT]
        # ...
        transport_url = rabbit://openstack:RABBIT_PASS@controller

     .. end

     Replace ``RABBIT_PASS`` with the password you chose for
     the ``openstack`` account in ``RabbitMQ``.

   * In the ``[DEFAULT]`` and ``[keystone_authtoken]`` sections,
     configure Identity service access:

     .. path /etc/cinder/cinder.conf
     .. code-block:: ini

        [DEFAULT]
        # ...
        auth_strategy = keystone

        [keystone_authtoken]
        # ...
        www_authenticate_uri = http://controller:5000
        auth_url = http://controller:5000
        memcached_servers = controller:11211
        auth_type = password
        project_domain_name = default
        user_domain_name = default
        project_name = service
        username = cinder
        password = CINDER_PASS

     .. end

     Replace ``CINDER_PASS`` with the password you chose for the
     ``cinder`` user in the Identity service.

     .. note::

        Comment out or remove any other options in the
        ``[keystone_authtoken]`` section.

   * In the ``[DEFAULT]`` section, configure the ``my_ip`` option:

     .. path /etc/cinder/cinder.conf
     .. code-block:: ini

        [DEFAULT]
        # ...
        my_ip = MANAGEMENT_INTERFACE_IP_ADDRESS

     .. end

     Replace ``MANAGEMENT_INTERFACE_IP_ADDRESS`` with the IP address
     of the management network interface on your storage node,
     typically 10.0.0.41 for the first node in the
     `example architecture <https://docs.openstack.org/install-guide/overview.html#example-architecture>`_.


   * In the ``[lvm]`` section, configure the LVM back end with the
     LVM driver, ``cinder-volumes`` volume group, iSCSI protocol,
     and appropriate iSCSI service:

     .. path /etc/cinder/cinder.conf
     .. code-block:: ini

        [lvm]
        # ...
        volume_driver = cinder.volume.drivers.lvm.LVMVolumeDriver
        volume_group = cinder-volumes
        target_protocol = iscsi
        target_helper = tgtadm

     .. end

   * In the ``[DEFAULT]`` section, enable the LVM back end:

     .. path /etc/cinder/cinder.conf
     .. code-block:: ini

        [DEFAULT]
        # ...
        enabled_backends = lvm

     .. end

     .. note::

        Back-end names are arbitrary. As an example, this guide
        uses the name of the driver as the name of the back end.

   * In the ``[DEFAULT]`` section, configure the location of the
     Image service API:

     .. path /etc/cinder/cinder.conf
     .. code-block:: ini

        [DEFAULT]
        # ...
        glance_api_servers = http://controller:9292

     .. end

   * In the ``[oslo_concurrency]`` section, configure the lock path:

     .. path /etc/cinder/cinder.conf
     .. code-block:: ini

        [oslo_concurrency]
        # ...
        lock_path = /var/lib/cinder/tmp

     .. end


Finalize installation
---------------------

#. Restart the Block Storage volume service including its dependencies:

   .. code-block:: console

      # service tgt restart
      # service cinder-volume restart

   .. end

