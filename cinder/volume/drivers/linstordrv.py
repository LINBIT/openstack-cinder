# Copyright (c) 2014-2018 LINBIT HA Solutions GmbH
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.


"""

This driver connects Cinder to an installed Linstor instance, see
https://docs.linbit.com/docs/users-guide-9.0/#ch-openstack
for more details.

"""

from eventlet import greenthread
import json
import six
import socket
import time
import uuid
import sys

from oslo_concurrency import processutils
from oslo_config import cfg
from oslo_log import log as logging
from oslo_utils import excutils
from oslo_utils import importutils
from oslo_utils import units

from cinder import exception
from cinder.i18n import _
from cinder import interface
from cinder.volume import configuration
from cinder.volume import driver

import linstor


# To override these values, update cinder.conf in /etc/cinder/
linstor_opts = [

    cfg.IntOpt('linstor_redundancy',
               default=1,
               help='Number of nodes that should replicate the data.'),
    cfg.StrOpt('linstor_disk_options',
               default='{"c-min-rate": "4M"}',
               help='Disk options to set on new resources. '
                    'See http://www.drbd.org/en/doc/users-guide-90/re-drbdconf'
                    ' for all the details.'),
    cfg.StrOpt('linstor_net_options',
               default='{"connect-int": "4", "allow-two-primaries": "yes", '
                       '"ko-count": "30", "max-buffers": "20000", '
                       '"ping-timeout": "100"}',
               help='Net options to set on new resources. '
                    'See http://www.drbd.org/en/doc/users-guide-90/re-drbdconf'
                    ' for all the details.'),
    cfg.StrOpt('linstor_resource_options',
               default='{"auto-promote-timeout": "300"}',
               help='Resource options to set on new resources. '
                    'See http://www.drbd.org/en/doc/users-guide-90/re-drbdconf'
                    ' for all the details.'),

    cfg.StrOpt('linstor_default_volume_group_name',
               default='vg-1',
               help='Default Volume Group name for Linstor. Not Cinder Volume.'),

    cfg.StrOpt('linstor_default_uri',
               default='linstor://localhost',
               help='Default storate URI for Linstor.'),

    cfg.StrOpt('linstor_default_storage_pool_name',
               default='DfltStorPool',
               help='Default Storage Pool name for Linstor.'),

    cfg.IntOpt('linstor_default_resource_size',
               default=1024000,
               help='Default resource size in Kibibytes.  1024000 = 1GiB'),

    cfg.FloatOpt('linstor_snapshot_upscale_factor',
               default=1.1,
               help='Default Upscaling size factor for a Linstor snapshot.'),

]
LOG = logging.getLogger(__name__)

CONF = cfg.CONF
CONF.register_opts(linstor_opts, group=configuration.SHARED_CONF_GROUP)

# CINDER
CINDER_UNKNOWN = 'unknown'
DM_VN_PREFIX = 'CV_'
DM_SN_PREFIX = 'SN_'
LVM = 'Lvm'


class LinstorBaseDriver(driver.BaseVD):
    """Cinder driver that uses Linstor for storage."""

    VERSION = '0.0.3'

    # ThirdPartySystems wiki page
    CI_WIKI_NAME = 'Cinder_Jenkins'

    def __init__(self, *args, **kwargs):
        super(LinstorBaseDriver, self).__init__(*args, **kwargs)
        LOG.debug('START: Base Init Linstor')

        self.configuration.append_config_values(linstor_opts)
        self.default_pool = self.configuration.safe_get('linstor_default_storage_pool_name')
        self.default_uri = self.configuration.safe_get('linstor_default_uri')
        self.default_rsc_size = self.configuration.safe_get('linstor_default_resource_size')
        self.default_snap_factor = self.configuration.safe_get('linstor_snapshot_upscale_factor')
        self.default_vg_name = self.configuration.safe_get('linstor_default_volume_group_name')

        # LOG.debug('CONFIG URI: '+str(self.default_uri))

    def _clean_uuid(self):
        """Returns a UUID string, WITHOUT braces."""
        # Some uuid library versions put braces around the result.
        # We don't want them, just a plain [0-9a-f-]+ string.
        id = str(uuid.uuid4())
        id = id.replace("{", "")
        id = id.replace("}", "")
        return id

    def _is_clean_volume_name(self, name, prefix):
        try:
            if (name.startswith(CONF.volume_name_template % "") and
                    uuid.UUID(name[7:]) is not None):
                return prefix + name[7:]
        except ValueError:
            return None

        try:
            if uuid.UUID(name) is not None:
                return prefix + name
        except ValueError:
            return None

    def _snapshot_name_from_cinder_snapshot(self, snapshot):
        sn_name = self._is_clean_volume_name(snapshot['id'], DM_SN_PREFIX)
        LOG.debug('SNAP NAME: '+str(sn_name))
        return sn_name

    def _cinder_volume_name_from_drbd_resource(self, rsc_name):
        cinder_volume_name = rsc_name.split(DM_VN_PREFIX)[1]
        return cinder_volume_name

    def _drbd_resource_name_from_cinder_snapshot(self, snapshot):
        drbd_resource_name = DM_VN_PREFIX + str(snapshot['volume_id'])
        LOG.debug('RSC NAME: ' + str(drbd_resource_name))
        return drbd_resource_name

    def _drbd_resource_name_from_cinder_volume(self, volume):
        drbd_resource_name = DM_VN_PREFIX + str(volume['id'])
        return drbd_resource_name

    def _get_storage_pool(self):

        LOG.debug("ENTER: _get_sp @ DRBD")

        with linstor.Linstor(self.default_uri) as lin:

            if not lin.connected:
                lin.connect()

            # Fetch Storage Pool List
            sp_list_reply = lin.storage_pool_list() # TODO(wp)-Maybe use lin.volume_list()
            assert len(str(sp_list_reply[0].proto_msg)), "Empty Storage Pool list"

            # Fetch Resource Definition List

            sp_list = []
            node_count = 0
            for node in sp_list_reply[0].proto_msg.stor_pools:
                sp_node = {}
                sp_node['node_uuid'] = node.node_uuid
                sp_node['node_name'] = node.node_name
                sp_node['sp_uuid'] = node.stor_pool_uuid
                sp_node['sp_name'] = node.stor_pool_name

                for prop in node.props:
                    if "Vg" in prop.key:
                        sp_node['vg_name'] = prop.value
                    if "ThinPool" in prop.key:
                        #LOG.debug(prop.value+" is a thinpool")
                        thin_pool = True

                # Free Space
                #
                # 1. Converted to GiB for cinder
                #
                # 2. Trying to optimize below causes incorrect result.
                #    ex. node.free_space.free_space * (units.Ki / units.Gi) is wrong for 2.7
                if thin_pool:
                    # TODO(wp) - Update w/ latest linstor-server release
                    sp_node['sp_free'] = CINDER_UNKNOWN
                else:
                    sp_node['sp_free'] = round(node.free_space.free_capacity / (units.Gi / units.Ki), 2)

                # Driver
                if node.driver == "LvmDriver":
                    sp_node['driver_name'] = LVM
                else:
                    sp_node['driver_name'] = node.driver

                sp_list.append(sp_node)
                node_count += 1

            LOG.debug('Found ' + str(node_count) + ' storage pools.')
            LOG.debug(sp_list)

            LOG.debug("EXIT: _get_sp @ DRBD")
            return sp_list

    def _get_volume_stats(self):

        data = {}
        data["volume_backend_name"] = self.configuration.safe_get('volume_backend_name')
        data["vendor_name"] = 'Open Source'
        data["driver_version"] = self.VERSION
        data["pools"] = []

        sp_data = self._get_storage_pool()
        rd_list = self._get_resource_definitions()

        # Total volumes and capacity
        num_vols = 0
        total_capacity_gb = 0
        for rd in rd_list:
            LOG.debug("VOL RD"+str(rd))
            num_vols += 1
            if 'rd_size' in rd:
                total_capacity_gb += rd['rd_size']

        # LOG.debug('VOL SP:'+str(sp_data[0]["sp_free"]))

        location_info = 'LinstorDrbdDriver:'+self.default_uri

        single_pool = {}
        single_pool["pool_name"] = data["volume_backend_name"]
        single_pool["free_capacity_gb"] = sp_data[0]['sp_free']
        single_pool["total_capacity_gb"] = total_capacity_gb
        single_pool["reserved_percentage"] = self.configuration.reserved_percentage
        single_pool["location_info"] = location_info
        single_pool["total_volumes"] = num_vols
        single_pool["filter_function"] = self.get_filter_function()
        single_pool["goodness_function"] = self.get_goodness_function()
        single_pool["QoS_support"] = False

        data["pools"].append(single_pool)

        return data

    # TODO(wp) Placeholder for the moment
    def _get_local_path(self, volume):
        pass

    def _get_resource_definitions(self):

        LOG.debug("ENTER: _get_resource_definitions @ DRBD")

        rd_list = []
        with linstor.Linstor(self.default_uri) as lin:

            if not lin.connected:
                lin.connect()

            rd_list_reply = lin.resource_dfn_list()

            for node in rd_list_reply[0].proto_msg.rsc_dfns:

                # Count only Cinder volumes
                if DM_VN_PREFIX in node.rsc_name:
                    rd_node = {}
                    rd_node['rd_uuid'] = node.rsc_dfn_uuid
                    rd_node['rd_name'] = node.rsc_name
                    rd_node['rd_port'] = node.rsc_dfn_port
                    # rd_node['rd_secret'] = node.rsc_dfn_secret

                    for vol in node.vlm_dfns:
                        if vol.vlm_nr == 0:
                            rd_node['rd_size'] = round(float(vol.vlm_size) / units.Mi / self.default_snap_factor, 2)

                    rd_list.append(rd_node)

        LOG.debug("EXIT: _get_resource_definitions @ DRBD")
        return rd_list

    def _get_resource_nodes(self, resource):
        """
        Returns all available resource nodes in a given DRBD cluster
        resource: Un-encoded backend resource name
        """
        with linstor.Linstor(self.default_uri) as lin:
            rsc_list_reply = lin.resource_list(filter_by_resources=resource)

            rsc_list = []
            for node in rsc_list_reply[0].proto_msg.resource_states:
                rsc_list.append(node.node_name)

            return rsc_list

    def _get_linstor_nodes(self):
        """
        Returns all available DRBD nodes
        """
        with linstor.Linstor(self.default_uri) as lin:
            node_list_reply = lin.node_list()

            node_list = []
            for node in node_list_reply[0].proto_msg.nodes:
                node_list.append(node.name)

            return node_list

    def _debug_api_reply(self, api_response):
        for response in api_response:
            LOG.debug("API: "+str(response))

        return linstor.Linstor.all_api_responses_success(api_response)

    def do_setup(self, context):
        super(LinstorBaseDriver, self).do_setup(context)

    #
    # Snapshot
    #
    def create_snapshot(self, snapshot):
        LOG.debug('ENTER: create_snapshot @ DRBD Base')

        snap_name = self._snapshot_name_from_cinder_snapshot(snapshot)
        drbd_rsc_name = self._drbd_resource_name_from_cinder_snapshot(snapshot)
        node_names = self._get_resource_nodes(drbd_rsc_name)

        with linstor.Linstor(self.default_uri) as lin:
            snap_reply = lin.snapshot_create(node_names=node_names,
                                             rsc_name=drbd_rsc_name,
                                             snapshot_name=snap_name,
                                             async=False)

            if not self._debug_api_reply(snap_reply):
                raise exception.VolumeBackendAPIException("ERROR on creating a DRBD Linstor snapshot")

        LOG.debug('EXIT: create_snapshot @ DRBD Base')

    def delete_snapshot(self, snapshot):

        LOG.debug('ENTER: delete_snapshot @ DRBD Base')

        snap_name = self._snapshot_name_from_cinder_snapshot(snapshot)
        drbd_rsc_name = self._drbd_resource_name_from_cinder_snapshot(snapshot)

        with linstor.Linstor(self.default_uri) as lin:

            # Check Connection
            if not lin.connected:
                lin.connect()

            snap_reply = lin.snapshot_delete(rsc_name=drbd_rsc_name,
                                             snapshot_name=snap_name)

            if not self._debug_api_reply(snap_reply):
                raise exception.VolumeBackendAPIException("ERROR on deleting a DRBD Linstor snapshot")

            # Delete RD if no other RSC are found
            if len(self._get_resource_nodes(drbd_rsc_name)) == 0:
                rd_reply = lin.resource_dfn_delete(drbd_rsc_name)

                self._debug_api_reply(rd_reply)

        LOG.debug('EXIT: delete_snapshot @ DRBD Base')

    def create_volume_from_snapshot(self, volume, snapshot):

        LOG.debug('ENTER: create_volume_from_snapshot @ DRBD Base')
        LOG.debug('VOL: ' + str(volume))
        LOG.debug('SNAP CTXT: ' + str(snapshot))
        src_rsc_name = self._drbd_resource_name_from_cinder_snapshot(snapshot)
        src_snap_name = self._snapshot_name_from_cinder_snapshot(snapshot)
        new_vol_name = self._drbd_resource_name_from_cinder_volume(volume)

        with linstor.Linstor(self.default_uri) as lin:

            # Check Connection
            if not lin.connected:
                lin.connect()

            # New RD
            rd_reply = lin.resource_dfn_create(new_vol_name)
            if not self._debug_api_reply(rd_reply):
                print("Error on creating a new RD")

            # New VD from Snap
            vd_reply = lin.snapshot_volume_definition_restore(src_rsc_name, src_snap_name, new_vol_name)
            if not self._debug_api_reply(vd_reply):
                print("Error on creating a new VD from snap")

            # New RSC from Snap
            # Assumes restoring to all the available nodes
            nodes = self._get_linstor_nodes()
            rsc_reply = lin.snapshot_resource_restore(nodes, src_rsc_name, src_snap_name, new_vol_name)
            if not self._debug_api_reply(rsc_reply):
                print("Error on creating RSCs from snap")

            # Upsize if larger volume than original snapshot
            src_rsc_size = int(snapshot['volume_size'])
            new_vol_size = int(volume['size'])

            if new_vol_name > src_rsc_size:

                upsize_target_name = self._is_clean_volume_name(volume['id'], DM_VN_PREFIX)

                snap_reply = lin.volume_dfn_modify(
                    rsc_name=upsize_target_name,
                    volume_nr=0,
                    size=int(self.default_snap_factor * new_vol_size * units.Gi / units.Ki))

                if not self._debug_api_reply(snap_reply):
                    print("ERROR Linstor Volume Extend")

        LOG.debug('EXIT: create_volume_from_snapshot @ DRBD Base')

    # TODO(wp) Test
    def revert_to_snapshot(self, context, volume, snapshot):

        LOG.debug('ENTER: revert_to_snapshot @ DRBD Base')
        LOG.debug('VOL: ' + str(volume))
        LOG.debug('SNAP CTXT: ' + str(snapshot))
        src_rsc_name = self._drbd_resource_name_from_cinder_snapshot(snapshot)
        src_snap_name = self._snapshot_name_from_cinder_snapshot(snapshot)

        # new_rsc_name = self._drbd_resource_name_from_cinder_volume(volume)
        #src_src_name should match new_rsc_name

        with linstor.Linstor(self.default_uri) as lin:

            # Check Connection
            if not lin.connected:
                lin.connect()

            # Delete existing RSCs before restoration
            rsc_list_reply = lin.resource_list()

            for node in rsc_list_reply[0].proto_msg.resource_states:
                if node.rsc_name == src_rsc_name:
                    # print(node)
                    LOG.debug('VOL Deleting ' + node.rsc_name + ' @ ' + node.node_name)

                    rsc_reply = lin.resource_delete(node.node_name, src_rsc_name)
                    self._debug_api_reply(rsc_reply)
                    time.sleep(1)

            # Delete existing VD before restoration
            print('VOL Deleting Volume Definition for ' + src_rsc_name)
            vd_reply = lin.volume_dfn_delete(src_rsc_name, 0)
            self._debug_api_reply(vd_reply)
            time.sleep(1)

            # Restore a VD from Snap
            vd_reply = lin.snapshot_volume_definition_restore(src_rsc_name, src_snap_name, src_rsc_name)
            if not self._debug_api_reply(vd_reply):
                print("VOL ERROR on creating a new VD from snap")

            # Restore old RSCs from Snap
            # Assumes restoring to all the available nodes
            nodes = self._get_linstor_nodes()
            rsc_reply = lin.snapshot_resource_restore(nodes, src_rsc_name, src_snap_name, src_rsc_name)
            if not self._debug_api_reply(rsc_reply):
                print("VOL ERROR on creating RSCs from snap")

            # Upsize if larger volume than original snapshot
            src_rsc_size = int(snapshot['volume_size'])
            new_vol_size = int(volume['size'])

            if src_rsc_name > src_rsc_size:

                upsize_target_name = self._is_clean_volume_name(volume['id'], DM_VN_PREFIX)

                snap_reply = lin.volume_dfn_modify(
                    rsc_name=upsize_target_name,
                    volume_nr=0,
                    size=int(self.default_snap_factor * new_vol_size * units.Gi / units.Ki))

                if not self._debug_api_reply(snap_reply):
                    print("VOL ERROR Linstor Volume Extend")

        LOG.debug('EXIT: revert_to_snapshot @ DRBD Base')
    #
    # Volume
    #
    # TODO (wp) Test
    def create_cloned_volume(self, volume, src_vref):
        temp_id = self._clean_uuid()
        snapshot = {'id': temp_id}

        self.create_snapshot({'id': temp_id,
                              'volume_id': src_vref['id']})

        snapshot['volume_size'] = src_vref['size']
        self._create_volume_from_snapshot(volume, snapshot)

        self.delete_snapshot(snapshot)


# Class with iSCSI interface methods
@interface.volumedriver
class LinstorIscsiDriver(LinstorBaseDriver):
    """Cinder iSCSI driver that uses Linstor for storage."""

    def __init__(self, *args, **kwargs):
        super(LinstorIscsiDriver, self).__init__(*args, **kwargs)
        target_driver = self.target_mapping[
            self.configuration.safe_get('target_helper')]

        LOG.info('START: Linstor iSCSI driver')

        self.target_driver = importutils.import_object(
            target_driver,
            configuration=self.configuration,
            db=self.db,
            executor=self._execute)

    def get_volume_stats(self, refresh=False):

        LOG.debug('ENTER: get_volume_stats @ iSCSI')

        data = self._get_volume_stats()
        data["storage_protocol"] = 'iSCSI'

        LOG.debug('EXIT: get_volume_stats @ iSCSI')

        return data

    # TODO(wp)
    def ensure_export(self, context, volume):
        # volume_path = self.local_path(volume)
        # return self.target_driver.ensure_export(
        #     context,
        #     volume,
        #     volume_path)
        pass

    # TODO(wp)
    def create_export(self, context, volume, connector):
        # volume_path = self.local_path(volume)
        # export_info = self.target_driver.create_export(
        #     context,
        #     volume,
        #     volume_path)
        #
        # return {'provider_location': export_info['location'],
        #         'provider_auth': export_info['auth'], }
        pass

    def remove_export(self, context, volume):
        return self.target_driver.remove_export(context, volume)

    def initialize_connection(self, volume, connector):
        return self.target_driver.initialize_connection(volume, connector)

    def validate_connector(self, connector):
        return self.target_driver.validate_connector(connector)

    def terminate_connection(self, volume, connector, **kwargs):
        return self.target_driver.terminate_connection(volume,
                                                       connector,
                                                       **kwargs)


# Class with DRBD transport mode
@interface.volumedriver
class LinstorDrbdDriver(LinstorBaseDriver):
    """Cinder DRBD driver that uses Linstor for storage."""

    def __init__(self, *args, **kwargs):
        LOG.debug('START: Linstor DRBD driver')

        super(LinstorDrbdDriver, self).__init__(*args, **kwargs)

    def _get_nodes(self):

        LOG.debug("ENTER: _get_nodes @ DRBD")

        with linstor.Linstor(self.default_uri) as lin:

            if not lin.connected:
                lin.connect()

            # Get Node List
            node_list_reply = lin.node_list()
            assert node_list_reply, "Empty response"

            node_list = []
            if len(str(node_list_reply[0])) == 0:
                LOG.debug("No LINSTOR nodes found on the network.")
            else:

                for node in node_list_reply[0].proto_msg.nodes:
                    # LOG.info('NODE: '+node.name+' = '+node.uuid+' = '+node.net_interfaces[0].address+'\n')
                    node_item = {}
                    node_item['node_name'] = node.name
                    node_item['node_uuid'] = node.uuid
                    node_item['node_address'] = node.net_interfaces[0].address
                    node_list.append(node_item)

            LOG.debug("EXIT: _get_nodes @ DRBD")
            return node_list

    def _get_spd(self):

        LOG.debug("ENTER: _get_spd @ DRBD")

        with linstor.Linstor(self.default_uri) as lin:

            if not lin.connected:
                lin.connect()

            # Storage Pool Definition List
            spd_list_reply = lin.storage_pool_dfn_list()
            assert len(str(spd_list_reply[0])), "Empty Storage Pool Definition list"

            node_list = spd_list_reply[0]
            spd_list = []
            for node in node_list.proto_msg.stor_pool_dfns:
                spd_item = {}
                spd_item['spd_uuid'] = node.uuid
                spd_item['spd_name'] = node.stor_pool_name
                spd_list.append(spd_item)

            LOG.debug("EXIT: _get_spd @ DRBD")
            return spd_list

    def _get_vol(self):

        """
        Local Path = node['volume'][0].device_path+'@'+node['node_name']
        """

        LOG.debug("ENTER: _get_vol @ DRBD")

        with linstor.Linstor(self.default_uri) as lin:

            if not lin.connected:
                lin.connect()

            vol_list_reply = lin.volume_list()

            if len(str(vol_list_reply[0])) == 0:
                LOG.debug("EXIT empty: _get_vol @ DRBD")
                return []
            else:
                vol_list = []
                for volume in vol_list_reply[0].proto_msg.resources:
                    # print(volume)
                    vol_node = {}
                    vol_node['node_name'] = volume.node_name
                    vol_node['rd_name'] = volume.name
                    vol_node['volume'] = volume.vlms
                    vol_list.append(vol_node)

                    LOG.debug("EXIT clean: _get_vol @ DRBD")
                return vol_list

    def _get_local_path(self, volumes):

        LOG.debug('ENTER: _get_local_path @ DRBD')

        host_name = socket.gethostname()
        for volume in volumes:
            if volume['node_name'] == host_name:

                LOG.debug("EXIT: _get_local_path @ DRBD")
                return volume['volume'][0].device_path

        message = _('Local Volume not found.')
        raise exception.VolumeBackendAPIException(data=message)

    def _get_rsc_path(self, rsc_name):

        with linstor.Linstor(self.default_uri) as lin:
            rsc_list_reply = lin.resource_list()
            host_name = socket.gethostname()

            for rsc in rsc_list_reply[0].proto_msg.resources:
                if rsc.name == rsc_name and rsc.node_name == host_name:
                    for volume in rsc.vlms:
                        if volume.vlm_nr == 0:
                            LOG.debug('RSC PATH: '+str(volume.device_path))
                            return volume.device_path

    def _return_drbd_config(self, volume):

        LOG.debug('ENTER-EXIT: _return_drbd_config @ DRBD')
        LOG.debug('VOL ID: ' + str(volume['id']))

        full_rsc_name = self._drbd_resource_name_from_cinder_volume(volume)

        return {
            'driver_volume_type': 'local',
            'data': {
                "device_path": str(self._get_rsc_path(full_rsc_name))
            }
        }

        # return {
        #     'driver_volume_type': 'drbd',
        #     'data': {
        #         'provider_location': "drbd provider",
        #         'device': "drbd device path",
        #         'devices': ["dev/one", "dev/two"],
        #         # 'provider_auth': subst_data['shared-secret'],
        #         # 'config': config,
        #         'name': "drbd rsc one"
        #     }
        # }

    def get_volume_stats(self, refresh=False):

        LOG.debug('ENTER: get_volume_stats @ DRBD')

        data = self._get_volume_stats()
        data["storage_protocol"] = 'DRBD'

        LOG.debug('EXIT: get_volume_stats @ DRBD')

        return data

    def check_for_setup_error(self):

        LOG.debug('ENTER: check_for_setup_error @ DRBD')

        if not linstor:
            msg = _('Linstor not found')
            LOG.error(msg)

            raise exception.VolumeDriverException(message=msg)

        LOG.debug('EXIT: check_for_setup_error @ DRBD')

    def initialize_connection(self, volume, connector):

        LOG.debug('ENTER: initialize_connection @ DRBD Base')

        with linstor.Linstor(self.default_uri) as lin:
            if not lin.connected:
                lin.connect()

            LOG.debug('VOL: ' + str(volume))
            LOG.debug('CON: ' + str(connector))

            rsc_name = self._is_clean_volume_name(volume['id'], DM_VN_PREFIX)


            LOG.debug('EXIT: initialize_connection @ DRBD Base')

            return self._return_drbd_config(volume)

    def create_volume(self, volume):

        LOG.debug('ENTER: create_volume @ DRBD')
        LOG.debug('  Display Name: '+volume['display_name'])
        LOG.debug('  Host        : '+volume['host'])
        LOG.debug('  Volume Size : '+str(volume['size']))

        with linstor.Linstor(self.default_uri) as lin:

            # Check Connection
            if not lin.connected:
                lin.connect()

            # Check for Storage Pool List
            sp_data = self._get_storage_pool()

            # Get default Storage Pool Definition
            # spd_default = self.default_vg_name
            rsc_size = volume['size'] if volume['size'] else self.default_rsc_size

            # No existing Storage Pools found
            if not sp_data:

                # Check for Nodes
                node_list = self._get_nodes()

                if len(node_list) == 0:
                    LOG.debug("Error: No resource nodes available")
                    message = _('No resource nodes available / configured')
                    raise exception.VolumeBackendAPIException(data=message)

                # Create Storage Pool (definition is implicit)
                spd_name = self._get_spd()[0]['spd_name']

                for node in node_list:
                    lin.storage_pool_create(
                        node_name=node['node_name'],
                        storage_pool_name=spd_name,
                        storage_driver=LVM,
                        driver_pool_name=self.default_vg_name)
                    LOG.debug('Created Storage Pool for ' + spd_name + ' @ ' + node['node_name'] + ' in ' + self.default_vg_name)
                # Move on
            else:
                LOG.debug("Found existing Storage Pools")
                # Move on

            LOG.debug('PROG: create_volume @ DRBD')

            # Check Connection
            if not lin.connected:
                lin.connect()

            # Check for RD
            rd_list = lin.resource_dfn_list()
            rsc_name = self._is_clean_volume_name(volume['id'], DM_VN_PREFIX)

            # if len(str(rd_list[0])) == 0:

            # Create a New RD
            # LOG.debug("No existing Resource Definition found.  Created a new one.")
            rd_reply = lin.resource_dfn_create(rsc_name)  # TODO Need error sorting
            self._debug_api_reply(rd_reply)

            rd_list = lin.resource_dfn_list()
            LOG.debug("Created RD: " + str(rd_list[0].proto_msg))

            # Create a New VD
            vd_reply = lin.volume_dfn_create(rsc_name=rsc_name,
                 size=int(self.default_snap_factor*rsc_size*units.Gi / units.Ki))  # size in KiB
            self._debug_api_reply(vd_reply)
            LOG.debug("Created VD: " + str(vd_reply[0].proto_msg))

            # LOG.debug(rd_list[0])

            # Create RSC's
            for node in sp_data:
                rsc_reply = lin.resource_create(rsc_name=rsc_name, node_name=node['node_name'])
                self._debug_api_reply(rsc_reply)

        return {}

    def delete_volume(self, volume):

        LOG.debug('ENTER: delete_volume @ DRBD')
        LOG.debug('  Display Name: '+volume['display_name'])
        LOG.debug('  Host        : '+volume['host'])
        LOG.debug('  Volume Size : '+str(volume['size']))

        with linstor.Linstor(self.default_uri) as lin:

            # Check Connection
            if not lin.connected:
                lin.connect()

            drbd_rsc_name = self._drbd_resource_name_from_cinder_volume(volume)
            rsc_list_reply = lin.resource_list() #filter_by_resources=drbd_rsc_name)

            LOG.debug('  Rsc Name: '+str(drbd_rsc_name))

            if len(str(rsc_list_reply[0])) == 0:
                LOG.debug("No RSCs to delete. Still success per Cinder doc.")

            else:

                # Delete Resources
                for node in rsc_list_reply[0].proto_msg.resource_states:
                    if node.rsc_name == drbd_rsc_name:
                        # print(node)
                        LOG.debug('Deleting ' + node.rsc_name + ' @ ' + node.node_name)

                        rsc_reply = lin.resource_delete(node.node_name, drbd_rsc_name)
                        self._debug_api_reply(rsc_reply)
                        time.sleep(1)

                # Delete VD
                print('Deleting Volume Definition for ' + drbd_rsc_name)
                vd_reply = lin.volume_dfn_delete(drbd_rsc_name, 0)
                self._debug_api_reply(vd_reply)
                time.sleep(1)

                # Delete RD
                print('Deleting Resource Definition for ' + drbd_rsc_name)
                rd_reply = lin.resource_dfn_delete(drbd_rsc_name) # Will fail if snap exists
                self._debug_api_reply(rd_reply)

        LOG.debug('EXIT: delete_volume @ DRBD')

        return True

    def extend_volume(self, volume, new_size):

        LOG.debug('ENTER: extend_volume @ DRBD')
        LOG.debug('  New Size : ' + str(new_size))

        with linstor.Linstor(self.default_uri) as lin:

            # Check Connection
            if not lin.connected:
                lin.connect()

            rsc_target_name = self._is_clean_volume_name(volume['id'], DM_VN_PREFIX)

            snap_reply = lin.volume_dfn_modify(
                rsc_name=rsc_target_name,
                volume_nr=0,
                size=int(self.default_snap_factor * new_size * units.Gi / units.Ki))

            if not self._debug_api_reply(snap_reply):
                print("ERROR Linstor Volume Extend")

        LOG.debug('EXIT: extend_volume @ DRBD')

    # TODO(wp) Not sure if necessary
    def terminate_connection(self, volume, connector, **kwargs):

        LOG.debug('ENTER: terminate_connection @ DRBD Base')
        LOG.debug('VOL: '+str(volume))
        LOG.debug('CON: '+str(connector))
        LOG.debug('EXIT: terminate_connection @ DRBD Base')

    # TODO(wp) Not sure if necessary
    def create_export(self, context, volume, connector):

        LOG.debug('ENTER: create_export @ DRBD')
        LOG.debug('VOL: '+str(volume))
        LOG.debug('CON: '+str(connector))
        LOG.debug('CTXT :'+str(context))
        LOG.debug('EXIT: create_export @ DRBD')

        return self._return_drbd_config(volume)

    # TODO(wp) Not sure if necessary
    def ensure_export(self, context, volume):

        LOG.debug('ENTER: ensure_export @ DRBD')
        LOG.debug('VOL :' + str(volume))
        LOG.debug('CTXT :' + str(context))
        LOG.debug('EXIT: ensure_export @ DRBD')

        return self._return_drbd_config(volume)

    def remove_export(self, context, volume):

        LOG.debug('ENTER: remove_export @ DRBD')
        LOG.debug('VOL: ' + str(volume))

        # TODO(wp) Test disconnect here.  Does not belong in terminate_connection
        # with linstor.Linstor(self.default_uri) as lin:
        #     if lin.connected:
        #         lin.disconnect()

        LOG.debug('EXIT: remove_export @ DRBD')

    # TODO(wp)
    def copy_image_to_volume(self, context, volume, image_service, image_id):

        LOG.debug('ENTER: copy_image_to_volume @ DRBD')
        LOG.debug('VOL :' + str(volume))
        LOG.debug('VOL IMG SVC :' + str(image_service))
        LOG.debug('VOL IMG ID :' + str(image_id))

        # self.create_volume(volume)

        LOG.debug('EXIT: copy_image_to_volume @ DRBD')
        pass

    # TODO(wp)
    def copy_volume_to_image(self, context, volume, image_service, image_meta):
        LOG.debug('ENTER: copy_volume_to_image @ DRBD')
        LOG.debug('VOL :' + str(volume))
        LOG.debug('VOL IMG SVC :' + str(image_service))
        LOG.debug('VOL IMG META :' + str(image_meta))
        LOG.debug('EXIT: copy_volume_to_image @ DRBD')

        pass