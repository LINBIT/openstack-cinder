# Copyright (c) 2015 FUJITSU LIMITED
# Copyright (c) 2012 EMC Corporation.
# Copyright (c) 2012 OpenStack Foundation
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
#

"""
Cinder Volume driver for Fujitsu ETERNUS DX S3 series.
"""
import ast
import base64
import hashlib
import time

from defusedxml import ElementTree as ET
from oslo_concurrency import lockutils
from oslo_config import cfg
from oslo_log import log as logging
from oslo_service import loopingcall
from oslo_utils import units
import six

from cinder import exception
from cinder.i18n import _
from cinder import utils
from cinder.volume import configuration as conf

LOG = logging.getLogger(__name__)
CONF = cfg.CONF

try:
    import pywbem
    pywbemAvailable = True
except ImportError:
    pywbemAvailable = False

VOL_PREFIX = "FJosv_"
RAIDGROUP = 2
TPPOOL = 5
SNAPOPC = 4
OPC = 5
RETURN_TO_RESOURCEPOOL = 19
DETACH = 8
INITIALIZED = 2
UNSYNCHRONIZED = 3
BROKEN = 5
PREPARED = 11
REPL = "FUJITSU_ReplicationService"
STOR_CONF = "FUJITSU_StorageConfigurationService"
CTRL_CONF = "FUJITSU_ControllerConfigurationService"
STOR_HWID = "FUJITSU_StorageHardwareIDManagementService"

UNDEF_MSG = 'Undefined Error!!'
JOB_RETRIES = 60
JOB_INTERVAL_SEC = 10

# Error code keyword.
VOLUME_IS_BUSY = 32786
DEVICE_IS_BUSY = 32787
VOLUMENAME_IN_USE = 32788
COPYSESSION_NOT_EXIST = 32793
LUNAME_IN_USE = 4102
LUNAME_NOT_EXIST = 4097  # Only for InvokeMethod(HidePaths).
EC_REC = 3
FJ_ETERNUS_DX_OPT_opts = [
    cfg.StrOpt('cinder_eternus_config_file',
               default='/etc/cinder/cinder_fujitsu_eternus_dx.xml',
               help='config file for cinder eternus_dx volume driver'),
]

POOL_TYPE_dic = {
    RAIDGROUP: 'RAID_GROUP',
    TPPOOL: 'Thinporvisioning_POOL',
}

OPERATION_dic = {
    SNAPOPC: RETURN_TO_RESOURCEPOOL,
    OPC: DETACH,
    EC_REC: DETACH,
}

RETCODE_dic = {
    '0': 'Success',
    '1': 'Method Not Supported',
    '4': 'Failed',
    '5': 'Invalid Parameter',
    '4096': 'Method Parameters Checked - Job Started',
    '4097': 'Size Not Supported',
    '4101': 'Target/initiator combination already exposed',
    '4102': 'Requested logical unit number in use',
    '32769': 'Maximum number of Logical Volume in a RAID group '
             'has been reached',
    '32770': 'Maximum number of Logical Volume in the storage device '
             'has been reached',
    '32771': 'Maximum number of registered Host WWN '
             'has been reached',
    '32772': 'Maximum number of affinity group has been reached',
    '32773': 'Maximum number of host affinity has been reached',
    '32785': 'The RAID group is in busy state',
    '32786': 'The Logical Volume is in busy state',
    '32787': 'The device is in busy state',
    '32788': 'Element Name is in use',
    '32792': 'No Copy License',
    '32793': 'Session is not exist',
    '32796': 'Quick Format Error',
    '32801': 'The CA port is in invalid setting',
    '32802': 'The Logical Volume is Mainframe volume',
    '32803': 'The RAID group is not operative',
    '32804': 'The Logical Volume is not operative',
    '32808': 'No Thin Provisioning License',
    '32809': 'The Logical Element is ODX volume',
    '32811': 'This operation cannot be performed to the NAS resources',
    '32812': 'This operation cannot be performed to the Storage Cluster '
             'resources',
    '32816': 'Fatal error generic',
    '35302': 'Invalid LogicalElement',
    '35304': 'LogicalElement state error',
    '35316': 'Multi-hop error',
    '35318': 'Maximum number of multi-hop has been reached',
    '35324': 'RAID is broken',
    '35331': 'Maximum number of session has been reached(per device)',
    '35333': 'Maximum number of session has been reached(per SourceElement)',
    '35334': 'Maximum number of session has been reached(per TargetElement)',
    '35335': 'Maximum number of Snapshot generation has been reached '
             '(per SourceElement)',
    '35346': 'Copy table size is not setup',
    '35347': 'Copy table size is not enough',
}

CONF.register_opts(FJ_ETERNUS_DX_OPT_opts, group=conf.SHARED_CONF_GROUP)


class FJDXCommon(object):
    """Common code that does not depend on protocol."""

    VERSION = "1.3.0"
    stats = {
        'driver_version': VERSION,
        'free_capacity_gb': 0,
        'reserved_percentage': 0,
        'storage_protocol': None,
        'total_capacity_gb': 0,
        'vendor_name': 'FUJITSU',
        'QoS_support': False,
        'volume_backend_name': None,
    }

    def __init__(self, prtcl, configuration=None):

        self.pywbemAvailable = pywbemAvailable

        self.protocol = prtcl
        self.configuration = configuration
        self.configuration.append_config_values(FJ_ETERNUS_DX_OPT_opts)

        if prtcl == 'iSCSI':
            # Get iSCSI ipaddress from driver configuration file.
            self.configuration.iscsi_ip_address = (
                self._get_drvcfg('EternusISCSIIP'))

    @staticmethod
    def get_driver_options():
        return FJ_ETERNUS_DX_OPT_opts

    def create_volume(self, volume):
        """Create volume on ETERNUS."""
        LOG.debug('create_volume, '
                  'volume id: %(vid)s, volume size: %(vsize)s.',
                  {'vid': volume['id'], 'vsize': volume['size']})

        self.conn = self._get_eternus_connection()
        volumesize = int(volume['size']) * units.Gi
        volumename = self._create_volume_name(volume['id'])

        LOG.debug('create_volume, volumename: %(volumename)s, '
                  'volumesize: %(volumesize)u.',
                  {'volumename': volumename,
                   'volumesize': volumesize})

        # get poolname from driver configuration file
        eternus_pool = self._get_drvcfg('EternusPool')
        # Existence check the pool
        pool = self._find_pool(eternus_pool)

        if 'RSP' in pool['InstanceID']:
            pooltype = RAIDGROUP
        else:
            pooltype = TPPOOL

        configservice = self._find_eternus_service(STOR_CONF)
        if configservice is None:
            msg = (_('create_volume, volume: %(volume)s, '
                     'volumename: %(volumename)s, '
                     'eternus_pool: %(eternus_pool)s, '
                     'Storage Configuration Service not found.')
                   % {'volume': volume,
                      'volumename': volumename,
                      'eternus_pool': eternus_pool})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        LOG.debug('create_volume, '
                  'CreateOrModifyElementFromStoragePool, '
                  'ConfigService: %(service)s, '
                  'ElementName: %(volumename)s, '
                  'InPool: %(eternus_pool)s, '
                  'ElementType: %(pooltype)u, '
                  'Size: %(volumesize)u.',
                  {'service': configservice,
                   'volumename': volumename,
                   'eternus_pool': eternus_pool,
                   'pooltype': pooltype,
                   'volumesize': volumesize})

        # Invoke method for create volume
        rc, errordesc, job = self._exec_eternus_service(
            'CreateOrModifyElementFromStoragePool',
            configservice,
            ElementName=volumename,
            InPool=pool,
            ElementType=self._pywbem_uint(pooltype, '16'),
            Size=self._pywbem_uint(volumesize, '64'))

        if rc == VOLUMENAME_IN_USE:  # Element Name is in use
            LOG.warning('create_volume, '
                        'volumename: %(volumename)s, '
                        'Element Name is in use.',
                        {'volumename': volumename})
            vol_instance = self._find_lun(volume)
            element = vol_instance
        elif rc != 0:
            msg = (_('create_volume, '
                     'volumename: %(volumename)s, '
                     'poolname: %(eternus_pool)s, '
                     'Return code: %(rc)lu, '
                     'Error: %(errordesc)s.')
                   % {'volumename': volumename,
                      'eternus_pool': eternus_pool,
                      'rc': rc,
                      'errordesc': errordesc})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        else:
            element = job['TheElement']

        # Get eternus model name
        try:
            systemnamelist = (
                self._enum_eternus_instances('FUJITSU_StorageProduct'))
        except Exception:
            msg = (_('create_volume, '
                     'volume: %(volume)s, '
                     'EnumerateInstances, '
                     'cannot connect to ETERNUS.')
                   % {'volume': volume})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        LOG.debug('create_volume, '
                  'volumename: %(volumename)s, '
                  'Return code: %(rc)lu, '
                  'Error: %(errordesc)s, '
                  'Backend: %(backend)s, '
                  'Pool Name: %(eternus_pool)s, '
                  'Pool Type: %(pooltype)s.',
                  {'volumename': volumename,
                   'rc': rc,
                   'errordesc': errordesc,
                   'backend': systemnamelist[0]['IdentifyingNumber'],
                   'eternus_pool': eternus_pool,
                   'pooltype': POOL_TYPE_dic[pooltype]})

        # Create return value.
        element_path = {
            'classname': element.classname,
            'keybindings': {
                'CreationClassName': element['CreationClassName'],
                'SystemName': element['SystemName'],
                'DeviceID': element['DeviceID'],
                'SystemCreationClassName': element['SystemCreationClassName']
            }
        }

        volume_no = "0x" + element['DeviceID'][24:28]

        metadata = {'FJ_Backend': systemnamelist[0]['IdentifyingNumber'],
                    'FJ_Volume_Name': volumename,
                    'FJ_Volume_No': volume_no,
                    'FJ_Pool_Name': eternus_pool,
                    'FJ_Pool_Type': POOL_TYPE_dic[pooltype]}

        return (element_path, metadata)

    def create_volume_from_snapshot(self, volume, snapshot):
        """Creates a volume from a snapshot."""
        LOG.debug('create_volume_from_snapshot, '
                  'volume id: %(vid)s, volume size: %(vsize)s, '
                  'snapshot id: %(sid)s.',
                  {'vid': volume['id'], 'vsize': volume['size'],
                   'sid': snapshot['id']})

        self.conn = self._get_eternus_connection()
        source_volume_instance = self._find_lun(snapshot)

        # Check the existence of source volume.
        if source_volume_instance is None:
            msg = _('create_volume_from_snapshot, '
                    'Source Volume does not exist in ETERNUS.')
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        # Create volume for the target volume.
        (element_path, metadata) = self.create_volume(volume)
        target_volume_instancename = self._create_eternus_instance_name(
            element_path['classname'], element_path['keybindings'])

        try:
            target_volume_instance = (
                self._get_eternus_instance(target_volume_instancename))
        except Exception:
            msg = (_('create_volume_from_snapshot, '
                     'target volume instancename: %(volume_instancename)s, '
                     'Get Instance Failed.')
                   % {'volume_instancename': target_volume_instancename})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        self._create_local_cloned_volume(target_volume_instance,
                                         source_volume_instance)

        return (element_path, metadata)

    def create_cloned_volume(self, volume, src_vref):
        """Create clone of the specified volume."""
        LOG.debug('create_cloned_volume, '
                  'tgt: (%(tid)s, %(tsize)s), src: (%(sid)s, %(ssize)s).',
                  {'tid': volume['id'], 'tsize': volume['size'],
                   'sid': src_vref['id'], 'ssize': src_vref['size']})

        self.conn = self._get_eternus_connection()
        source_volume_instance = self._find_lun(src_vref)

        if source_volume_instance is None:
            msg = _('create_cloned_volume, '
                    'Source Volume does not exist in ETERNUS.')
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        (element_path, metadata) = self.create_volume(volume)
        target_volume_instancename = self._create_eternus_instance_name(
            element_path['classname'], element_path['keybindings'])

        try:
            target_volume_instance = (
                self._get_eternus_instance(target_volume_instancename))
        except Exception:
            msg = (_('create_cloned_volume, '
                     'target volume instancename: %(volume_instancename)s, '
                     'Get Instance Failed.')
                   % {'volume_instancename': target_volume_instancename})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        self._create_local_cloned_volume(target_volume_instance,
                                         source_volume_instance)

        return (element_path, metadata)

    @lockutils.synchronized('ETERNUS-vol', 'cinder-', True)
    def _create_local_cloned_volume(self, tgt_vol_instance, src_vol_instance):
        """Create local clone of the specified volume."""
        s_volumename = src_vol_instance['ElementName']
        t_volumename = tgt_vol_instance['ElementName']

        LOG.debug('_create_local_cloned_volume, '
                  'tgt volume name: %(t_volumename)s, '
                  'src volume name: %(s_volumename)s, ',
                  {'t_volumename': t_volumename,
                   's_volumename': s_volumename})

        # Get replicationservice for CreateElementReplica.
        repservice = self._find_eternus_service(REPL)

        if repservice is None:
            msg = _('_create_local_cloned_volume, '
                    'Replication Service not found.')
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        # Invoke method for create cloned volume from volume.
        rc, errordesc, job = self._exec_eternus_service(
            'CreateElementReplica',
            repservice,
            SyncType=self._pywbem_uint(8, '16'),
            SourceElement=src_vol_instance.path,
            TargetElement=tgt_vol_instance.path)

        if rc != 0:
            msg = (_('_create_local_cloned_volume, '
                     'volumename: %(volumename)s, '
                     'sourcevolumename: %(sourcevolumename)s, '
                     'source volume instance: %(source_volume)s, '
                     'target volume instance: %(target_volume)s, '
                     'Return code: %(rc)lu, '
                     'Error: %(errordesc)s.')
                   % {'volumename': t_volumename,
                      'sourcevolumename': s_volumename,
                      'source_volume': src_vol_instance.path,
                      'target_volume': tgt_vol_instance.path,
                      'rc': rc,
                      'errordesc': errordesc})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        LOG.debug('_create_local_cloned_volume, out: %(rc)s, %(job)s.',
                  {'rc': rc, 'job': job})

    def delete_volume(self, volume):
        """Delete volume on ETERNUS."""
        LOG.debug('delete_volume, volume id: %s.', volume['id'])

        self.conn = self._get_eternus_connection()
        vol_exist = self._delete_volume_setting(volume)

        if not vol_exist:
            LOG.debug('delete_volume, volume not found in 1st check.')
            return False

        # Check volume existence on ETERNUS again
        # because volume is deleted when SnapOPC copysession is deleted.
        vol_instance = self._find_lun(volume)
        if vol_instance is None:
            LOG.debug('delete_volume, volume not found in 2nd check, '
                      'but no problem.')
            return True

        self._delete_volume(vol_instance)
        return True

    @lockutils.synchronized('ETERNUS-vol', 'cinder-', True)
    def _delete_volume_setting(self, volume):
        """Delete volume setting (HostAffinity, CopySession) on ETERNUS."""
        LOG.debug('_delete_volume_setting, volume id: %s.', volume['id'])

        # Check the existence of volume.
        volumename = self._create_volume_name(volume['id'])
        vol_instance = self._find_lun(volume)

        if vol_instance is None:
            LOG.info('_delete_volume_setting, volumename:%(volumename)s, '
                     'volume not found on ETERNUS.',
                     {'volumename': volumename})
            return False

        # Delete host-affinity setting remained by unexpected error.
        self._unmap_lun(volume, None, force=True)

        # Check copy session relating to target volume.
        cpsessionlist = self._find_copysession(vol_instance)
        delete_copysession_list = []
        wait_copysession_list = []

        for cpsession in cpsessionlist:
            LOG.debug('_delete_volume_setting, '
                      'volumename: %(volumename)s, '
                      'cpsession: %(cpsession)s.',
                      {'volumename': volumename,
                       'cpsession': cpsession})

            if cpsession['SyncedElement'] == vol_instance.path:
                # Copy target : other_volume --(copy)--> vol_instance
                delete_copysession_list.append(cpsession)
            elif cpsession['SystemElement'] == vol_instance.path:
                # Copy source : vol_instance --(copy)--> other volume
                wait_copysession_list.append(cpsession)

        LOG.debug('_delete_volume_setting, '
                  'wait_cpsession: %(wait_cpsession)s, '
                  'delete_cpsession: %(delete_cpsession)s.',
                  {'wait_cpsession': wait_copysession_list,
                   'delete_cpsession': delete_copysession_list})

        for cpsession in wait_copysession_list:
            self._wait_for_copy_complete(cpsession)

        for cpsession in delete_copysession_list:
            self._delete_copysession(cpsession)

        LOG.debug('_delete_volume_setting, '
                  'wait_cpsession: %(wait_cpsession)s, '
                  'delete_cpsession: %(delete_cpsession)s, complete.',
                  {'wait_cpsession': wait_copysession_list,
                   'delete_cpsession': delete_copysession_list})
        return True

    @lockutils.synchronized('ETERNUS-vol', 'cinder-', True)
    def _delete_volume(self, vol_instance):
        """Delete volume on ETERNUS."""
        LOG.debug('_delete_volume, volume name: %s.',
                  vol_instance['ElementName'])

        volumename = vol_instance['ElementName']

        configservice = self._find_eternus_service(STOR_CONF)
        if configservice is None:
            msg = (_('_delete_volume, volumename: %(volumename)s, '
                     'Storage Configuration Service not found.')
                   % {'volumename': volumename})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        LOG.debug('_delete_volume, volumename: %(volumename)s, '
                  'vol_instance: %(vol_instance)s, '
                  'Method: ReturnToStoragePool.',
                  {'volumename': volumename,
                   'vol_instance': vol_instance.path})

        # Invoke method for delete volume
        rc, errordesc, job = self._exec_eternus_service(
            'ReturnToStoragePool',
            configservice,
            TheElement=vol_instance.path)

        if rc != 0:
            msg = (_('_delete_volume, volumename: %(volumename)s, '
                     'Return code: %(rc)lu, '
                     'Error: %(errordesc)s.')
                   % {'volumename': volumename,
                      'rc': rc,
                      'errordesc': errordesc})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        LOG.debug('_delete_volume, volumename: %(volumename)s, '
                  'Return code: %(rc)lu, '
                  'Error: %(errordesc)s.',
                  {'volumename': volumename,
                   'rc': rc,
                   'errordesc': errordesc})

    @lockutils.synchronized('ETERNUS-vol', 'cinder-', True)
    def create_snapshot(self, snapshot):
        """Create snapshot using SnapOPC."""
        LOG.debug('create_snapshot, '
                  'snapshot id: %(sid)s, volume id: %(vid)s.',
                  {'sid': snapshot['id'], 'vid': snapshot['volume_id']})

        self.conn = self._get_eternus_connection()
        snapshotname = snapshot['name']
        volumename = snapshot['volume_name']
        vol_id = snapshot['volume_id']
        volume = snapshot['volume']
        d_volumename = self._create_volume_name(snapshot['id'])
        s_volumename = self._create_volume_name(vol_id)
        vol_instance = self._find_lun(volume)
        repservice = self._find_eternus_service(REPL)

        # Check the existence of volume.
        if vol_instance is None:
            # Volume not found on ETERNUS.
            msg = (_('create_snapshot, '
                     'volumename: %(s_volumename)s, '
                     'source volume not found on ETERNUS.')
                   % {'s_volumename': s_volumename})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        if repservice is None:
            msg = (_('create_snapshot, '
                     'volumename: %(volumename)s, '
                     'Replication Service not found.')
                   % {'volumename': volumename})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        # Get poolname from driver configuration file.
        eternus_pool = self._get_drvcfg('EternusSnapPool')
        # Check the existence of pool
        pool = self._find_pool(eternus_pool)
        if pool is None:
            msg = (_('create_snapshot, '
                     'eternus_pool: %(eternus_pool)s, '
                     'pool not found.')
                   % {'eternus_pool': eternus_pool})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        LOG.debug('create_snapshot, '
                  'snapshotname: %(snapshotname)s, '
                  'source volume name: %(volumename)s, '
                  'vol_instance.path: %(vol_instance)s, '
                  'dest_volumename: %(d_volumename)s, '
                  'pool: %(pool)s, '
                  'Invoke CreateElementReplica.',
                  {'snapshotname': snapshotname,
                   'volumename': volumename,
                   'vol_instance': vol_instance.path,
                   'd_volumename': d_volumename,
                   'pool': pool})

        # Invoke method for create snapshot
        rc, errordesc, job = self._exec_eternus_service(
            'CreateElementReplica',
            repservice,
            ElementName=d_volumename,
            TargetPool=pool,
            SyncType=self._pywbem_uint(7, '16'),
            SourceElement=vol_instance.path)

        if rc != 0:
            msg = (_('create_snapshot, '
                     'snapshotname: %(snapshotname)s, '
                     'source volume name: %(volumename)s, '
                     'vol_instance.path: %(vol_instance)s, '
                     'dest volume name: %(d_volumename)s, '
                     'pool: %(pool)s, '
                     'Return code: %(rc)lu, '
                     'Error: %(errordesc)s.')
                   % {'snapshotname': snapshotname,
                      'volumename': volumename,
                      'vol_instance': vol_instance.path,
                      'd_volumename': d_volumename,
                      'pool': pool,
                      'rc': rc,
                      'errordesc': errordesc})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        else:
            element = job['TargetElement']

        LOG.debug('create_snapshot, '
                  'volumename:%(volumename)s, '
                  'Return code:%(rc)lu, '
                  'Error:%(errordesc)s.',
                  {'volumename': volumename,
                   'rc': rc,
                   'errordesc': errordesc})

        # Create return value.
        element_path = {
            'classname': element.classname,
            'keybindings': {
                'CreationClassName': element['CreationClassName'],
                'SystemName': element['SystemName'],
                'DeviceID': element['DeviceID'],
                'SystemCreationClassName': element['SystemCreationClassName']
            }
        }

        sdv_no = "0x" + element['DeviceID'][24:28]
        metadata = {'FJ_SDV_Name': d_volumename,
                    'FJ_SDV_No': sdv_no,
                    'FJ_Pool_Name': eternus_pool}
        return (element_path, metadata)

    def delete_snapshot(self, snapshot):
        """Delete snapshot."""
        LOG.debug('delete_snapshot, '
                  'snapshot id: %(sid)s, volume id: %(vid)s.',
                  {'sid': snapshot['id'], 'vid': snapshot['volume_id']})

        vol_exist = self.delete_volume(snapshot)
        LOG.debug('delete_snapshot, vol_exist: %s.', vol_exist)
        return vol_exist

    def initialize_connection(self, volume, connector):
        """Allow connection to connector and return connection info."""
        LOG.debug('initialize_connection, '
                  'volume id: %(vid)s, protocol: %(prtcl)s.',
                  {'vid': volume['id'], 'prtcl': self.protocol})

        self.conn = self._get_eternus_connection()
        vol_instance = self._find_lun(volume)
        # Check the existence of volume
        if vol_instance is None:
            # Volume not found
            msg = (_('initialize_connection, '
                     'volume: %(volume)s, '
                     'Volume not found.')
                   % {'volume': volume['name']})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        target_portlist = self._get_target_port()
        mapdata = self._get_mapdata(vol_instance, connector, target_portlist)

        if mapdata:
            # volume is already mapped
            target_lun = mapdata.get('target_lun', None)
            target_luns = mapdata.get('target_luns', None)

            LOG.info('initialize_connection, '
                     'volume: %(volume)s, '
                     'target_lun: %(target_lun)s, '
                     'target_luns: %(target_luns)s, '
                     'Volume is already mapped.',
                     {'volume': volume['name'],
                      'target_lun': target_lun,
                      'target_luns': target_luns})
        else:
            self._map_lun(vol_instance, connector, target_portlist)
            mapdata = self._get_mapdata(vol_instance,
                                        connector, target_portlist)

        mapdata['target_discovered'] = True
        mapdata['volume_id'] = volume['id']

        if self.protocol == 'fc':
            device_info = {'driver_volume_type': 'fibre_channel',
                           'data': mapdata}
        elif self.protocol == 'iSCSI':
            device_info = {'driver_volume_type': 'iscsi',
                           'data': mapdata}

        LOG.debug('initialize_connection, '
                  'device_info:%(info)s.',
                  {'info': device_info})
        return device_info

    def terminate_connection(self, volume, connector, force=False, **kwargs):
        """Disallow connection from connector."""
        LOG.debug('terminate_connection, '
                  'volume id: %(vid)s, protocol: %(prtcl)s, force: %(frc)s.',
                  {'vid': volume['id'], 'prtcl': self.protocol, 'frc': force})

        self.conn = self._get_eternus_connection()
        force = True if not connector else force
        map_exist = self._unmap_lun(volume, connector, force)

        LOG.debug('terminate_connection, map_exist: %s.', map_exist)
        return map_exist

    def build_fc_init_tgt_map(self, connector, target_wwn=None):
        """Build parameter for Zone Manager"""
        LOG.debug('build_fc_init_tgt_map, target_wwn: %s.', target_wwn)

        initiatorlist = self._find_initiator_names(connector)

        if target_wwn is None:
            target_wwn = []
            target_portlist = self._get_target_port()
            for target_port in target_portlist:
                target_wwn.append(target_port['Name'])

        init_tgt_map = {initiator: target_wwn for initiator in initiatorlist}

        LOG.debug('build_fc_init_tgt_map, '
                  'initiator target mapping: %s.', init_tgt_map)
        return init_tgt_map

    def check_attached_volume_in_zone(self, connector):
        """Check Attached Volume in Same FC Zone or not"""
        LOG.debug('check_attached_volume_in_zone, connector: %s.', connector)

        aglist = self._find_affinity_group(connector)
        if not aglist:
            attached = False
        else:
            attached = True

        LOG.debug('check_attached_volume_in_zone, attached: %s.', attached)
        return attached

    @lockutils.synchronized('ETERNUS-vol', 'cinder-', True)
    def extend_volume(self, volume, new_size):
        """Extend volume on ETERNUS."""
        LOG.debug('extend_volume, volume id: %(vid)s, '
                  'size: %(size)s, new_size: %(nsize)s.',
                  {'vid': volume['id'],
                   'size': volume['size'], 'nsize': new_size})

        self.conn = self._get_eternus_connection()
        volumesize = new_size * units.Gi
        volumename = self._create_volume_name(volume['id'])

        # Get source volume instance.
        vol_instance = self._find_lun(volume)
        if vol_instance is None:
            msg = (_('extend_volume, '
                     'volumename: %(volumename)s, '
                     'volume not found.')
                   % {'volumename': volumename})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        LOG.debug('extend_volume, volumename: %(volumename)s, '
                  'volumesize: %(volumesize)u, '
                  'volume instance: %(vol_instance)s.',
                  {'volumename': volumename,
                   'volumesize': volumesize,
                   'vol_instance': vol_instance.path})

        # Get poolname from driver configuration file.
        eternus_pool = self._get_drvcfg('EternusPool')
        # Check the existence of volume.
        pool = self._find_pool(eternus_pool)
        if pool is None:
            msg = (_('extend_volume, '
                     'eternus_pool: %(eternus_pool)s, '
                     'pool not found.')
                   % {'eternus_pool': eternus_pool})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        # Set pooltype.
        if 'RSP' in pool['InstanceID']:
            pooltype = RAIDGROUP
        else:
            pooltype = TPPOOL

        configservice = self._find_eternus_service(STOR_CONF)
        if configservice is None:
            msg = (_('extend_volume, volume: %(volume)s, '
                     'volumename: %(volumename)s, '
                     'eternus_pool: %(eternus_pool)s, '
                     'Storage Configuration Service not found.')
                   % {'volume': volume,
                      'volumename': volumename,
                      'eternus_pool': eternus_pool})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        LOG.debug('extend_volume, '
                  'CreateOrModifyElementFromStoragePool, '
                  'ConfigService: %(service)s, '
                  'ElementName: %(volumename)s, '
                  'InPool: %(eternus_pool)s, '
                  'ElementType: %(pooltype)u, '
                  'Size: %(volumesize)u, '
                  'TheElement: %(vol_instance)s.',
                  {'service': configservice,
                   'volumename': volumename,
                   'eternus_pool': eternus_pool,
                   'pooltype': pooltype,
                   'volumesize': volumesize,
                   'vol_instance': vol_instance.path})

        # Invoke method for extend volume
        rc, errordesc, job = self._exec_eternus_service(
            'CreateOrModifyElementFromStoragePool',
            configservice,
            ElementName=volumename,
            InPool=pool,
            ElementType=self._pywbem_uint(pooltype, '16'),
            Size=self._pywbem_uint(volumesize, '64'),
            TheElement=vol_instance.path)

        if rc != 0:
            msg = (_('extend_volume, '
                     'volumename: %(volumename)s, '
                     'Return code: %(rc)lu, '
                     'Error: %(errordesc)s, '
                     'PoolType: %(pooltype)s.')
                   % {'volumename': volumename,
                      'rc': rc,
                      'errordesc': errordesc,
                      'pooltype': POOL_TYPE_dic[pooltype]})

            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        LOG.debug('extend_volume, '
                  'volumename: %(volumename)s, '
                  'Return code: %(rc)lu, '
                  'Error: %(errordesc)s, '
                  'Pool Name: %(eternus_pool)s, '
                  'Pool Type: %(pooltype)s.',
                  {'volumename': volumename,
                   'rc': rc,
                   'errordesc': errordesc,
                   'eternus_pool': eternus_pool,
                   'pooltype': POOL_TYPE_dic[pooltype]})

        return eternus_pool

    @lockutils.synchronized('ETERNUS-update', 'cinder-', True)
    def update_volume_stats(self):
        """get pool capacity."""

        self.conn = self._get_eternus_connection()
        eternus_pool = self._get_drvcfg('EternusPool')

        LOG.debug('update_volume_stats, pool name: %s.', eternus_pool)

        pool = self._find_pool(eternus_pool, True)
        if pool:
            # pool is found
            self.stats['total_capacity_gb'] = (
                pool['TotalManagedSpace'] / units.Gi)

            self.stats['free_capacity_gb'] = (
                pool['RemainingManagedSpace'] / units.Gi)
        else:
            # if pool information is unknown, set 0 GB to capacity information
            LOG.warning('update_volume_stats, '
                        'eternus_pool:%(eternus_pool)s, '
                        'specified pool is not found.',
                        {'eternus_pool': eternus_pool})
            self.stats['total_capacity_gb'] = 0
            self.stats['free_capacity_gb'] = 0

        self.stats['multiattach'] = False

        LOG.debug('update_volume_stats, '
                  'eternus_pool:%(eternus_pool)s, '
                  'total capacity[%(total)s], '
                  'free capacity[%(free)s].',
                  {'eternus_pool': eternus_pool,
                   'total': self.stats['total_capacity_gb'],
                   'free': self.stats['free_capacity_gb']})

        return (self.stats, eternus_pool)

    def _get_mapdata(self, vol_instance, connector, target_portlist):
        """return mapping information."""
        mapdata = None
        multipath = connector.get('multipath', False)

        LOG.debug('_get_mapdata, volume name: %(vname)s, '
                  'protocol: %(prtcl)s, multipath: %(mpath)s.',
                  {'vname': vol_instance['ElementName'],
                   'prtcl': self.protocol, 'mpath': multipath})

        # find affinity group
        # attach the connector and include the volume
        aglist = self._find_affinity_group(connector, vol_instance)
        if not aglist:
            LOG.debug('_get_mapdata, ag_list:%s.', aglist)
        else:
            if self.protocol == 'fc':
                mapdata = self._get_mapdata_fc(aglist, vol_instance,
                                               target_portlist)
            elif self.protocol == 'iSCSI':
                mapdata = self._get_mapdata_iscsi(aglist, vol_instance,
                                                  multipath)

        LOG.debug('_get_mapdata, mapdata: %s.', mapdata)
        return mapdata

    def _get_mapdata_fc(self, aglist, vol_instance, target_portlist):
        """_get_mapdata for FibreChannel."""
        target_wwn = []

        try:
            ag_volmaplist = self._reference_eternus_names(
                aglist[0],
                ResultClass='CIM_ProtocolControllerForUnit')
            vo_volmaplist = self._reference_eternus_names(
                vol_instance.path,
                ResultClass='CIM_ProtocolControllerForUnit')
        except pywbem.CIM_Error:
            msg = (_('_get_mapdata_fc, '
                     'getting host-affinity from aglist/vol_instance failed, '
                     'affinitygroup: %(ag)s, '
                     'ReferenceNames, '
                     'cannot connect to ETERNUS.')
                   % {'ag': aglist[0]})
            LOG.exception(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        volmap = None
        for vo_volmap in vo_volmaplist:
            if vo_volmap in ag_volmaplist:
                volmap = vo_volmap
                break

        try:
            volmapinstance = self._get_eternus_instance(
                volmap,
                LocalOnly=False)
        except pywbem.CIM_Error:
            msg = (_('_get_mapdata_fc, '
                     'getting host-affinity instance failed, '
                     'volmap: %(volmap)s, '
                     'GetInstance, '
                     'cannot connect to ETERNUS.')
                   % {'volmap': volmap})
            LOG.exception(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        target_lun = int(volmapinstance['DeviceNumber'], 16)

        for target_port in target_portlist:
            target_wwn.append(target_port['Name'])

        mapdata = {'target_wwn': target_wwn,
                   'target_lun': target_lun}
        LOG.debug('_get_mapdata_fc, mapdata: %s.', mapdata)
        return mapdata

    def _get_mapdata_iscsi(self, aglist, vol_instance, multipath):
        """_get_mapdata for iSCSI."""
        target_portals = []
        target_iqns = []
        target_luns = []

        try:
            vo_volmaplist = self._reference_eternus_names(
                vol_instance.path,
                ResultClass='CIM_ProtocolControllerForUnit')
        except Exception:
            msg = (_('_get_mapdata_iscsi, '
                     'vol_instance: %(vol_instance)s, '
                     'ReferenceNames: CIM_ProtocolControllerForUnit, '
                     'cannot connect to ETERNUS.')
                   % {'vol_instance': vol_instance})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        target_properties_list = self._get_eternus_iscsi_properties()
        target_list = [prop[0] for prop in target_properties_list]
        properties_list = (
            [(prop[1], prop[2]) for prop in target_properties_list])

        for ag in aglist:
            try:
                iscsi_endpointlist = (
                    self._assoc_eternus_names(
                        ag,
                        AssocClass='FUJITSU_SAPAvailableForElement',
                        ResultClass='FUJITSU_iSCSIProtocolEndpoint'))
            except Exception:
                msg = (_('_get_mapdata_iscsi, '
                         'Associators: FUJITSU_SAPAvailableForElement, '
                         'cannot connect to ETERNUS.'))
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

            iscsi_endpoint = iscsi_endpointlist[0]
            if iscsi_endpoint not in target_list:
                continue

            idx = target_list.index(iscsi_endpoint)
            target_portal, target_iqn = properties_list[idx]

            try:
                ag_volmaplist = self._reference_eternus_names(
                    ag,
                    ResultClass='CIM_ProtocolControllerForUnit')
            except Exception:
                msg = (_('_get_mapdata_iscsi, '
                         'affinitygroup: %(ag)s, '
                         'ReferenceNames, '
                         'cannot connect to ETERNUS.')
                       % {'ag': ag})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

            volmap = None
            for vo_volmap in vo_volmaplist:
                if vo_volmap in ag_volmaplist:
                    volmap = vo_volmap
                    break

            if volmap is None:
                continue

            try:
                volmapinstance = self._get_eternus_instance(
                    volmap,
                    LocalOnly=False)
            except Exception:
                msg = (_('_get_mapdata_iscsi, '
                         'volmap: %(volmap)s, '
                         'GetInstance, '
                         'cannot connect to ETERNUS.')
                       % {'volmap': volmap})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

            target_lun = int(volmapinstance['DeviceNumber'], 16)

            target_portals.append(target_portal)
            target_iqns.append(target_iqn)
            target_luns.append(target_lun)

        if multipath:
            mapdata = {'target_portals': target_portals,
                       'target_iqns': target_iqns,
                       'target_luns': target_luns}
        else:
            mapdata = {'target_portal': target_portals[0],
                       'target_iqn': target_iqns[0],
                       'target_lun': target_luns[0]}

        LOG.debug('_get_mapdata_iscsi, mapdata: %s.', mapdata)
        return mapdata

    def _get_drvcfg(self, tagname, filename=None, multiple=False):
        """read from driver configuration file."""
        if filename is None:
            # set default configuration file name
            filename = self.configuration.cinder_eternus_config_file

        LOG.debug("_get_drvcfg, input[%(filename)s][%(tagname)s].",
                  {'filename': filename, 'tagname': tagname})

        tree = ET.parse(filename)
        elem = tree.getroot()

        ret = None
        if not multiple:
            ret = elem.findtext(".//" + tagname)
        else:
            ret = []
            for e in elem.findall(".//" + tagname):
                if (e.text is not None) and (e.text not in ret):
                    ret.append(e.text)

        if not ret:
            msg = (_('_get_drvcfg, '
                     'filename: %(filename)s, '
                     'tagname: %(tagname)s, '
                     'data is None!! '
                     'Please edit driver configuration file and correct.')
                   % {'filename': filename,
                      'tagname': tagname})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        return ret

    def _get_eternus_connection(self, filename=None):
        """return WBEM connection."""
        LOG.debug('_get_eternus_connection, filename: %s.', filename)

        ip = self._get_drvcfg('EternusIP', filename)
        port = self._get_drvcfg('EternusPort', filename)
        user = self._get_drvcfg('EternusUser', filename)
        passwd = self._get_drvcfg('EternusPassword', filename)
        url = 'http://' + ip + ':' + port

        conn = pywbem.WBEMConnection(url, (user, passwd),
                                     default_namespace='root/eternus')

        if conn is None:
            msg = (_('_get_eternus_connection, '
                     'filename: %(filename)s, '
                     'ip: %(ip)s, '
                     'port: %(port)s, '
                     'user: %(user)s, '
                     'passwd: ****, '
                     'url: %(url)s, '
                     'FAILED!!.')
                   % {'filename': filename,
                      'ip': ip,
                      'port': port,
                      'user': user,
                      'url': url})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        LOG.debug('_get_eternus_connection, conn: %s.', conn)
        return conn

    def _create_volume_name(self, id_code):
        """create volume_name on ETERNUS from id on OpenStack."""
        LOG.debug('_create_volume_name, id_code: %s.', id_code)

        if id_code is None:
            msg = _('_create_volume_name, id_code is None.')
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        m = hashlib.md5()
        m.update(id_code.encode('utf-8'))

        # pylint: disable=E1121
        volumename = base64.urlsafe_b64encode(m.digest()).decode()
        ret = VOL_PREFIX + six.text_type(volumename)

        LOG.debug('_create_volume_name, ret: %s', ret)
        return ret

    def _find_pool(self, eternus_pool, detail=False):
        """find Instance or InstanceName of pool by pool name on ETERNUS."""
        LOG.debug('_find_pool, pool name: %s.', eternus_pool)

        tppoollist = []
        rgpoollist = []

        # Get pools info form CIM instance(include info about instance path).
        try:
            tppoollist = self._enum_eternus_instances(
                'FUJITSU_ThinProvisioningPool')
            rgpoollist = self._enum_eternus_instances(
                'FUJITSU_RAIDStoragePool')
        except Exception:
            msg = (_('_find_pool, '
                     'eternus_pool:%(eternus_pool)s, '
                     'EnumerateInstances, '
                     'cannot connect to ETERNUS.')
                   % {'eternus_pool': eternus_pool})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        # Make total pools list.
        poollist = tppoollist + rgpoollist

        # One eternus backend has only one special pool name
        # so just use pool name can get the target pool.
        for pool in poollist:
            if pool['ElementName'] == eternus_pool:
                poolinstance = pool
                break
        else:
            poolinstance = None

        if poolinstance is None:
            ret = None
        elif detail is True:
            ret = poolinstance
        else:
            ret = poolinstance.path

        LOG.debug('_find_pool, pool: %s.', ret)
        return ret

    def _find_eternus_service(self, classname):
        """find CIM instance about service information."""
        LOG.debug('_find_eternus_service, '
                  'classname: %s.', classname)

        try:
            services = self._enum_eternus_instance_names(
                six.text_type(classname))
        except Exception:
            msg = (_('_find_eternus_service, '
                     'classname: %(classname)s, '
                     'EnumerateInstanceNames, '
                     'cannot connect to ETERNUS.')
                   % {'classname': classname})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        ret = services[0]
        LOG.debug('_find_eternus_service, '
                  'classname: %(classname)s, '
                  'ret: %(ret)s.',
                  {'classname': classname, 'ret': ret})
        return ret

    @lockutils.synchronized('ETERNUS-SMIS-exec', 'cinder-', True)
    @utils.retry(exception.VolumeBackendAPIException)
    def _exec_eternus_service(self, classname, instanceNameList, **param_dict):
        """Execute SMI-S Method."""
        LOG.debug('_exec_eternus_service, '
                  'classname: %(a)s, '
                  'instanceNameList: %(b)s, '
                  'parameters: %(c)s.',
                  {'a': classname,
                   'b': instanceNameList,
                   'c': param_dict})

        # Use InvokeMethod.
        try:
            rc, retdata = self.conn.InvokeMethod(
                classname,
                instanceNameList,
                **param_dict)
        except Exception:
            if rc is None:
                msg = (_('_exec_eternus_service, '
                         'classname: %(classname)s, '
                         'InvokeMethod, '
                         'cannot connect to ETERNUS.')
                       % {'classname': classname})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

        # If the result has job information, wait for job complete
        if "Job" in retdata:
            rc = self._wait_for_job_complete(self.conn, retdata)

        if rc == DEVICE_IS_BUSY:
            msg = _('Device is in Busy state')
            raise exception.VolumeBackendAPIException(data=msg)

        errordesc = RETCODE_dic.get(six.text_type(rc), UNDEF_MSG)

        ret = (rc, errordesc, retdata)

        LOG.debug('_exec_eternus_service, '
                  'classname: %(a)s, '
                  'instanceNameList: %(b)s, '
                  'parameters: %(c)s, '
                  'Return code: %(rc)s, '
                  'Error: %(errordesc)s, '
                  'Return data: %(retdata)s.',
                  {'a': classname,
                   'b': instanceNameList,
                   'c': param_dict,
                   'rc': rc,
                   'errordesc': errordesc,
                   'retdata': retdata})
        return ret

    @lockutils.synchronized('ETERNUS-SMIS-other', 'cinder-', True)
    @utils.retry(exception.VolumeBackendAPIException)
    def _enum_eternus_instances(self, classname):
        """Enumerate Instances."""
        LOG.debug('_enum_eternus_instances, classname: %s.', classname)

        ret = self.conn.EnumerateInstances(classname)

        LOG.debug('_enum_eternus_instances, enum %d instances.', len(ret))
        return ret

    @lockutils.synchronized('ETERNUS-SMIS-other', 'cinder-', True)
    @utils.retry(exception.VolumeBackendAPIException)
    def _enum_eternus_instance_names(self, classname):
        """Enumerate Instance Names."""
        LOG.debug('_enum_eternus_instance_names, classname: %s.', classname)

        ret = self.conn.EnumerateInstanceNames(classname)

        LOG.debug('_enum_eternus_instance_names, enum %d names.', len(ret))
        return ret

    @lockutils.synchronized('ETERNUS-SMIS-getinstance', 'cinder-', True)
    @utils.retry(exception.VolumeBackendAPIException)
    def _get_eternus_instance(self, classname, **param_dict):
        """Get Instance."""
        LOG.debug('_get_eternus_instance, '
                  'classname: %(cls)s, param: %(param)s.',
                  {'cls': classname, 'param': param_dict})

        ret = self.conn.GetInstance(classname, **param_dict)

        LOG.debug('_get_eternus_instance, ret: %s.', ret)
        return ret

    @lockutils.synchronized('ETERNUS-SMIS-other', 'cinder-', True)
    @utils.retry(exception.VolumeBackendAPIException)
    def _assoc_eternus(self, classname, **param_dict):
        """Associator."""
        LOG.debug('_assoc_eternus, '
                  'classname: %(cls)s, param: %(param)s.',
                  {'cls': classname, 'param': param_dict})

        ret = self.conn.Associators(classname, **param_dict)

        LOG.debug('_assoc_eternus, enum %d instances.', len(ret))
        return ret

    @lockutils.synchronized('ETERNUS-SMIS-other', 'cinder-', True)
    @utils.retry(exception.VolumeBackendAPIException)
    def _assoc_eternus_names(self, classname, **param_dict):
        """Associator Names."""
        LOG.debug('_assoc_eternus_names, '
                  'classname: %(cls)s, param: %(param)s.',
                  {'cls': classname, 'param': param_dict})

        ret = self.conn.AssociatorNames(classname, **param_dict)

        LOG.debug('_assoc_eternus_names, enum %d names.', len(ret))
        return ret

    @lockutils.synchronized('ETERNUS-SMIS-other', 'cinder-', True)
    @utils.retry(exception.VolumeBackendAPIException)
    def _reference_eternus_names(self, classname, **param_dict):
        """Refference Names."""
        LOG.debug('_reference_eternus_names, '
                  'classname: %(cls)s, param: %(param)s.',
                  {'cls': classname, 'param': param_dict})

        ret = self.conn.ReferenceNames(classname, **param_dict)

        LOG.debug('_reference_eternus_names, enum %d names.', len(ret))
        return ret

    def _create_eternus_instance_name(self, classname, bindings):
        """create CIM InstanceName from classname and bindings."""
        LOG.debug('_create_eternus_instance_name, '
                  'classname: %(cls)s, bindings: %(bind)s.',
                  {'cls': classname, 'bind': bindings})

        instancename = None

        try:
            instancename = pywbem.CIMInstanceName(
                classname,
                namespace='root/eternus',
                keybindings=bindings)
        except NameError:
            instancename = None

        LOG.debug('_create_eternus_instance_name, ret: %s.', instancename)
        return instancename

    def _find_lun(self, volume):
        """find lun instance from volume class or volumename on ETERNUS."""
        LOG.debug('_find_lun, volume id: %s.', volume['id'])
        volumeinstance = None
        volumename = self._create_volume_name(volume['id'])

        try:
            location = ast.literal_eval(volume['provider_location'])
            classname = location['classname']
            bindings = location['keybindings']

            if classname and bindings:
                LOG.debug('_find_lun, '
                          'classname: %(classname)s, '
                          'bindings: %(bindings)s.',
                          {'classname': classname,
                           'bindings': bindings})
                volume_instance_name = (
                    self._create_eternus_instance_name(classname, bindings))

                LOG.debug('_find_lun, '
                          'volume_insatnce_name: %(volume_instance_name)s.',
                          {'volume_instance_name': volume_instance_name})

                vol_instance = (
                    self._get_eternus_instance(volume_instance_name))

                if vol_instance['ElementName'] == volumename:
                    volumeinstance = vol_instance
        except Exception:
            volumeinstance = None
            LOG.debug('_find_lun, '
                      'Cannot get volume instance from provider location, '
                      'Search all volume using EnumerateInstanceNames.')

        if volumeinstance is None:
            # for old version

            LOG.debug('_find_lun, '
                      'volumename: %(volumename)s.',
                      {'volumename': volumename})

            # get volume instance from volumename on ETERNUS
            try:
                namelist = self._enum_eternus_instance_names(
                    'FUJITSU_StorageVolume')
            except Exception:
                msg = (_('_find_lun, '
                         'volumename: %(volumename)s, '
                         'EnumerateInstanceNames, '
                         'cannot connect to ETERNUS.')
                       % {'volumename': volumename})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

            for name in namelist:
                try:
                    vol_instance = self._get_eternus_instance(name)

                    if vol_instance['ElementName'] == volumename:
                        volumeinstance = vol_instance
                        path = volumeinstance.path

                        LOG.debug('_find_lun, '
                                  'volumename: %(volumename)s, '
                                  'vol_instance: %(vol_instance)s.',
                                  {'volumename': volumename,
                                   'vol_instance': path})
                        break
                except Exception:
                    continue
            else:
                LOG.debug('_find_lun, '
                          'volumename: %(volumename)s, '
                          'volume not found on ETERNUS.',
                          {'volumename': volumename})

        LOG.debug('_find_lun, ret: %s.', volumeinstance)
        return volumeinstance

    def _find_copysession(self, vol_instance):
        """find copysession from volumename on ETERNUS."""
        LOG.debug('_find_copysession, volume name: %s.',
                  vol_instance['ElementName'])

        try:
            cpsessionlist = self.conn.ReferenceNames(
                vol_instance.path,
                ResultClass='FUJITSU_StorageSynchronized')
        except Exception:
            msg = (_('_find_copysession, '
                     'ReferenceNames, '
                     'vol_instance: %(vol_instance_path)s, '
                     'Cannot connect to ETERNUS.')
                   % {'vol_instance_path': vol_instance.path})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        LOG.debug('_find_copysession, '
                  'cpsessionlist: %(cpsessionlist)s.',
                  {'cpsessionlist': cpsessionlist})

        LOG.debug('_find_copysession, ret: %s.', cpsessionlist)
        return cpsessionlist

    def _wait_for_copy_complete(self, cpsession):
        """Wait for the completion of copy."""
        LOG.debug('_wait_for_copy_complete, cpsession: %s.', cpsession)

        cpsession_instance = None

        while True:
            try:
                cpsession_instance = self.conn.GetInstance(
                    cpsession,
                    LocalOnly=False)
            except Exception:
                cpsession_instance = None

            # if copy session is none,
            # it means copy session was finished,break and return
            if cpsession_instance is None:
                break

            LOG.debug('_wait_for_copy_complete, '
                      'find target copysession, '
                      'wait for end of copysession.')

            if cpsession_instance['CopyState'] == BROKEN:
                msg = (_('_wait_for_copy_complete, '
                         'cpsession: %(cpsession)s, '
                         'copysession state is BROKEN.')
                       % {'cpsession': cpsession})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

            time.sleep(10)

    @utils.retry(exception.VolumeBackendAPIException)
    def _delete_copysession(self, cpsession):
        """delete copysession."""
        LOG.debug('_delete_copysession: cpssession: %s.', cpsession)

        try:
            cpsession_instance = self._get_eternus_instance(
                cpsession, LocalOnly=False)
        except Exception:
            LOG.info('_delete_copysession, '
                     'the copysession was already completed.')
            return

        copytype = cpsession_instance['CopyType']

        # set oparation code
        # SnapOPC: 19 (Return To ResourcePool)
        # OPC:8 (Detach)
        # EC/REC:8 (Detach)
        operation = OPERATION_dic.get(copytype, None)
        if operation is None:
            msg = (_('_delete_copysession, '
                     'copy session type is undefined! '
                     'copy session: %(cpsession)s, '
                     'copy type: %(copytype)s.')
                   % {'cpsession': cpsession,
                      'copytype': copytype})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        repservice = self._find_eternus_service(REPL)
        if repservice is None:
            msg = (_('_delete_copysession, '
                     'Cannot find Replication Service'))
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        # Invoke method for delete copysession
        rc, errordesc, job = self._exec_eternus_service(
            'ModifyReplicaSynchronization',
            repservice,
            Operation=self._pywbem_uint(operation, '16'),
            Synchronization=cpsession,
            Force=True,
            WaitForCopyState=self._pywbem_uint(15, '16'))

        LOG.debug('_delete_copysession, '
                  'copysession: %(cpsession)s, '
                  'operation: %(operation)s, '
                  'Return code: %(rc)lu, '
                  'Error: %(errordesc)s.',
                  {'cpsession': cpsession,
                   'operation': operation,
                   'rc': rc,
                   'errordesc': errordesc})

        if rc == COPYSESSION_NOT_EXIST:
            LOG.debug('_delete_copysession, '
                      'cpsession: %(cpsession)s, '
                      'copysession is not exist.',
                      {'cpsession': cpsession})
        elif rc == VOLUME_IS_BUSY:
            msg = (_('_delete_copysession, '
                     'copysession: %(cpsession)s, '
                     'operation: %(operation)s, '
                     'Error: Volume is in Busy state')
                   % {'cpsession': cpsession,
                      'operation': operation})
            raise exception.VolumeIsBusy(data=msg)
        elif rc != 0:
            msg = (_('_delete_copysession, '
                     'copysession: %(cpsession)s, '
                     'operation: %(operation)s, '
                     'Return code: %(rc)lu, '
                     'Error: %(errordesc)s.')
                   % {'cpsession': cpsession,
                      'operation': operation,
                      'rc': rc,
                      'errordesc': errordesc})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

    def _get_target_port(self):
        """return target portid."""
        LOG.debug('_get_target_port, protocol: %s.', self.protocol)

        target_portlist = []
        if self.protocol == 'fc':
            prtcl_endpoint = 'FUJITSU_SCSIProtocolEndpoint'
            connection_type = 2
        elif self.protocol == 'iSCSI':
            prtcl_endpoint = 'FUJITSU_iSCSIProtocolEndpoint'
            connection_type = 7

        try:
            tgtportlist = self._enum_eternus_instances(prtcl_endpoint)
        except Exception:
            msg = (_('_get_target_port, '
                     'EnumerateInstances, '
                     'cannot connect to ETERNUS.'))
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        for tgtport in tgtportlist:
            # Check : protocol of tgtport
            if tgtport['ConnectionType'] != connection_type:
                continue

            # Check : if port is for remote copy, continue
            if (tgtport['RAMode'] & 0x7B) != 0x00:
                continue

            # Check : if port is for StorageCluster, continue
            if 'SCGroupNo' in tgtport:
                continue

            target_portlist.append(tgtport)

            LOG.debug('_get_target_port, '
                      'connection type: %(cont)s, '
                      'ramode: %(ramode)s.',
                      {'cont': tgtport['ConnectionType'],
                       'ramode': tgtport['RAMode']})

        LOG.debug('_get_target_port, '
                  'target port: %(target_portid)s.',
                  {'target_portid': target_portlist})

        if len(target_portlist) == 0:
            msg = (_('_get_target_port, '
                     'protcol: %(protocol)s, '
                     'target_port not found.')
                   % {'protocol': self.protocol})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        LOG.debug('_get_target_port, ret: %s.', target_portlist)
        return target_portlist

    @lockutils.synchronized('ETERNUS-connect', 'cinder-', True)
    def _map_lun(self, vol_instance, connector, targetlist=None):
        """map volume to host."""
        volumename = vol_instance['ElementName']
        LOG.debug('_map_lun, '
                  'volume name: %(vname)s, connector: %(connector)s.',
                  {'vname': volumename, 'connector': connector})

        volume_uid = vol_instance['Name']
        initiatorlist = self._find_initiator_names(connector)
        aglist = self._find_affinity_group(connector)
        configservice = self._find_eternus_service(CTRL_CONF)

        if targetlist is None:
            targetlist = self._get_target_port()

        if configservice is None:
            msg = (_('_map_lun, '
                     'vol_instance.path:%(vol)s, '
                     'volumename: %(volumename)s, '
                     'volume_uid: %(uid)s, '
                     'initiator: %(initiator)s, '
                     'target: %(tgt)s, '
                     'aglist: %(aglist)s, '
                     'Storage Configuration Service not found.')
                   % {'vol': vol_instance.path,
                      'volumename': volumename,
                      'uid': volume_uid,
                      'initiator': initiatorlist,
                      'tgt': targetlist,
                      'aglist': aglist})

            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        LOG.debug('_map_lun, '
                  'vol_instance.path: %(vol_instance)s, '
                  'volumename:%(volumename)s, '
                  'initiator:%(initiator)s, '
                  'target:%(tgt)s.',
                  {'vol_instance': vol_instance.path,
                   'volumename': [volumename],
                   'initiator': initiatorlist,
                   'tgt': targetlist})

        if not aglist:
            # Create affinity group and set host-affinity.
            for target in targetlist:
                LOG.debug('_map_lun, '
                          'lun_name: %(volume_uid)s, '
                          'Initiator: %(initiator)s, '
                          'target: %(target)s.',
                          {'volume_uid': [volume_uid],
                           'initiator': initiatorlist,
                           'target': target['Name']})

                rc, errordesc, job = self._exec_eternus_service(
                    'ExposePaths',
                    configservice,
                    LUNames=[volume_uid],
                    InitiatorPortIDs=initiatorlist,
                    TargetPortIDs=[target['Name']],
                    DeviceAccesses=[self._pywbem_uint(2, '16')])

                LOG.debug('_map_lun, '
                          'Error: %(errordesc)s, '
                          'Return code: %(rc)lu, '
                          'Create affinitygroup and set host-affinity.',
                          {'errordesc': errordesc,
                           'rc': rc})

                if rc != 0 and rc != LUNAME_IN_USE:
                    LOG.warning('_map_lun, '
                                'lun_name: %(volume_uid)s, '
                                'Initiator: %(initiator)s, '
                                'target: %(target)s, '
                                'Return code: %(rc)lu, '
                                'Error: %(errordesc)s.',
                                {'volume_uid': [volume_uid],
                                 'initiator': initiatorlist,
                                 'target': target['Name'],
                                 'rc': rc,
                                 'errordesc': errordesc})
        else:
            # Add lun to affinity group
            for ag in aglist:
                LOG.debug('_map_lun, '
                          'ag: %(ag)s, lun_name: %(volume_uid)s.',
                          {'ag': ag,
                           'volume_uid': volume_uid})

                rc, errordesc, job = self._exec_eternus_service(
                    'ExposePaths',
                    configservice, LUNames=[volume_uid],
                    DeviceAccesses=[self._pywbem_uint(2, '16')],
                    ProtocolControllers=[ag])

                LOG.debug('_map_lun, '
                          'Error: %(errordesc)s, '
                          'Return code: %(rc)lu, '
                          'Add lun to affinity group.',
                          {'errordesc': errordesc,
                           'rc': rc})

                if rc != 0 and rc != LUNAME_IN_USE:
                    LOG.warning('_map_lun, '
                                'lun_name: %(volume_uid)s, '
                                'Initiator: %(initiator)s, '
                                'ag: %(ag)s, '
                                'Return code: %(rc)lu, '
                                'Error: %(errordesc)s.',
                                {'volume_uid': [volume_uid],
                                 'initiator': initiatorlist,
                                 'ag': ag,
                                 'rc': rc,
                                 'errordesc': errordesc})

    def _find_initiator_names(self, connector):
        """return initiator names."""

        initiatornamelist = []

        if self.protocol == 'fc' and connector['wwpns']:
            LOG.debug('_find_initiator_names, wwpns: %s.',
                      connector['wwpns'])
            initiatornamelist = connector['wwpns']
        elif self.protocol == 'iSCSI' and connector['initiator']:
            LOG.debug('_find_initiator_names, initiator: %s.',
                      connector['initiator'])
            initiatornamelist.append(connector['initiator'])

        if not initiatornamelist:
            msg = (_('_find_initiator_names, '
                     'connector: %(connector)s, '
                     'initiator not found.')
                   % {'connector': connector})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        LOG.debug('_find_initiator_names, '
                  'initiator list: %(initiator)s.',
                  {'initiator': initiatornamelist})

        return initiatornamelist

    def _find_affinity_group(self, connector, vol_instance=None):
        """find affinity group from connector."""
        LOG.debug('_find_affinity_group, vol_instance: %s.', vol_instance)

        affinity_grouplist = []
        initiatorlist = self._find_initiator_names(connector)

        if vol_instance is None:
            try:
                aglist = self._enum_eternus_instance_names(
                    'FUJITSU_AffinityGroupController')
            except Exception:
                msg = (_('_find_affinity_group, '
                         'connector: %(connector)s, '
                         'EnumerateInstanceNames, '
                         'cannot connect to ETERNUS.')
                       % {'connector': connector})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

            LOG.debug('_find_affinity_group,'
                      'affinity_groups:%s', aglist)
        else:
            try:
                aglist = self._assoc_eternus_names(
                    vol_instance.path,
                    AssocClass='FUJITSU_ProtocolControllerForUnit',
                    ResultClass='FUJITSU_AffinityGroupController')
            except Exception:
                msg = (_('_find_affinity_group,'
                         'connector: %(connector)s,'
                         'AssocNames: FUJITSU_ProtocolControllerForUnit, '
                         'cannot connect to ETERNUS.')
                       % {'connector': connector})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

            LOG.debug('_find_affinity_group, '
                      'vol_instance.path: %(volume)s, '
                      'affinity_groups: %(aglist)s.',
                      {'volume': vol_instance.path,
                       'aglist': aglist})

        for ag in aglist:
            try:
                hostaglist = self._assoc_eternus(
                    ag,
                    AssocClass='FUJITSU_AuthorizedTarget',
                    ResultClass='FUJITSU_AuthorizedPrivilege')
            except Exception:
                msg = (_('_find_affinity_group, '
                         'connector: %(connector)s, '
                         'Associators: FUJITSU_AuthorizedTarget, '
                         'cannot connect to ETERNUS.')
                       % {'connector': connector})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

            for hostag in hostaglist:
                for initiator in initiatorlist:
                    if initiator.lower() not in hostag['InstanceID'].lower():
                        continue

                    LOG.debug('_find_affinity_group, '
                              'AffinityGroup: %(ag)s.', {'ag': ag})
                    affinity_grouplist.append(ag)
                    break
                break

        LOG.debug('_find_affinity_group, '
                  'initiators: %(initiator)s, '
                  'affinity_group: %(affinity_group)s.',
                  {'initiator': initiatorlist,
                   'affinity_group': affinity_grouplist})
        return affinity_grouplist

    @lockutils.synchronized('ETERNUS-connect', 'cinder-', True)
    def _unmap_lun(self, volume, connector, force=False):
        """unmap volume from host."""
        LOG.debug('_map_lun, volume id: %(vid)s, '
                  'connector: %(connector)s, force: %(frc)s.',
                  {'vid': volume['id'],
                   'connector': connector, 'frc': force})

        volumename = self._create_volume_name(volume['id'])
        vol_instance = self._find_lun(volume)
        if vol_instance is None:
            LOG.info('_unmap_lun, '
                     'volumename:%(volumename)s, '
                     'volume not found.',
                     {'volumename': volumename})
            return False

        volume_uid = vol_instance['Name']

        if not force:
            aglist = self._find_affinity_group(connector, vol_instance)
            if not aglist:
                LOG.info('_unmap_lun, '
                         'volumename: %(volumename)s, '
                         'volume is not mapped.',
                         {'volumename': volumename})
                return False
        else:
            try:
                aglist = self._assoc_eternus_names(
                    vol_instance.path,
                    AssocClass='CIM_ProtocolControllerForUnit',
                    ResultClass='FUJITSU_AffinityGroupController')
            except Exception:
                msg = (_('_unmap_lun,'
                         'vol_instance.path: %(volume)s, '
                         'AssociatorNames: CIM_ProtocolControllerForUnit, '
                         'cannot connect to ETERNUS.')
                       % {'volume': vol_instance.path})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

            LOG.debug('_unmap_lun, '
                      'vol_instance.path: %(volume)s, '
                      'affinity_groups: %(aglist)s.',
                      {'volume': vol_instance.path,
                       'aglist': aglist})

        configservice = self._find_eternus_service(CTRL_CONF)
        if configservice is None:
            msg = (_('_unmap_lun, '
                     'vol_instance.path: %(volume)s, '
                     'volumename: %(volumename)s, '
                     'volume_uid: %(uid)s, '
                     'aglist: %(aglist)s, '
                     'Controller Configuration Service not found.')
                   % {'vol': vol_instance.path,
                      'volumename': [volumename],
                      'uid': [volume_uid],
                      'aglist': aglist})

            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        for ag in aglist:
            LOG.debug('_unmap_lun, '
                      'volumename: %(volumename)s, '
                      'volume_uid: %(volume_uid)s, '
                      'AffinityGroup: %(ag)s.',
                      {'volumename': volumename,
                       'volume_uid': volume_uid,
                       'ag': ag})

            rc, errordesc, job = self._exec_eternus_service(
                'HidePaths',
                configservice,
                LUNames=[volume_uid],
                ProtocolControllers=[ag])

            LOG.debug('_unmap_lun, '
                      'Error: %(errordesc)s, '
                      'Return code: %(rc)lu.',
                      {'errordesc': errordesc,
                       'rc': rc})

            if rc == LUNAME_NOT_EXIST:
                LOG.debug('_unmap_lun, '
                          'volumename: %(volumename)s, '
                          'Invalid LUNames.',
                          {'volumename': volumename})
            elif rc != 0:
                msg = (_('_unmap_lun, '
                         'volumename: %(volumename)s, '
                         'volume_uid: %(volume_uid)s, '
                         'AffinityGroup: %(ag)s, '
                         'Return code: %(rc)lu, '
                         'Error: %(errordesc)s.')
                       % {'volumename': volumename,
                          'volume_uid': volume_uid,
                          'ag': ag,
                          'rc': rc,
                          'errordesc': errordesc})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

        LOG.debug('_unmap_lun, '
                  'volumename: %(volumename)s.',
                  {'volumename': volumename})
        return True

    def _get_eternus_iscsi_properties(self):
        """get target port iqns and target_portals."""

        iscsi_properties_list = []
        iscsiip_list = self._get_drvcfg('EternusISCSIIP', multiple=True)
        iscsi_port = self.configuration.target_port

        LOG.debug('_get_eternus_iscsi_properties, iplist: %s.', iscsiip_list)

        try:
            ip_endpointlist = self._enum_eternus_instance_names(
                'FUJITSU_IPProtocolEndpoint')
        except Exception:
            msg = (_('_get_eternus_iscsi_properties, '
                     'iscsiip: %(iscsiip)s, '
                     'EnumerateInstanceNames, '
                     'cannot connect to ETERNUS.')
                   % {'iscsiip': iscsiip_list})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        for ip_endpoint in ip_endpointlist:
            try:
                ip_endpoint_instance = self._get_eternus_instance(
                    ip_endpoint)
                ip_address = ip_endpoint_instance['IPv4Address']
                LOG.debug('_get_eternus_iscsi_properties, '
                          'instanceip: %(ip)s, '
                          'iscsiip: %(iscsiip)s.',
                          {'ip': ip_address,
                           'iscsiip': iscsiip_list})
            except Exception:
                msg = (_('_get_eternus_iscsi_properties, '
                         'iscsiip: %(iscsiip)s, '
                         'GetInstance, '
                         'cannot connect to ETERNUS.')
                       % {'iscsiip': iscsiip_list})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

            if ip_address not in iscsiip_list:
                continue

            LOG.debug('_get_eternus_iscsi_properties, '
                      'find iscsiip: %(ip)s.', {'ip': ip_address})
            try:
                tcp_endpointlist = self._assoc_eternus_names(
                    ip_endpoint,
                    AssocClass='CIM_BindsTo',
                    ResultClass='FUJITSU_TCPProtocolEndpoint')
            except Exception:
                msg = (_('_get_eternus_iscsi_properties, '
                         'iscsiip: %(iscsiip)s, '
                         'AssociatorNames: CIM_BindsTo, '
                         'cannot connect to ETERNUS.')
                       % {'iscsiip': ip_address})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

            for tcp_endpoint in tcp_endpointlist:
                try:
                    iscsi_endpointlist = (
                        self._assoc_eternus(tcp_endpoint,
                                            AssocClass='CIM_BindsTo',
                                            ResultClass='FUJITSU_iSCSI'
                                            'ProtocolEndpoint'))
                except Exception:
                    msg = (_('_get_eternus_iscsi_properties, '
                             'iscsiip: %(iscsiip)s, '
                             'AssociatorNames: CIM_BindsTo, '
                             'cannot connect to ETERNUS.')
                           % {'iscsiip': ip_address})
                    LOG.error(msg)
                    raise exception.VolumeBackendAPIException(data=msg)

                for iscsi_endpoint in iscsi_endpointlist:
                    target_portal = "%s:%s" % (ip_address, iscsi_port)
                    iqn = iscsi_endpoint['Name'].split(',')[0]
                    iscsi_properties_list.append((iscsi_endpoint.path,
                                                  target_portal,
                                                  iqn))
                    LOG.debug('_get_eternus_iscsi_properties, '
                              'target_portal: %(target_portal)s, '
                              'iqn: %(iqn)s.',
                              {'target_portal': target_portal,
                               'iqn': iqn})

        if len(iscsi_properties_list) == 0:
            msg = (_('_get_eternus_iscsi_properties, '
                     'iscsiip list: %(iscsiip_list)s, '
                     'iqn not found.')
                   % {'iscsiip_list': iscsiip_list})

            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        LOG.debug('_get_eternus_iscsi_properties, '
                  'iscsi_properties_list: %(iscsi_properties_list)s.',
                  {'iscsi_properties_list': iscsi_properties_list})

        return iscsi_properties_list

    def _wait_for_job_complete(self, conn, job):
        """Given the job wait for it to complete."""
        self.retries = 0
        self.wait_for_job_called = False

        def _wait_for_job_complete():
            """Called at an interval until the job is finished."""
            if self._is_job_finished(conn, job):
                raise loopingcall.LoopingCallDone()
            if self.retries > JOB_RETRIES:
                LOG.error("_wait_for_job_complete, "
                          "failed after %(retries)d tries.",
                          {'retries': self.retries})
                raise loopingcall.LoopingCallDone()

            try:
                self.retries += 1
                if not self.wait_for_job_called:
                    if self._is_job_finished(conn, job):
                        self.wait_for_job_called = True
            except Exception:
                exceptionMessage = _("Issue encountered waiting for job.")
                LOG.exception(exceptionMessage)
                raise exception.VolumeBackendAPIException(exceptionMessage)

        self.wait_for_job_called = False
        timer = loopingcall.FixedIntervalLoopingCall(_wait_for_job_complete)
        timer.start(interval=JOB_INTERVAL_SEC).wait()

        jobInstanceName = job['Job']
        jobinstance = conn.GetInstance(jobInstanceName,
                                       LocalOnly=False)

        rc = jobinstance['ErrorCode']

        LOG.debug('_wait_for_job_complete, rc: %s.', rc)
        return rc

    def _is_job_finished(self, conn, job):
        """Check if the job is finished."""
        jobInstanceName = job['Job']
        jobinstance = conn.GetInstance(jobInstanceName,
                                       LocalOnly=False)
        jobstate = jobinstance['JobState']
        LOG.debug('_is_job_finished,'
                  'state: %(state)s', {'state': jobstate})
        # From ValueMap of JobState in CIM_ConcreteJob
        # 2=New, 3=Starting, 4=Running, 32767=Queue Pending
        # ValueMap("2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13..32767,
        # 32768..65535"),
        # Values("New, Starting, Running, Suspended, Shutting Down,
        # Completed, Terminated, Killed, Exception, Service,
        # Query Pending, DMTF Reserved, Vendor Reserved")]
        # NOTE(deva): string matching based on
        #             http://ipmitool.cvs.sourceforge.net/
        #               viewvc/ipmitool/ipmitool/lib/ipmi_chassis.c

        if jobstate in [2, 3, 4]:
            job_finished = False
        else:
            job_finished = True

        LOG.debug('_is_job_finished, finish: %s.', job_finished)
        return job_finished

    def _pywbem_uint(self, num, datatype):
        try:
            result = {
                '8': pywbem.Uint8(num),
                '16': pywbem.Uint16(num),
                '32': pywbem.Uint32(num),
                '64': pywbem.Uint64(num)
            }
            result = result.get(datatype, num)
        except NameError:
            result = num

        return result
