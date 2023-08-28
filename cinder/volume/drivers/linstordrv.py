#  Copyright (c) 2014-2019 LINBIT HA Solutions GmbH
#  All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License"); you may
#  not use this file except in compliance with the License. You may obtain
#  a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#  License for the specific language governing permissions and limitations
#  under the License.

"""This driver connects Cinder to an installed LINSTOR instance.

See https://docs.linbit.com/docs/users-guide-9.0/#ch-openstack-linstor
for more details.
"""
import contextlib
import functools
import socket

from eventlet.green import threading
from oslo_config import cfg
from oslo_log import log as logging
from oslo_utils import importutils
from oslo_utils import units

from cinder.common import constants
from cinder import exception
from cinder.i18n import _
from cinder import interface
from cinder.volume import configuration
from cinder.volume import driver
from cinder.volume.targets import driver as targets
from cinder.volume import volume_utils

try:
    import linstor
except ImportError:
    linstor = None

# To override these values, update cinder.conf in /etc/cinder/
linstor_opts = [
    cfg.ListOpt('linstor_uris',
                default=['linstor://localhost'],
                deprecated_name='linstor_default_uri',
                help='URI(s) of Linstor controller to connect to. Specify '
                     'multiple URIs to take advantage of a LINSTOR HA'
                     'deployment.'),

    cfg.StrOpt('linstor_client_key',
               help='Path to the PEM encoded private key used for HTTPS '
                    'connections to the server'),

    cfg.StrOpt('linstor_client_cert',
               help='Path to the PEM encoded client certificate to present '
                    'the controller on HTTPS connections'),

    cfg.StrOpt('linstor_trusted_ca',
               help='Path to the PEM encoded CA certificate, used to verify '
                    'the LINSTOR controller authenticity'),

    cfg.StrOpt('linstor_default_storage_pool_name',
               help='Default LINSTOR Storage Pool to use.'),

    cfg.StrOpt('linstor_default_resource_group_name',
               help='Resource Group to use when no volume type was provided',
               default="DfltRscGrp"),

    cfg.BoolOpt('linstor_direct',
                default=False,
                help='True, if the volume should be directly attached on the'
                     'target. Requires the target to be part of the Linstor '
                     'cluster. False, if the volume should be attached via '
                     'one of the transports included in Cinder (i.e. ISCSI).'),
    cfg.IntOpt('linstor_timeout',
               default=60,
               help='How long to wait for a response from the Linstor API'),

    cfg.BoolOpt('linstor_force_udev',
                default=True,
                help='True, if the driver should assume udev created links'
                     'always exist.')
]

LOG = logging.getLogger(__name__)  # type: logging.logging.Logger

CONF = cfg.CONF
CONF.register_opts(linstor_opts, group=configuration.SHARED_CONF_GROUP)


def wrap_linstor_api_exception(func):
    """Wrap Linstor exceptions in LinstorDriverApiExceptions"""
    @functools.wraps(func)
    def f(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except linstor.LinstorError as error:
            raise LinstorDriverApiException(error)

    return f


class ThreadSafeLinstorClient(object):
    def __init__(self, configuration):
        self.configuration = configuration
        self._thread_local = threading.local()  # pylint: disable=no-member

    def get(self):
        """Returns a (thread-local) linstor client

        :rtype: linstor.Linstor
        """
        if not hasattr(self._thread_local, 'linstor_client'):
            client = linstor.MultiLinstor(
                self.configuration.linstor_uris,
                timeout=self.configuration.linstor_timeout,
            )
            client.keyfile = self.configuration.safe_get('linstor_client_key')
            client.certfile = self.configuration.safe_get(
                'linstor_client_cert',
            )
            client.cafile = self.configuration.safe_get('linstor_trusted_ca')
            self._thread_local.linstor_client = client
        return self._thread_local.linstor_client


@interface.volumedriver
class LinstorDriver(driver.VolumeDriver):
    """LINSTOR Driver.

    Manages Cinder volumes provisioned by LINSTOR.

    A quick overview on how names and concepts are mapped between Linstor and
    Cinder:
    * A Cinder Volume maps to a Resource (with one volume) in Linstor
    * A Cinder Snapshot maps to a Snapshot of a Resource in Linstor
    * A Cinder Volume Type maps to a Resource Group in Linstor.

    Version history:

    .. code-block:: none

        1.0.0 - Initial driver
        1.0.1 - Added support for LINSTOR 0.9.12
        1.1.0 - Updated driver to match LINSTOR backend improvements
        2.0.0 - Complete rewrite using python-linstor high-level API
          * Removed node and storage-pool creation
          * Support Linstor resource groups via cinder storage pools
          * Support live migration in direct attach mode
          * Limited support for snapshot revert
    """

    VERSION = '2.0.0'

    CI_WIKI_NAME = 'LINBIT_LINSTOR_CI'

    @volume_utils.trace
    def __init__(self, *args, **kwargs):
        super(LinstorDriver, self).__init__(*args, **kwargs)

        self.configuration.append_config_values(linstor_opts)
        self._stats = {}
        self._vendor_properties = {}
        self.target_driver = None  # type: targets.Target
        self.protocol = None  # type: str
        self.c = ThreadSafeLinstorClient(self.configuration)

    @staticmethod
    @volume_utils.trace
    def get_driver_options():
        return linstor_opts

    @volume_utils.trace
    def _use_direct_connection(self):
        """Should the driver attach the volume directly or not

        :returns: True, if the volume should be directly attached on the
          target. Requires the target to be part of the Linstor cluster
          False, if the volume should be attached via one of the transports
          included in Cinder (i.e. ISCSI).
        """
        return self.configuration.linstor_direct

    @wrap_linstor_api_exception
    @volume_utils.trace
    def check_for_setup_error(self):
        """Runs the set-up and verifies that it is in working order"""
        if not linstor:
            msg = _('Package python-linstor is not installed')
            raise LinstorDriverException(msg)

        if not hasattr(linstor, 'MultiLinstor'):
            msg = _('Package python-linstor does not support MultiLinstor, '
                    'please update')
            raise LinstorDriverException(msg)

        try:
            linstor.Resource('test', existing_client=self.c.get())
        except TypeError:
            msg = _('Package python-linstor does not support passing clients '
                    'to Resource class, please update')
            raise LinstorDriverException(msg)

        with self.c.get() as client:
            version_str = client.controller_version().rest_api_version
            nodes = client.node_list_raise(filter_by_nodes=[self._hostname])

        rest_version = tuple(int(n) for n in version_str.split("."))
        if rest_version < (1, 4, 0):
            msg = _('Linstor API not supported: %s < (1, 4, 0)') \
                % str(rest_version)
            raise LinstorDriverException(msg)

        if len(nodes.nodes) < 1:
            msg = _('Cinder host %s is not a configured Linstor '
                    'node') % self._hostname
            raise LinstorDriverException(msg)

        if self._use_direct_connection():
            self.target_driver = LinstorDirectTarget(self.c, self._force_udev)
            self.protocol = self.target_driver.protocol
        else:
            target_driver = self.target_mapping[
                self.configuration.target_helper
            ]

            LOG.debug('Attempting to initialize LINSTOR driver with the '
                      'following target_driver: %s',
                      target_driver)

            self.target_driver = importutils.import_object(
                target_driver,
                configuration=self.configuration,
                executor=self._execute)  # type: targets.Target
            self.protocol = self.target_driver.protocol

    @property
    def _linstor_uri_str(self):
        """String representation of all Linstor URIs"""
        return ",".join(sorted(self.configuration.linstor_uris))

    @property
    def _hostname(self):
        """Get the name of the local host"""
        if self.host:
            return volume_utils.extract_host(self.host, level='host')

        return socket.gethostname()

    @property
    def _force_udev(self):
        return self.configuration.safe_get('linstor_force_udev')

    @volume_utils.trace
    def _init_vendor_properties(self):
        """Return the vendor properties supported by this driver"""
        self._set_property(
            self._vendor_properties,
            'linstor:storage_pool',
            title='LINSTOR Storage Pool',
            description='Storage pool to use when auto-placing',
            type='str',
            default=self.configuration.safe_get(
                'linstor_default_storage_pool_name'
            ),
        )
        self._set_property(
            self._vendor_properties,
            'linstor:diskless_on_remaining',
            title='Diskless on remaining',
            description='Create diskless replicas on non-selected nodes after'
                        'auto-placing',
            type='bool',
            default=False,
        )
        self._set_property(
            self._vendor_properties,
            'linstor:do_not_place_with_regex',
            title='Do not place with regex',
            description='Do not place the resource on a node which has a '
                        'resource with a name matching the regex.',
            type='str',
        )
        self._set_property(
            self._vendor_properties,
            'linstor:layer_list',
            title='Layer List',
            description='Comma-separated list of layers to apply for resources'
                        'If empty, defaults to DRBD,Storage.',
            type='str',
        )
        self._set_property(
            self._vendor_properties,
            'linstor:provider_list',
            title='Provider list',
            description='Comma-separated list of providers to use',
            type='str',
        )
        self._set_property(
            self._vendor_properties,
            'linstor:redundancy',
            title='Redudancy',
            description='Number of replicas to create. Defaults to 2',
            type='int',
        )
        self._set_property(
            self._vendor_properties,
            'linstor:replicas_on_different',
            title='Replicas on different',
            description='A comma-separated list of key or key=value items '
                        'used as autoplacement selection labels when '
                        'autoplace is used to determine where to provision '
                        'storage',
            type='str',
        )
        self._set_property(
            self._vendor_properties,
            'linstor:replicas_on_same',
            title='Replicas on same',
            description='A comma-separated list of key or key=value items '
                        'used as autoplacement selection labels when '
                        'autoplace is used to determine where to provision '
                        'storage',
            type='str',
        )
        return self._vendor_properties, 'linstor'

    def _get_linstor_property(self, name, volume_type):
        """Retrieve the named property, either from the volume type or defaults

        :param str name: the name of the property to retrieve (without linstor
         prefix)
        :param cinder.objects.volume_type.VolumeType volume_type: The volume
         type containing the extra specs to check
        :return: The property value, if set
        :rtype: str|None
        """
        prefixed_name = "linstor:" + name
        extras = volume_type.get('extra_specs', {})
        if prefixed_name in extras:
            return extras[prefixed_name]
        return self._vendor_properties[prefixed_name].get('default')

    def _resource_group_for_volume_type(self, volume_type):
        """Ensure a LINSTOR resource group exists matching the volume type

        :param cinder.objects.volume_type.VolumeType volume_type: The volume
         type for which the resource group should exist
        :return:
        :rtype: linstor.ResourceGroup
        """
        if not volume_type:
            return linstor.ResourceGroup(
                self.configuration.linstor_default_resource_group_name,
                existing_client=self.c.get(),
            )

        # We use the ID here, as it is unique and compatible with LINSTOR
        # naming requirements. The cinder- prefix is required as LINSTOR names
        # have to start with an alphabetic character
        rg = linstor.ResourceGroup(
            'cinder-' + volume_type['id'],
            existing_client=self.c.get(),
        )

        description = 'For volume type "%s"' % volume_type['name']
        if rg.description != description:
            rg.description = description

        nr_volumes = 1
        if rg.nr_volumes != nr_volumes:
            rg.nr_volumes = nr_volumes

        storage_pool = self._get_linstor_property('storage_pool', volume_type)
        if storage_pool and rg.storage_pool != storage_pool.split(','):
            rg.storage_pool = storage_pool.split(',')

        diskless = self._get_linstor_property(
            'diskless_on_remaining', volume_type,
        )
        if rg.diskless_on_remaining != diskless:
            rg.diskless_on_remaining = diskless

        # do_not_place_with intentionally skipped, just use the regex version
        dnpw_r = self._get_linstor_property(
            'do_not_place_with_regex', volume_type,
        )
        if dnpw_r and rg.do_not_place_with_regex != dnpw_r:
            rg.do_not_place_with_regex = dnpw_r

        layer_list = self._get_linstor_property(
            'layer_list', volume_type
        )
        if layer_list and rg.layer_list != layer_list.split(','):
            rg.layer_list = layer_list.split(',')

        provider_list = self._get_linstor_property(
            'provider_list', volume_type,
        )
        if provider_list and rg.provider_list != provider_list.split(','):
            rg.provider_list = provider_list.split(',')

        redundancy = self._get_linstor_property(
            'redundancy', volume_type,
        )
        if redundancy and rg.redundancy != redundancy:
            rg.redundancy = redundancy

        def make_aux_list(propvalue):
            if propvalue is None:
                return None
            return ['Aux/%s' % item for item in propvalue.split(',')]

        on_diff = self._get_linstor_property(
            'replicas_on_different', volume_type,
        )
        on_diff = make_aux_list(on_diff)
        if on_diff and rg.replicas_on_different != on_diff:
            rg.replicas_on_different = on_diff

        on_same = self._get_linstor_property(
            'replicas_on_same', volume_type
        )
        on_same = make_aux_list(on_same)
        if on_same is not None and rg.replicas_on_same != on_same:
            rg.replicas_on_same = on_same

        extra_props = {}
        props_need_update = False
        existing = rg.property_dict
        for k, v in volume_type.get('extra_specs', {}).items():
            if not k.startswith('linstor:property:'):
                continue
            prop_name = k[len('linstor:property:'):]
            prop_name = prop_name.replace(':', '/')
            extra_props[prop_name] = v
            props_need_update |= existing.get(prop_name) != v
        if props_need_update:
            existing.update(extra_props)
            rg.property_dict = existing

        return rg

    @wrap_linstor_api_exception
    @volume_utils.trace
    def create_volume(self, volume):
        """Create a new volume

        :param cinder.objects.volume.Volume volume: The new volume to create
        :return: A dict of fields to update on the volume object
        """
        linstor_size = volume['size'] * units.Gi // units.Ki

        rg = self._resource_group_for_volume_type(volume['volume_type'])
        linstor.Resource.from_resource_group(
            uri="[unused]",
            resource_group_name=rg.name,
            resource_name=volume['name'],
            vlm_sizes=[linstor_size],
            existing_client=self.c.get(),
        )

        return {}

    @wrap_linstor_api_exception
    @volume_utils.trace
    def create_volume_from_snapshot(self, volume, snapshot):
        """Create a new volume from a snapshot

        :param cinder.objects.volume.Volume volume: The new volume to create
        :param cinder.objects.snapshot.Snapshot snapshot: snapshot to restore
         from
        :return: update for the volume model
        :rtype: dict
        """
        src = _get_existing_resource(
            self.c.get(),
            snapshot['volume']['name'],
            snapshot['volume_id'],
        )

        try:
            rsc = _restore_snapshot_to_new_resource(
                src, snapshot, volume['name'],
            )
        except linstor.LinstorError:
            leftover_rsc = linstor.Resource(
                volume['name'],
                existing_client=self.c.get(),
            )
            leftover_rsc.delete()
            raise

        try:
            expected_size = volume['size'] * units.Gi
            if rsc.volumes[0].size < expected_size:
                rsc.volumes[0].size = expected_size
        except linstor.LinstorError:
            # Ensure we don't have invalid volumes lying around in the backend
            LOG.exception('Could not resize restored Linstor volume, '
                          'deleting volume')
            rsc.delete()
            raise

        return {}

    @wrap_linstor_api_exception
    @volume_utils.trace
    def delete_volume(self, volume):
        """Delete the volume in the backend

        Does not succeed if snapshots are still attached. This should already
        be enforced in Cinder, Linstor just double checks.
        :param cinder.objects.volume.Volume volume: the volume to delete
        """
        rsc = _get_existing_resource(
            self.c.get(),
            volume['name'],
            volume['id'],
        )
        rsc.delete(snapshots=False)

        try:
            rg = linstor.ResourceGroup(
                rsc.resource_group_name,
                existing_client=self.c.get(),
            )
            rg.delete()
        except linstor.LinstorError as e:
            LOG.debug(
                'could not delete resource group %s, ignoring: %s',
                rsc.resource_group_name,
                e
            )

    @wrap_linstor_api_exception
    @volume_utils.trace
    def create_snapshot(self, snapshot):
        """Create a snapshot

        :param cinder.objects.snapshot.Snapshot snapshot: snapshot to create
        """
        rsc = _get_existing_resource(
            self.c.get(),
            snapshot['volume']['name'],
            snapshot['volume_id'],
        )
        rsc.snapshot_create(snapshot['name'])

    @wrap_linstor_api_exception
    @volume_utils.trace
    def delete_snapshot(self, snapshot):
        """Delete the given snapshot

        :param cinder.objects.snapshot.Snapshot snapshot: snapshot to delete
        """
        rsc = _get_existing_resource(
            self.c.get(),
            snapshot['volume']['name'],
            snapshot['volume_id'],
        )
        rsc.snapshot_delete(snapshot['name'])
        # This could _also_ be a snapshot created by the v1 driver, so delete
        # try to as well
        rsc.snapshot_delete('SN_' + snapshot['id'])

    @wrap_linstor_api_exception
    @volume_utils.trace
    def revert_to_snapshot(self, context, volume, snapshot):
        """Reverts a volume to a snapshot state

        LINSTOR can only revert to the last snapshot. Reverting to an older
        snapshot would mean we had to delete other snapshots first, which we
        can't do as Cinder still expects them to be present after the revert.
        :param context: request context
        :param cinder.objects.volume.Volume volume: The volume to revert
        :param cinder.objects.snapshot.Snapshot snapshot: The snapshot to
         revert to
        """
        rsc = _get_existing_resource(
            self.c.get(),
            snapshot['volume']['name'],
            snapshot['volume_id'],
        )
        try:
            rsc.snapshot_rollback(snapshot['name'])
        except linstor.LinstorError:
            LOG.info('Failed to rollback snapshot, retrying with v1 driver '
                     'name %s', 'SN_' + snapshot['id'])
            rsc.snapshot_rollback('SN_' + snapshot['id'])

        expected_size = volume['size'] * units.Gi
        if rsc.volumes[0].size < expected_size:
            rsc.volumes[0].size = expected_size

    def snapshot_revert_use_temp_snapshot(self):
        """Do not create a snapshot in case revert_to_snapshot_fails

        Otherwise we could never revert to any snapshot: Linstor only supports
        reverting to the last snapshot, but if this returns true, a new
        snapshot is created before every call to revert_to_snapshot.
        """
        return False

    @wrap_linstor_api_exception
    @volume_utils.trace
    def create_cloned_volume(self, volume, src_vref):
        """Create a copy of an existing volume

        :param cinder.objects.volume.Volume volume: The new clone
        :param cinder.objects.volume.Volume src_vref: The volume to clone from
        """
        temp_snap = {
            'id': 'for-' + volume['id'],
            'name': 'for-' + volume['id'],
            'volume': {
                'name': src_vref['name'],
                'id': src_vref['id'],
            },
            'volume_id': src_vref['id'],
        }

        try:
            self.create_snapshot(temp_snap)
            return self.create_volume_from_snapshot(volume, temp_snap)
        finally:
            self.delete_snapshot(temp_snap)

    @wrap_linstor_api_exception
    @volume_utils.trace
    def copy_volume_to_image(self, context, volume, image_service, image_meta):
        rsc = _get_existing_resource(
            self.c.get(),
            volume['name'],
            volume['id'],
        )

        with _temp_resource_path(self.c.get(), rsc, self._hostname,
                                 self._force_udev) as path:
            attach_info = {
                'conn': 'local',
                'device': {'path': path},
            }

            volume_utils.upload_volume(context,
                                       image_service,
                                       image_meta,
                                       attach_info['device']['path'],
                                       volume,
                                       compress=True)

    @wrap_linstor_api_exception
    @volume_utils.trace
    def _update_volume_stats(self):
        """Refresh the Cinder storage pool statistics for scheduling decisions

        We just aggregate all (per-node) storage pools as a total capacity,
        even if that does clash with how replicated volumes are using these
        storage pools.
        """

        with self.c.get() as lclient:
            storage_pools = lclient.storage_pool_list_raise() \
                .storage_pools
            resource_dfns = lclient.resource_dfn_list_raise() \
                .resource_definitions

        storage_pools = [sp for sp in storage_pools if
                         not sp.is_diskless()]

        backend_name = self.configuration.volume_backend_name or \
            self._linstor_uri_str

        tot = _kib_to_gib(sum(
            p.free_space.total_capacity for p in storage_pools
        ))
        free = _kib_to_gib(sum(
            p.free_space.free_capacity for p in storage_pools
        ))
        provisioned_cap = _kib_to_gib(sum(
            vd.size for rd in resource_dfns for vd in rd.volume_definitions
        ))
        thin = any(p.is_thin() for p in storage_pools)
        fat = any(not p.is_fat() for p in storage_pools)
        volumes_in_pool = len(resource_dfns)

        self._stats = {
            'volume_backend_name': backend_name,
            'vendor_name': 'LINBIT',
            'driver_version': self.get_version(),
            'storage_protocol': self.protocol,
            'location_info': self._linstor_uri_str,
            # Multiattach works in case of ISCSI, as the backing volume
            # is only opened on the cinder host.
            'multiattach': not self._use_direct_connection(),
            # nova does not support resizing local volumes
            'online_extend_support': not self._use_direct_connection(),
            'total_capacity_gb': tot,
            'provisioned_capacity_gb': provisioned_cap,
            'free_capacity_gb': free,
            'max_over_subscription_ratio': 20.0 if thin else 0.0,
            'thin_provisioning_support': thin,
            'thick_provisioning_support': fat,
            'total_volumes': volumes_in_pool,
            'goodness_function': self.get_goodness_function(),
            'filter_function': self.get_filter_function(),
        }

        return self._stats

    @wrap_linstor_api_exception
    @volume_utils.trace
    def extend_volume(self, volume, new_size):
        rsc = _get_existing_resource(
            self.c.get(),
            volume['name'],
            volume['id'],
        )

        rsc.volumes[0].size = new_size * units.Gi

        if hasattr(self.target_driver, 'extend_target'):
            # ISCSI targets require additional resize encouragement
            self.target_driver.extend_target(volume)

    @wrap_linstor_api_exception
    @volume_utils.trace
    def retype(self, context, volume, new_type, diff, host):
        """Retype a volume, i.e. allow updating QoS and extra specs"""
        LOG.debug('LINSTOR retype called for volume %s. No action '
                  'required for LINSTOR volumes.',
                  volume['id'])
        rg = self._resource_group_for_volume_type(new_type)
        rsc = _get_existing_resource(
            self.c.get(),
            volume['name'],
            volume['id'],
        )

        with self.c.get() as lclient:
            responses = lclient.resource_dfn_modify(
                rsc.linstor_name, property_dict={}, resource_group=rg.name,
            )
            if not lclient.all_api_responses_no_error(responses):
                raise LinstorDriverApiException(responses)

        return True, None

    @wrap_linstor_api_exception
    @volume_utils.trace
    def migrate_volume(self, context, volume, host):
        """Migrate a volume from one backend to another

        :param context: the request context
        :param volume: The volume to migrate (away from the self)
        :param host: The host/backend to migrate to
        :return: (True, model_update) if migration was successful
                 (False, None) if the volume could not be migrated
        """
        target_ctrl = host['capabilities'].get('location_info')
        if self._linstor_uri_str != target_ctrl:
            LOG.debug('Target is not using the same controllers: %s != %s',
                      self._linstor_uri_str, target_ctrl)
            return False, None

        target_protocol = host['capabilities'].get('storage_protocol')
        if volume['status'] in {'attached', 'in-use'} and \
                self.protocol != target_protocol:
            LOG.debug('Cannot migrate attached volume between different '
                      'transport protocols: %s -> %s',
                      self.protocol, target_protocol)
            return False, None

        return True, None

    # ====================== Transport related operations ====================
    # Mostly just passes through to the transport. One thing to note: in case
    # Of non-linstor-managed attach (linstor_direct=False) we need to have the
    # resource available locally for the target helper to attach
    @wrap_linstor_api_exception
    @volume_utils.trace
    def ensure_export(self, context, volume):
        rsc = _get_existing_resource(
            self.c.get(),
            volume['name'],
            volume['id'],
        )
        volume_path = None
        if not self._use_direct_connection():
            LOG.debug('using non-direct driver method, need to create local '
                      'replica on cinder host')
            volume_path = _ensure_resource_path(
                self.c.get(), rsc, self._hostname,
            )

        return self.target_driver.ensure_export(context, volume, volume_path)

    @wrap_linstor_api_exception
    @volume_utils.trace
    def create_export(self, context, volume, connector):
        rsc = _get_existing_resource(
            self.c.get(),
            volume['name'],
            volume['id'],
        )
        volume_path = None
        if not self._use_direct_connection():
            LOG.debug('using non-direct driver method, need to create local '
                      'replica on cinder host')
            volume_path = _ensure_resource_path(
                self.c.get(), rsc, self._hostname, self._force_udev
            )

        export_info = self.target_driver.create_export(
            context,
            volume,
            volume_path)

        if export_info:
            return {'provider_location': export_info['location'],
                    'provider_auth': export_info['auth'], }

        return {}

    @wrap_linstor_api_exception
    @volume_utils.trace
    def remove_export(self, context, volume):
        self.target_driver.remove_export(context, volume)
        if not self._use_direct_connection():
            LOG.debug('using non-direct driver method, need to delete local '
                      'replica on cinder host')
            rsc = _get_existing_resource(
                self.c.get(),
                volume['name'],
                volume['id'],
            )
            rsc.deactivate(self._hostname)

    @wrap_linstor_api_exception
    @volume_utils.trace
    def initialize_connection(self, volume, connector, **kwargs):
        return self.target_driver.initialize_connection(volume, connector)

    @wrap_linstor_api_exception
    @volume_utils.trace
    def terminate_connection(self, volume, connector, **kwargs):
        # This is lifted from the LVMDriver. We only want to terminate
        # the connection if no attachments remain. This is important in multi-
        # attach scenarios.
        #
        # Linstor exposes a similar interface to LVM for ISCSI Targets, so we
        # can reuse this code from there. DRBD targets do not support multi-
        # attach in any case, so it will do a normal detach in that case.
        attachments = volume['volume_attachment']
        if volume['multiattach']:
            if sum(1 for a in attachments if a.connector and
                    a.connector['initiator'] == connector['initiator']) > 1:
                return True

        self.target_driver.terminate_connection(volume, connector, **kwargs)
        return len(attachments) > 1


@interface.volumedriver
class LinstorDrbdDriver(LinstorDriver):
    """Shim for a Linstor driver compatible with v1 LinstorDrbdDriver"""

    def __init__(self, *args, **kwargs):
        super(LinstorDrbdDriver, self).__init__(*args, **kwargs)

    def _use_direct_connection(self):
        return True


@interface.volumedriver
class LinstorIscsiDriver(LinstorDriver):
    """Shim for a Linstor driver compatible with v1 LinstorIscsiDriver"""

    def __init__(self, *args, **kwargs):
        super(LinstorIscsiDriver, self).__init__(*args, **kwargs)

    def _use_direct_connection(self):
        return False


class LinstorDirectTarget(targets.Target):
    """Target object that uses Linstor to create block devices"""
    # This may be a lie as there are other ways LINSTOR could do replication,
    # but this way we stay compatible with the v1 drivers
    protocol = constants.DRBD

    def __init__(self, client, force_udev=True, *args, **kwargs):
        """Uses Linstor to deploy resources directly on the target host

        :param ThreadSafeLinstorClient client: the client wrapper to use
        :param bool force_udev: Assume udev paths always exist.
        """
        super().__init__(*args, **kwargs)
        self.c = client
        self._force_udev = force_udev

    def ensure_export(self, context, volume, volume_path):
        pass

    def create_export(self, context, volume, volume_path):
        pass

    def remove_export(self, context, volume):
        pass

    @wrap_linstor_api_exception
    def initialize_connection(self, volume, connector):
        """Creates a connection and tells the target how to connect

        This target-driver ensures a replica of the request volume is available
        locally on the connection target.
        """
        rsc = _get_existing_resource(
            self.c.get(),
            volume['name'],
            volume['id']
        )

        if connector['host'] not in _attached_on(volume):
            LOG.debug('Trying to attach to a volume in use, looks like live '
                      'migration. Setting "allow_two_primaries=True"')
            rsc.allow_two_primaries = True

        path = _ensure_resource_path(
            self.c.get(), rsc, connector['host'], self._force_udev,
        )
        return {
            'driver_volume_type': 'local',
            'data': {'device_path': path},
        }

    @wrap_linstor_api_exception
    def terminate_connection(self, volume, connector, **kwargs):
        """Terminates an existing connection

        This target-driver removes replicas created in initialize_connection.
        :param volume: the connected volume
        :param dict|None connector: which connection to terminate, or None if
         all connection should be terminated
        """
        rsc = _get_existing_resource(
            self.c.get(),
            volume['name'],
            volume['id']
        )

        if connector is None:
            LOG.debug('force detach of volume, no clever deactivating of '
                      'resources required')
            # Since we detach everything we can also reset this
            rsc.allow_two_primaries = False
            return

        if volume['status'] == 'in-use' and \
                any(x != connector['host'] for x in _attached_on(volume)):
            LOG.debug('Trying to detach from a volume in use, looks like live '
                      'migration. Resetting "allow_two_primaries=False"')
            rsc.allow_two_primaries = False

        # This might delete the tiebreaker. Think about a workaround!
        rsc.deactivate(connector['host'])


@contextlib.contextmanager
@wrap_linstor_api_exception
def _temp_resource_path(linstor_client, rsc, host, force_udev=True):
    """Temporarily attach the given resource on a host

    :param linstor.Linstor linstor_client: Client used for API calls
    :param linstor.Resource rsc: The resource to attach
    :param str host: The host as named in LINSTOR
    :param bool force_udev: Assume udev paths always exist.
    :return: The path to the temporary device
    :rtype: str
    """
    try:
        yield _ensure_resource_path(linstor_client, rsc, host, force_udev)
    finally:
        rsc.deactivate(host)


@wrap_linstor_api_exception
def _ensure_resource_path(linstor_client, rsc, host, force_udev=True):
    """Ensure a resource is deployed on a host and return its device path

    :param linstor.Linstor linstor_client: Client used for API calls
    :param linstor.Resource rsc: The resource to deploy on the node
    :param str host: The host as named in Linstor
    :param bool force_udev: Assume udev paths always exist.
    :return: The path to the deployed node
    :rtype: str
    """
    rsc.activate(host)
    symlink = _find_symlink_to_device(linstor_client, rsc.name, host)
    if symlink:
        return symlink
    if force_udev:
        return "/dev/drbd/by-res/%s/0" % rsc.name

    return rsc.volumes[0].device_path


def _get_existing_resource(linstor_client, volume_name, volume_id):
    """Get an existing resource matching a cinder volume

    :param linstor.Linstor linstor_client: Client used for API calls
    :param str volume_name: The name of the volume (most likely volume-<id>)
    :param str volume_id: The id of the volume (most likely a UUIDv4)
    :return: The matching resource object
    :rtype: linstor.Resource
    """
    LOG.debug('Searching existing volume %s in LINSTOR '
              'backend', volume_name)
    plain_rsc = linstor.Resource(volume_name, existing_client=linstor_client)
    if plain_rsc.defined:
        LOG.debug('Found matching resource named: %s', plain_rsc.name)
        return plain_rsc

    alt_name = "CV_" + volume_id
    alt_rsc = linstor.Resource(alt_name, existing_client=linstor_client)
    if alt_rsc.defined:
        LOG.debug('Found matching resource named: %s', alt_rsc.name)
        return alt_rsc

    msg = _('Found no matching resource in LINSTOR backend for '
            'volume %s') % volume_name
    raise LinstorDriverException(msg)


@wrap_linstor_api_exception
def _restore_snapshot_to_new_resource(resource, snap, restore_name):
    """Try restoring a snapshot to a new resource

    Note: in case of an exception during the restore process things such as
    the resource definition of the target can be left on the server. To retry
    the process ensure those resources are deleted first.
    :param linstor.Resource resource: The source of the snapshot
    :param cinder.objects.snapshot.Snapshot snap: The snapshot to restore.
    If it can't be found, it's retried with a v1 compatible name.
    :param str restore_name: The name of the resource to restore to.
    :return: The restored resource
    :rtype: linstor.Resource
    """
    try:
        return resource.restore_from_snapshot(snap['name'], restore_name)
    except linstor.LinstorError:
        LOG.info('failed to restore snapshot, retrying with fallback id')
        return resource.restore_from_snapshot('SN_' + snap['id'], restore_name)


@wrap_linstor_api_exception
def _find_symlink_to_device(linstor_client, resource_name, node):
    """Get the symlink to a device managed by Linstor

    This is useful when managing encrypted volumes, since os_brick likes to
    mess with the device_path provided. In particular, it replaces the local
    device path with a symlink to the decrypted volume, but never changes it
    back. If we gave the device path directly os_brick would be happy to
    replace the device file with a symlink, which finally breaks Linstor as
    the device points to a potentially non-existent volume.

    :param linstor.Linstor linstor_client: Client used for API calls
    :param str resource_name: The resource for which to find the symlink
    :param str node: The name of the node for which to find the symlink
    :returns: A symlink as a path, or None if no symlink was found
    :rtype: str|None
    """
    with linstor_client as lclient:
        vol_list = lclient.volume_list_raise(
            filter_by_nodes=[node],
            filter_by_resources=[resource_name],
        )
    if len(vol_list.resources) != 1:
        msg = _('Unexpected response to volume_list: %s') % vol_list.resources
        raise LinstorDriverException(msg)

    volume = vol_list.resources[0].volumes[0]
    links = [v for k, v in volume.properties.items()
             if k.startswith('Satellite/Device/Symlinks/')]

    if not links:
        LOG.debug('Could not find symlinks: No udev rules or Linstor too old?')
        return None

    for symlink in links:
        # /dev/drbd/by-res/... is the preferred symlink
        if 'by-res' in symlink:
            return symlink

    # Fallback: just return any
    return links[0]


def _kib_to_gib(kib):
    """Converts KiB to GiB with rounding up

    :param int kib: the value to convert in KiB
    :returns: Value in GiB, rounded up
    :rtype: int
    """
    return linstor.SizeCalc.convert_round_up(
        kib,
        linstor.SizeCalc.UNIT_KiB,
        linstor.SizeCalc.UNIT_GiB,
    )


def _attached_on(volume):
    """Checks if the volume is attached on another host

    :param volume cinder.objects.volume.Volume: the current volume state
    :returns: The list of hosts this is currently attached at
    :rtype: list[str]
    """
    return [attachment['attached_host']
            for attachment in volume['volume_attachment']]


class LinstorDriverException(exception.VolumeDriverException):
    def __init__(self, message=None, **kwargs):
        super().__init__(message, **kwargs)


class LinstorDriverApiException(exception.VolumeBackendAPIException):
    def __init__(self, error, **kwargs):
        super().__init__(data=error, **kwargs)
