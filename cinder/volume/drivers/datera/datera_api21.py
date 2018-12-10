# Copyright 2017 Datera
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

import random
import re
import uuid

import eventlet
import ipaddress
import six

from oslo_log import log as logging
from oslo_utils import excutils
from oslo_utils import units

from cinder import exception
from cinder.i18n import _
from cinder.volume import utils as volutils

import cinder.volume.drivers.datera.datera_common as datc

LOG = logging.getLogger(__name__)


class DateraApi(object):

    # =================
    # = Create Volume =
    # =================

    def _create_volume_2_1(self, volume):
        tenant = self._create_tenant(volume)
        policies = self._get_policies_for_resource(volume)
        num_replicas = int(policies['replica_count'])
        storage_name = policies['default_storage_name']
        volume_name = policies['default_volume_name']
        template = policies['template']
        placement = policies['placement_mode']

        if template:
            app_params = (
                {
                    'create_mode': "openstack",
                    # 'uuid': str(volume['id']),
                    'name': datc._get_name(volume['id']),
                    'app_template': '/app_templates/{}'.format(template)
                })

        else:

            app_params = (
                {
                    'create_mode': "openstack",
                    'uuid': str(volume['id']),
                    'name': datc._get_name(volume['id']),
                    'access_control_mode': 'deny_all',
                    'storage_instances': [
                        {
                            'name': storage_name,
                            'volumes': [
                                {
                                    'name': volume_name,
                                    'size': volume['size'],
                                    'placement_mode': placement,
                                    'replica_count': num_replicas,
                                    'snapshot_policies': [
                                    ]
                                }
                            ]
                        }
                    ]
                })
        self._issue_api_request(
            datc.URL_TEMPLATES['ai'](),
            'post',
            body=app_params,
            api_version='2.1',
            tenant=tenant)
        self._update_qos_2_1(volume, policies, tenant)

    # =================
    # = Extend Volume =
    # =================

    def _extend_volume_2_1(self, volume, new_size):
        tenant = self._create_tenant(volume)
        policies = self._get_policies_for_resource(volume)
        template = policies['template']
        if template:
            LOG.warning("Volume size not extended due to template binding:"
                        " volume: %(volume)s, template: %(template)s",
                        volume=volume, template=template)
            return

        # Offline App Instance, if necessary
        reonline = False
        app_inst = self._issue_api_request(
            datc.URL_TEMPLATES['ai_inst']().format(
                datc._get_name(volume['id'])),
            api_version='2.1', tenant=tenant)
        if app_inst['data']['admin_state'] == 'online':
            reonline = True
            self._detach_volume_2_1(None, volume)
        # Change Volume Size
        app_inst = datc._get_name(volume['id'])
        data = {
            'size': new_size
        }
        store_name, vol_name = self._scrape_template(policies)
        self._issue_api_request(
            datc.URL_TEMPLATES['vol_inst'](
                store_name, vol_name).format(app_inst),
            method='put',
            body=data,
            api_version='2.1',
            tenant=tenant)
        # Online Volume, if it was online before
        if reonline:
            self._create_export_2_1(None, volume, None)

    # =================
    # = Cloned Volume =
    # =================

    def _create_cloned_volume_2_1(self, volume, src_vref):
        policies = self._get_policies_for_resource(volume)
        tenant = self._create_tenant(volume)
        store_name, vol_name = self._scrape_template(policies)

        src = "/" + datc.URL_TEMPLATES['vol_inst'](
            store_name, vol_name).format(datc._get_name(src_vref['id']))
        data = {
            'create_mode': 'openstack',
            'name': datc._get_name(volume['id']),
            'uuid': str(volume['id']),
            'clone_volume_src': {'path': src},
        }
        self._issue_api_request(
            datc.URL_TEMPLATES['ai'](), 'post', body=data, api_version='2.1',
            tenant=tenant)

        if volume['size'] > src_vref['size']:
            self._extend_volume_2_1(volume, volume['size'])

    # =================
    # = Delete Volume =
    # =================

    def _delete_volume_2_1(self, volume):
        self._detach_volume_2_1(None, volume)
        tenant = self._create_tenant(volume)
        app_inst = datc._get_name(volume['id'])
        try:
            self._issue_api_request(
                datc.URL_TEMPLATES['ai_inst']().format(app_inst),
                method='delete',
                api_version='2.1',
                tenant=tenant)
        except exception.NotFound:
            msg = ("Tried to delete volume %s, but it was not found in the "
                   "Datera cluster. Continuing with delete.")
            LOG.info(msg, datc._get_name(volume['id']))

    # =================
    # = Ensure Export =
    # =================

    def _ensure_export_2_1(self, context, volume, connector=None):
        self.create_export(context, volume, connector)

    # =========================
    # = Initialize Connection =
    # =========================

    def _initialize_connection_2_1(self, volume, connector):
        # Now online the app_instance (which will online all storage_instances)
        multipath = connector.get('multipath', False)
        tenant = self._create_tenant(volume)
        url = datc.URL_TEMPLATES['ai_inst']().format(
            datc._get_name(volume['id']))
        data = {
            'admin_state': 'online'
        }
        app_inst = self._issue_api_request(
            url, method='put', body=data, api_version='2.1', tenant=tenant)[
            'data']
        storage_instances = app_inst["storage_instances"]
        si = storage_instances[0]

        # randomize portal chosen
        choice = 0
        policies = self._get_policies_for_resource(volume)
        if policies["round_robin"]:
            choice = random.randint(0, 1)
        portal = si['access']['ips'][choice] + ':3260'
        iqn = si['access']['iqn']
        if multipath:
            portals = [p + ':3260' for p in si['access']['ips']]
            iqns = [iqn for _ in si['access']['ips']]
            lunids = [self._get_lunid() for _ in si['access']['ips']]

            result = {
                'driver_volume_type': 'iscsi',
                'data': {
                    'target_discovered': False,
                    'target_iqn': iqn,
                    'target_iqns': iqns,
                    'target_portal': portal,
                    'target_portals': portals,
                    'target_lun': self._get_lunid(),
                    'target_luns': lunids,
                    'volume_id': volume['id'],
                    'discard': False}}
        else:
            result = {
                'driver_volume_type': 'iscsi',
                'data': {
                    'target_discovered': False,
                    'target_iqn': iqn,
                    'target_portal': portal,
                    'target_lun': self._get_lunid(),
                    'volume_id': volume['id'],
                    'discard': False}}

        return result

    # =================
    # = Create Export =
    # =================

    def _create_export_2_1(self, context, volume, connector):
        tenant = self._create_tenant(volume)
        url = datc.URL_TEMPLATES['ai_inst']().format(
            datc._get_name(volume['id']))
        data = {
            'admin_state': 'offline',
            'force': True
        }
        self._issue_api_request(
            url, method='put', body=data, api_version='2.1', tenant=tenant)
        policies = self._get_policies_for_resource(volume)
        store_name, _ = self._scrape_template(policies)
        if connector and connector.get('ip'):
            # Case where volume_type has non default IP Pool info
            if policies['ip_pool'] != 'default':
                initiator_ip_pool_path = self._issue_api_request(
                    "access_network_ip_pools/{}".format(
                        policies['ip_pool']),
                    api_version='2.1',
                    tenant=tenant)['path']
            # Fallback to trying reasonable IP based guess
            else:
                initiator_ip_pool_path = self._get_ip_pool_for_string_ip_2_1(
                    connector['ip'])

            ip_pool_url = datc.URL_TEMPLATES['si_inst'](
                store_name).format(datc._get_name(volume['id']))
            ip_pool_data = {'ip_pool': {'path': initiator_ip_pool_path}}
            self._issue_api_request(ip_pool_url,
                                    method="put",
                                    body=ip_pool_data,
                                    api_version='2.1',
                                    tenant=tenant)
        url = datc.URL_TEMPLATES['ai_inst']().format(
            datc._get_name(volume['id']))
        data = {
            'admin_state': 'online'
        }
        self._issue_api_request(
            url, method='put', body=data, api_version='2.1', tenant=tenant)
        # Check if we've already setup everything for this volume
        url = (datc.URL_TEMPLATES['si']().format(datc._get_name(volume['id'])))
        storage_instances = self._issue_api_request(
            url, api_version='2.1', tenant=tenant)
        # Handle adding initiator to product if necessary
        # Then add initiator to ACL
        if (connector and
                connector.get('initiator') and
                not policies['acl_allow_all']):
            initiator_name = "OpenStack_{}_{}".format(
                self.driver_prefix, str(uuid.uuid4())[:4])
            initiator_group = datc.INITIATOR_GROUP_PREFIX + str(uuid.uuid4())
            found = False
            initiator = connector['initiator']
            if not found:
                data = {'id': initiator, 'name': initiator_name}
                # Try and create the initiator
                # If we get a conflict, ignore it
                self._issue_api_request("initiators",
                                        method="post",
                                        body=data,
                                        conflict_ok=True,
                                        api_version='2.1',
                                        tenant=tenant)
            # Create initiator group with initiator in it
            initiator_path = "/initiators/{}".format(initiator)
            initiator_group_path = "/initiator_groups/{}".format(
                initiator_group)
            ig_data = {'name': initiator_group,
                       'members': [{'path': initiator_path}]}
            self._issue_api_request("initiator_groups",
                                    method="post",
                                    body=ig_data,
                                    conflict_ok=True,
                                    api_version='2.1',
                                    tenant=tenant)
            # Create ACL with initiator group as reference for each
            # storage_instance in app_instance
            # TODO(_alastor_): We need to avoid changing the ACLs if the
            # template already specifies an ACL policy.
            for si in storage_instances['data']:
                acl_url = (datc.URL_TEMPLATES['si']() +
                           "/{}/acl_policy").format(
                    datc._get_name(volume['id']), si['name'])
                existing_acl = self._issue_api_request(acl_url,
                                                       method="get",
                                                       api_version='2.1',
                                                       tenant=tenant)['data']
                data = {}
                data['initiators'] = existing_acl['initiators']
                data['initiator_groups'] = existing_acl['initiator_groups']
                data['initiator_groups'].append({"path": initiator_group_path})
                self._issue_api_request(acl_url,
                                        method="put",
                                        body=data,
                                        api_version='2.1',
                                        tenant=tenant)
        # Check to ensure we're ready for go-time
        self._si_poll_2_1(volume, policies, tenant)

    # =================
    # = Detach Volume =
    # =================

    def _detach_volume_2_1(self, context, volume, attachment=None):
        tenant = self._create_tenant(volume)
        url = datc.URL_TEMPLATES['ai_inst']().format(
            datc._get_name(volume['id']))
        data = {
            'admin_state': 'offline',
            'force': True
        }
        try:
            self._issue_api_request(url, method='put', body=data,
                                    api_version='2.1', tenant=tenant)
        except exception.NotFound:
            msg = ("Tried to detach volume %s, but it was not found in the "
                   "Datera cluster. Continuing with detach.")
            LOG.info(msg, volume['id'])
        # TODO(_alastor_): Make acl cleaning multi-attach aware
        self._clean_acl_2_1(volume, tenant)

    def _check_for_acl_2_1(self, initiator_path):
        """Returns True if an acl is found for initiator_path """
        # TODO(_alastor_) when we get a /initiators/:initiator/acl_policies
        # endpoint use that instead of this monstrosity
        initiator_groups = self._issue_api_request("initiator_groups",
                                                   api_version='2.1')
        for ig, igdata in initiator_groups.items():
            if initiator_path in igdata['members']:
                LOG.debug("Found initiator_group: %s for initiator: %s",
                          ig, initiator_path)
                return True
        LOG.debug("No initiator_group found for initiator: %s", initiator_path)
        return False

    def _clean_acl_2_1(self, volume, tenant):
        policies = self._get_policies_for_resource(volume)

        store_name, _ = self._scrape_template(policies)

        acl_url = (datc.URL_TEMPLATES["si_inst"](
            store_name) + "/acl_policy").format(datc._get_name(volume['id']))
        try:
            initiator_group = self._issue_api_request(
                acl_url, api_version='2.1', tenant=tenant)['data'][
                    'initiator_groups'][0]['path']
            # TODO(_alastor_): Re-enable this when we get a force-delete
            # option on the /initiators endpoint
            # initiator_iqn_path = self._issue_api_request(
            #     initiator_group.lstrip("/"), api_version='2.1',
            #     tenant=tenant)[
            #         "data"]["members"][0]["path"]
            # Clear out ACL and delete initiator group
            self._issue_api_request(acl_url,
                                    method="put",
                                    body={'initiator_groups': []},
                                    api_version='2.1',
                                    tenant=tenant)
            self._issue_api_request(initiator_group.lstrip("/"),
                                    method="delete",
                                    api_version='2.1',
                                    tenant=tenant)
            # TODO(_alastor_): Re-enable this when we get a force-delete
            # option on the /initiators endpoint
            # if not self._check_for_acl_2_1(initiator_iqn_path):
            #     self._issue_api_request(initiator_iqn_path.lstrip("/"),
            #                             method="delete",
            #                             api_version='2.1',
            #                             tenant=tenant)
        except (IndexError, exception.NotFound):
            LOG.debug("Did not find any initiator groups for volume: %s",
                      volume)

    # ===================
    # = Create Snapshot =
    # ===================

    def _create_snapshot_2_1(self, snapshot):
        tenant = self._create_tenant(snapshot)
        policies = self._get_policies_for_resource(snapshot)

        store_name, vol_name = self._scrape_template(policies)

        url_template = datc.URL_TEMPLATES['vol_inst'](
            store_name, vol_name) + '/snapshots'
        url = url_template.format(datc._get_name(snapshot['volume_id']))

        snap_params = {
            'uuid': snapshot['id'],
        }
        snap = self._issue_api_request(url, method='post', body=snap_params,
                                       api_version='2.1', tenant=tenant)
        snapu = "/".join((url, snap['data']['timestamp']))
        self._snap_poll_2_1(snapu, tenant)

    # ===================
    # = Delete Snapshot =
    # ===================

    def _delete_snapshot_2_1(self, snapshot):
        tenant = self._create_tenant(snapshot)
        policies = self._get_policies_for_resource(snapshot)

        store_name, vol_name = self._scrape_template(policies)

        snap_temp = datc.URL_TEMPLATES['vol_inst'](
            store_name, vol_name) + '/snapshots'
        snapu = snap_temp.format(datc._get_name(snapshot['volume_id']))
        snapshots = []
        try:
            snapshots = self._issue_api_request(snapu,
                                                method='get',
                                                api_version='2.1',
                                                tenant=tenant)
        except exception.NotFound:
            msg = ("Tried to delete snapshot %s, but parent volume %s was "
                   "not found in Datera cluster. Continuing with delete.")
            LOG.info(msg,
                     datc._get_name(snapshot['id']),
                     datc._get_name(snapshot['volume_id']))
            return

        try:
            for snap in snapshots['data']:
                if snap['uuid'] == snapshot['id']:
                    url_template = snapu + '/{}'
                    url = url_template.format(snap['timestamp'])
                    self._issue_api_request(
                        url,
                        method='delete',
                        api_version='2.1',
                        tenant=tenant)
                    break
            else:
                raise exception.NotFound
        except exception.NotFound:
            msg = ("Tried to delete snapshot %s, but was not found in "
                   "Datera cluster. Continuing with delete.")
            LOG.info(msg, datc._get_name(snapshot['id']))

    # ========================
    # = Volume From Snapshot =
    # ========================

    def _create_volume_from_snapshot_2_1(self, volume, snapshot):
        tenant = self._create_tenant(volume)
        policies = self._get_policies_for_resource(snapshot)

        store_name, vol_name = self._scrape_template(policies)

        snap_temp = datc.URL_TEMPLATES['vol_inst'](
            store_name, vol_name) + '/snapshots'
        snapu = snap_temp.format(datc._get_name(snapshot['volume_id']))
        snapshots = self._issue_api_request(
            snapu, method='get', api_version='2.1', tenant=tenant)

        for snap in snapshots['data']:
            if snap['uuid'] == snapshot['id']:
                found_ts = snap['utc_ts']
                break
        else:
            raise exception.NotFound

        snap_url = (snap_temp + '/{}').format(
            datc._get_name(snapshot['volume_id']), found_ts)

        self._snap_poll_2_1(snap_url, tenant)

        src = "/" + snap_url
        app_params = (
            {
                'create_mode': 'openstack',
                'uuid': str(volume['id']),
                'name': datc._get_name(volume['id']),
                'clone_snapshot_src': {'path': src},
            })
        self._issue_api_request(
            datc.URL_TEMPLATES['ai'](),
            method='post',
            body=app_params,
            api_version='2.1',
            tenant=tenant)

        if (volume['size'] > snapshot['volume_size']):
            self._extend_volume_2_1(volume, volume['size'])

    # ==========
    # = Retype =
    # ==========

    def _retype_2_1(self, ctxt, volume, new_type, diff, host):
        LOG.debug("Retype called\n"
                  "Volume: %(volume)s\n"
                  "NewType: %(new_type)s\n"
                  "Diff: %(diff)s\n"
                  "Host: %(host)s\n", {'volume': volume, 'new_type': new_type,
                                       'diff': diff, 'host': host})
        # We'll take the fast route only if the types share the same backend
        # And that backend matches this driver
        old_pol = self._get_policies_for_resource(volume)
        new_pol = self._get_policies_for_volume_type(new_type)
        if (host['capabilities']['vendor_name'].lower() ==
                self.backend_name.lower()):
            LOG.debug("Starting fast volume retype")

            if old_pol.get('template') or new_pol.get('template'):
                LOG.warning(
                    "Fast retyping between template-backed volume-types "
                    "unsupported.  Type1: %s, Type2: %s",
                    volume['volume_type_id'], new_type)

            tenant = self._create_tenant(volume)
            self._update_qos_2_1(volume, new_pol, tenant)
            vol_params = (
                {
                    'placement_mode': new_pol['placement_mode'],
                    'replica_count': new_pol['replica_count'],
                })
            url = datc.URL_TEMPLATES['vol_inst'](
                old_pol['default_storage_name'],
                old_pol['default_volume_name']).format(
                    datc._get_name(volume['id']))
            self._issue_api_request(url, method='put', body=vol_params,
                                    api_version='2.1', tenant=tenant)
            return True

        else:
            LOG.debug("Couldn't fast-retype volume between specified types")
            return False

    # ==========
    # = Manage =
    # ==========

    def _manage_existing_2_1(self, volume, existing_ref):
        # Only volumes created under the requesting tenant can be managed in
        # the v2.1 API.  Eg.  If tenant A is the tenant for the volume to be
        # managed, it must also be tenant A that makes this request.
        # This will be fixed in a later API update
        tenant = self._create_tenant(volume)
        existing_ref = existing_ref['source-name']
        if existing_ref.count(":") not in (2, 3):
            raise exception.ManageExistingInvalidReference(
                _("existing_ref argument must be of this format: "
                  "tenant:app_inst_name:storage_inst_name:vol_name or "
                  "app_inst_name:storage_inst_name:vol_name"))
        app_inst_name = existing_ref.split(":")[0]
        try:
            (tenant, app_inst_name, storage_inst_name,
                vol_name) = existing_ref.split(":")
        except TypeError:
            app_inst_name, storage_inst_name, vol_name = existing_ref.split(
                ":")
            tenant = None
        LOG.debug("Managing existing Datera volume %s  "
                  "Changing name to %s",
                  datc._get_name(volume['id']), existing_ref)
        data = {'name': datc._get_name(volume['id'])}
        self._issue_api_request(datc.URL_TEMPLATES['ai_inst']().format(
            app_inst_name), method='put', body=data, api_version='2.1',
            tenant=tenant)

    # ===================
    # = Manage Get Size =
    # ===================

    def _manage_existing_get_size_2_1(self, volume, existing_ref):
        tenant = self._create_tenant(volume)
        existing_ref = existing_ref['source-name']
        if existing_ref.count(":") != 2:
            raise exception.ManageExistingInvalidReference(
                _("existing_ref argument must be of this format:"
                  "app_inst_name:storage_inst_name:vol_name"))
        app_inst_name, si_name, vol_name = existing_ref.split(":")
        app_inst = self._issue_api_request(
            datc.URL_TEMPLATES['ai_inst']().format(app_inst_name),
            api_version='2.1', tenant=tenant)
        return self._get_size_2_1(
            volume, tenant, app_inst, si_name, vol_name)

    def _get_size_2_1(self, volume, tenant=None, app_inst=None, si_name=None,
                      vol_name=None):
        """Helper method for getting the size of a backend object

        If app_inst is provided, we'll just parse the dict to get
        the size instead of making a separate http request
        """
        policies = self._get_policies_for_resource(volume)
        si_name = si_name if si_name else policies['default_storage_name']
        vol_name = vol_name if vol_name else policies['default_volume_name']
        if not app_inst:
            vol_url = datc.URL_TEMPLATES['ai_inst']().format(
                datc._get_name(volume['id']))
            app_inst = self._issue_api_request(
                vol_url, api_version='2.1', tenant=tenant)['data']
        if 'data' in app_inst:
            app_inst = app_inst['data']
        sis = app_inst['storage_instances']
        found_si = None
        for si in sis:
            if si['name'] == si_name:
                found_si = si
                break
        found_vol = None
        for vol in found_si['volumes']:
            if vol['name'] == vol_name:
                found_vol = vol
        size = found_vol['size']
        return size

    # =========================
    # = Get Manageable Volume =
    # =========================

    def _get_manageable_volumes_2_1(self, cinder_volumes, marker, limit,
                                    offset, sort_keys, sort_dirs):
        # Use the first volume to determine the tenant we're working under
        if cinder_volumes:
            tenant = self._create_tenant(cinder_volumes[0])
        else:
            tenant = None
        LOG.debug("Listing manageable Datera volumes")
        app_instances = self._issue_api_request(
            datc.URL_TEMPLATES['ai'](), api_version='2.1',
            tenant=tenant)['data']

        results = []

        cinder_volume_ids = [vol['id'] for vol in cinder_volumes]

        for ai in app_instances:
            ai_name = ai['name']
            reference = None
            size = None
            safe_to_manage = False
            reason_not_safe = ""
            cinder_id = None
            extra_info = None
            if re.match(datc.UUID4_RE, ai_name):
                cinder_id = ai_name.lstrip(datc.OS_PREFIX)
            if (not cinder_id and
                    ai_name.lstrip(datc.OS_PREFIX) not in cinder_volume_ids):
                safe_to_manage, reason_not_safe = self._is_manageable_2_1(ai)
            if safe_to_manage:
                si = list(ai['storage_instances'].values())[0]
                si_name = si['name']
                vol = list(si['volumes'].values())[0]
                vol_name = vol['name']
                size = vol['size']
                reference = {"source-name": "{}:{}:{}".format(
                    ai_name, si_name, vol_name)}

            results.append({
                'reference': reference,
                'size': size,
                'safe_to_manage': safe_to_manage,
                'reason_not_safe': reason_not_safe,
                'cinder_id': cinder_id,
                'extra_info': extra_info})

        page_results = volutils.paginate_entries_list(
            results, marker, limit, offset, sort_keys, sort_dirs)

        return page_results

    def _is_manageable_2_1(self, app_inst):
        if len(app_inst['storage_instances']) == 1:
            si = list(app_inst['storage_instances'].values())[0]
            if len(si['volumes']) == 1:
                return (True, "")
        return (False,
                "App Instance has more than one storage instance or volume")
    # ============
    # = Unmanage =
    # ============

    def _unmanage_2_1(self, volume):
        tenant = self._create_tenant(volume)
        LOG.debug("Unmanaging Cinder volume %s.  Changing name to %s",
                  volume['id'], datc._get_unmanaged(volume['id']))
        data = {'name': datc._get_unmanaged(volume['id'])}
        self._issue_api_request(datc.URL_TEMPLATES['ai_inst']().format(
            datc._get_name(volume['id'])),
            method='put',
            body=data,
            api_version='2.1',
            tenant=tenant)

    # ================
    # = Volume Stats =
    # ================

    # =========
    # = Login =
    # =========

    # ===========
    # = Tenancy =
    # ===========

    def _create_tenant(self, volume=None):
        # Create the Datera tenant if specified in the config
        # Otherwise use the tenant provided
        if self.tenant_id is None:
            tenant = None
        elif self.tenant_id.lower() == "map" and volume:
            # Convert dashless uuid to uuid with dashes
            # Eg: 0e33e95a9b154d348c675a1d8ea5b651 -->
            #       0e33e95a-9b15-4d34-8c67-5a1d8ea5b651
            tenant = datc._get_name(str(uuid.UUID(volume["project_id"])))
        elif self.tenant_id.lower() == "map" and not volume:
            tenant = None
        else:
            tenant = self.tenant_id

        if tenant:
            params = {'name': tenant}
            self._issue_api_request(
                'tenants', method='post', body=params, conflict_ok=True,
                api_version='2.1')
        return tenant

    # =========
    # = Login =
    # =========

    def _login_2_1(self):
        """Use the san_login and san_password to set token."""
        body = {
            'name': self.username,
            'password': self.password
        }

        # Unset token now, otherwise potential expired token will be sent
        # along to be used for authorization when trying to login.
        self.datera_api_token = None

        try:
            LOG.debug('Getting Datera auth token.')
            results = self._issue_api_request(
                'login', 'put', body=body, sensitive=True, api_version='2.1',
                tenant=None)
            self.datera_api_token = results['key']
        except exception.NotAuthorized:
            with excutils.save_and_reraise_exception():
                LOG.error('Logging into the Datera cluster failed. Please '
                          'check your username and password set in the '
                          'cinder.conf and start the cinder-volume '
                          'service again.')

    # ===========
    # = Polling =
    # ===========

    def _snap_poll_2_1(self, url, tenant):
        eventlet.sleep(datc.DEFAULT_SNAP_SLEEP)
        TIMEOUT = 20
        retry = 0
        poll = True
        while poll and retry < TIMEOUT:
            retry += 1
            snap = self._issue_api_request(url,
                                           api_version='2.1',
                                           tenant=tenant)['data']
            if snap['op_state'] == 'available':
                poll = False
            else:
                eventlet.sleep(1)
        if retry >= TIMEOUT:
            raise exception.VolumeDriverException(
                message=_('Snapshot not ready.'))

    def _si_poll_2_1(self, volume, policies, tenant):
        # Initial 4 second sleep required for some Datera versions
        eventlet.sleep(datc.DEFAULT_SI_SLEEP)
        TIMEOUT = 10
        retry = 0
        check_url = datc.URL_TEMPLATES['si_inst'](
            policies['default_storage_name']).format(
                datc._get_name(volume['id']))
        poll = True
        while poll and retry < TIMEOUT:
            retry += 1
            si = self._issue_api_request(check_url,
                                         api_version='2.1',
                                         tenant=tenant)['data']
            if si['op_state'] == 'available':
                poll = False
            else:
                eventlet.sleep(1)
        if retry >= TIMEOUT:
            raise exception.VolumeDriverException(
                message=_('Resource not ready.'))

    # ================
    # = Volume Stats =
    # ================

    def _get_volume_stats_2_1(self, refresh=False):
        if refresh or not self.cluster_stats:
            try:
                LOG.debug("Updating cluster stats info.")

                results = self._issue_api_request(
                    'system', api_version='2.1')['data']

                if 'uuid' not in results:
                    LOG.error(
                        'Failed to get updated stats from Datera Cluster.')

                stats = {
                    'volume_backend_name': self.backend_name,
                    'vendor_name': 'Datera',
                    'driver_version': self.VERSION,
                    'storage_protocol': 'iSCSI',
                    'total_capacity_gb': (
                        int(results['total_capacity']) / units.Gi),
                    'free_capacity_gb': (
                        int(results['available_capacity']) / units.Gi),
                    'reserved_percentage': 0,
                    'QoS_support': True,
                }

                self.cluster_stats = stats
            except exception.DateraAPIException:
                LOG.error('Failed to get updated stats from Datera cluster.')
        return self.cluster_stats

    # =======
    # = QoS =
    # =======

    def _update_qos_2_1(self, resource, policies, tenant):
        url = datc.URL_TEMPLATES['vol_inst'](
            policies['default_storage_name'],
            policies['default_volume_name']) + '/performance_policy'
        url = url.format(datc._get_name(resource['id']))
        type_id = resource.get('volume_type_id', None)
        if type_id is not None:
            # Filter for just QOS policies in result. All of their keys
            # should end with "max"
            fpolicies = {k: int(v) for k, v in
                         policies.items() if k.endswith("max")}
            # Filter all 0 values from being passed
            fpolicies = dict(filter(lambda _v: _v[1] > 0, fpolicies.items()))
            if fpolicies:
                self._issue_api_request(url, 'delete', api_version='2.1',
                                        tenant=tenant)
                self._issue_api_request(url, 'post', body=fpolicies,
                                        api_version='2.1', tenant=tenant)

    # ============
    # = IP Pools =
    # ============

    def _get_ip_pool_for_string_ip_2_1(self, ip):
        """Takes a string ipaddress and return the ip_pool API object dict """
        pool = 'default'
        ip_obj = ipaddress.ip_address(six.text_type(ip))
        ip_pools = self._issue_api_request('access_network_ip_pools',
                                           api_version='2.1')
        for ipdata in ip_pools['data']:
            for adata in ipdata['network_paths']:
                if not adata.get('start_ip'):
                    continue
                pool_if = ipaddress.ip_interface(
                    "/".join((adata['start_ip'], str(adata['netmask']))))
                if ip_obj in pool_if.network:
                    pool = ipdata['name']
        return self._issue_api_request(
            "access_network_ip_pools/{}".format(pool),
            api_version='2.1')['path']
