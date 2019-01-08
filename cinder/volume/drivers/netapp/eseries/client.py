# Copyright (c) 2014 NetApp, Inc
# Copyright (c) 2014 Navneet Singh
# Copyright (c) 2015 Alex Meade
# Copyright (c) 2015 Rushil Chugh
# Copyright (c) 2015 Yogesh Kshirsagar
# Copyright (c) 2015 Jose Porrua
# Copyright (c) 2015 Michael Price
#  All Rights Reserved.
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
Client classes for web services.
"""

import copy
import json
import uuid

from oslo_log import log as logging
import requests
import six
from six.moves import urllib

from cinder import exception
from cinder.i18n import _
import cinder.utils as cinder_utils
from cinder.volume.drivers.netapp.eseries import exception as es_exception
from cinder.volume.drivers.netapp.eseries import utils
from cinder.volume.drivers.netapp import utils as na_utils


LOG = logging.getLogger(__name__)


class WebserviceClient(object):
    """Base client for NetApp Storage web services."""

    def __init__(self, scheme, host, port, service_path, username,
                 password, **kwargs):
        self._validate_params(scheme, host, port)
        self._create_endpoint(scheme, host, port, service_path)
        self._username = username
        self._password = password
        self._init_connection()

    def _validate_params(self, scheme, host, port):
        """Does some basic validation for web service params."""
        if host is None or port is None or scheme is None:
            msg = _('One of the required inputs from host, '
                    'port or scheme was not found.')
            raise exception.InvalidInput(reason=msg)
        if scheme not in ('http', 'https'):
            raise exception.InvalidInput(reason=_("Invalid transport type."))

    def _create_endpoint(self, scheme, host, port, service_path):
        """Creates end point url for the service."""
        netloc = '%s:%s' % (host, port)
        self._endpoint = urllib.parse.urlunparse((scheme, netloc, service_path,
                                                 None, None, None))

    def _init_connection(self):
        """Do client specific set up for session and connection pooling."""
        self.conn = requests.Session()
        if self._username and self._password:
            self.conn.auth = (self._username, self._password)

    def invoke_service(self, method='GET', url=None, params=None, data=None,
                       headers=None, timeout=None, verify=False):
        url = url or self._endpoint
        try:
            response = self.conn.request(method, url, params, data,
                                         headers=headers, timeout=timeout,
                                         verify=verify)
        # Catching error conditions other than the perceived ones.
        # Helps propagating only known exceptions back to the caller.
        except Exception as e:
            LOG.exception("Unexpected error while invoking web service."
                          " Error - %s.", e)
            raise exception.NetAppDriverException(
                _("Invoking web service failed."))
        return response


class RestClient(WebserviceClient):
    """REST client specific to e-series storage service."""

    ID = 'id'
    WWN = 'worldWideName'
    NAME = 'label'

    ASUP_VALID_VERSION = (1, 52, 9000, 3)
    CHAP_VALID_VERSION = (1, 53, 9010, 15)
    # We need to check for both the release and the pre-release versions
    SSC_VALID_VERSIONS = ((1, 53, 9000, 1), (1, 53, 9010, 17))
    REST_1_3_VERSION = (1, 53, 9000, 1)
    REST_1_4_VERSIONS = ((1, 54, 9000, 1), (1, 54, 9090, 0))

    RESOURCE_PATHS = {
        'volumes': '/storage-systems/{system-id}/volumes',
        'volume': '/storage-systems/{system-id}/volumes/{object-id}',
        'pool_operation_progress':
            '/storage-systems/{system-id}/storage-pools/{object-id}'
            '/action-progress',
        'volume_expand':
            '/storage-systems/{system-id}/volumes/{object-id}/expand',
        'thin_volume_expand':
            '/storage-systems/{system-id}/thin-volumes/{object-id}/expand',
        'ssc_volumes': '/storage-systems/{system-id}/ssc/volumes',
        'ssc_volume': '/storage-systems/{system-id}/ssc/volumes/{object-id}',
        'snapshot_groups': '/storage-systems/{system-id}/snapshot-groups',
        'snapshot_group':
            '/storage-systems/{system-id}/snapshot-groups/{object-id}',
        'snapshot_volumes': '/storage-systems/{system-id}/snapshot-volumes',
        'snapshot_volume':
            '/storage-systems/{system-id}/snapshot-volumes/{object-id}',
        'snapshot_images': '/storage-systems/{system-id}/snapshot-images',
        'snapshot_image':
            '/storage-systems/{system-id}/snapshot-images/{object-id}',
        'cgroup':
            '/storage-systems/{system-id}/consistency-groups/{object-id}',
        'cgroups': '/storage-systems/{system-id}/consistency-groups',
        'cgroup_members':
            '/storage-systems/{system-id}/consistency-groups/{object-id}'
            '/member-volumes',
        'cgroup_member':
            '/storage-systems/{system-id}/consistency-groups/{object-id}'
            '/member-volumes/{vol-id}',
        'cgroup_snapshots':
            '/storage-systems/{system-id}/consistency-groups/{object-id}'
            '/snapshots',
        'cgroup_snapshot':
            '/storage-systems/{system-id}/consistency-groups/{object-id}'
            '/snapshots/{seq-num}',
        'cgroup_snapshots_by_seq':
            '/storage-systems/{system-id}/consistency-groups/{object-id}'
            '/snapshots/{seq-num}',
        'cgroup_cgsnap_view':
            '/storage-systems/{system-id}/consistency-groups/{object-id}'
            '/views/{seq-num}',
        'cgroup_cgsnap_views':
            '/storage-systems/{system-id}/consistency-groups/{object-id}'
            '/views/',
        'cgroup_snapshot_views':
            '/storage-systems/{system-id}/consistency-groups/{object-id}'
            '/views/{view-id}/views',
        'persistent-stores': '/storage-systems/{'
                             'system-id}/persistent-records/',
        'persistent-store': '/storage-systems/{'
                            'system-id}/persistent-records/{key}'
    }

    def __init__(self, scheme, host, port, service_path, username,
                 password, **kwargs):

        super(RestClient, self).__init__(scheme, host, port, service_path,
                                         username, password, **kwargs)

        kwargs = kwargs or {}

        self._system_id = kwargs.get('system_id')
        self._content_type = kwargs.get('content_type') or 'json'

    def _init_features(self):
        """Sets up and initializes E-Series feature support map."""
        self.features = na_utils.Features()
        self.api_operating_mode, self.api_version = self.get_eseries_api_info(
            verify=False)

        api_version_tuple = tuple(int(version)
                                  for version in self.api_version.split('.'))

        chap_valid_version = self._validate_version(
            self.CHAP_VALID_VERSION, api_version_tuple)
        self.features.add_feature('CHAP_AUTHENTICATION',
                                  supported=chap_valid_version,
                                  min_version=self._version_tuple_to_str(
                                      self.CHAP_VALID_VERSION))

        asup_api_valid_version = self._validate_version(
            self.ASUP_VALID_VERSION, api_version_tuple)
        self.features.add_feature('AUTOSUPPORT',
                                  supported=asup_api_valid_version,
                                  min_version=self._version_tuple_to_str(
                                      self.ASUP_VALID_VERSION))

        rest_1_3_api_valid_version = self._validate_version(
            self.REST_1_3_VERSION, api_version_tuple)

        rest_1_4_api_valid_version = any(
            self._validate_version(valid_version, api_version_tuple)
            for valid_version in self.REST_1_4_VERSIONS)

        ssc_api_valid_version = any(self._validate_version(valid_version,
                                                           api_version_tuple)
                                    for valid_version
                                    in self.SSC_VALID_VERSIONS)
        self.features.add_feature('SSC_API_V2',
                                  supported=ssc_api_valid_version,
                                  min_version=self._version_tuple_to_str(
                                      self.SSC_VALID_VERSIONS[0]))
        self.features.add_feature(
            'REST_1_3_RELEASE', supported=rest_1_3_api_valid_version,
            min_version=self._version_tuple_to_str(self.REST_1_3_VERSION))
        self.features.add_feature(
            'REST_1_4_RELEASE', supported=rest_1_4_api_valid_version,
            min_version=self._version_tuple_to_str(self.REST_1_4_VERSIONS[0]))

    def _version_tuple_to_str(self, version):
        return ".".join([str(part) for part in version])

    def _validate_version(self, version, actual_version):
        """Determine if version is newer than, or equal to the actual version

        The proxy version number is formatted as AA.BB.CCCC.DDDD
        A: Major version part 1
        B: Major version part 2
        C: Release version: 9000->Release, 9010->Pre-release, 9090->Integration
        D: Minor version

        Examples:
        02.53.9000.0010
        02.52.9010.0001

        Note: The build version is actually 'newer' the lower the release
        (CCCC) number is.

        :param version: The version to validate
        :param actual_version: The running version of the Webservice
        :returns: True if the actual_version is equal or newer than the
        current running version, otherwise False
        """
        major_1, major_2, release, minor = version
        actual_major_1, actual_major_2, actual_release, actual_minor = (
            actual_version)

        # We need to invert the release number for it to work with this
        # comparison
        return (actual_major_1, actual_major_2, 10000 - actual_release,
                actual_minor) >= (major_1, major_2, 10000 - release, minor)

    def set_system_id(self, system_id):
        """Set the storage system id."""
        self._system_id = system_id

    def get_system_id(self):
        """Get the storage system id."""
        return getattr(self, '_system_id', None)

    def _get_resource_url(self, path, use_system=True, **kwargs):
        """Creates end point url for rest service."""
        kwargs = kwargs or {}
        if use_system:
            if not self._system_id:
                raise exception.NotFound(_('Storage system id not set.'))
            kwargs['system-id'] = self._system_id
        path = path.format(**kwargs)
        if not self._endpoint.endswith('/'):
            self._endpoint = '%s/' % self._endpoint
        return urllib.parse.urljoin(self._endpoint, path.lstrip('/'))

    def _invoke(self, method, path, data=None, use_system=True,
                timeout=None, verify=False, **kwargs):
        """Invokes end point for resource on path."""
        url = self._get_resource_url(path, use_system, **kwargs)
        if self._content_type == 'json':
            headers = {'Accept': 'application/json',
                       'Content-Type': 'application/json'}
            if cinder_utils.TRACE_API:
                self._log_http_request(method, url, headers, data)
            data = json.dumps(data) if data else None
            res = self.invoke_service(method, url, data=data,
                                      headers=headers,
                                      timeout=timeout, verify=verify)

            try:
                res_dict = res.json() if res.text else None
            # This should only occur if we expected JSON, but were sent
            # something else
            except ValueError:
                res_dict = None

            if cinder_utils.TRACE_API:
                self._log_http_response(res.status_code, dict(res.headers),
                                        res_dict)

            self._eval_response(res)
            return res_dict
        else:
            raise exception.NetAppDriverException(
                _("Content type not supported."))

    def _to_pretty_dict_string(self, data):
        """Convert specified dict to pretty printed string."""
        return json.dumps(data, sort_keys=True,
                          indent=2, separators=(',', ': '))

    def _log_http_request(self, verb, url, headers, body):
        scrubbed_body = copy.deepcopy(body)
        if scrubbed_body:
            if 'password' in scrubbed_body:
                scrubbed_body['password'] = "****"
            if 'storedPassword' in scrubbed_body:
                scrubbed_body['storedPassword'] = "****"

        params = {'verb': verb, 'path': url,
                  'body': self._to_pretty_dict_string(scrubbed_body) or "",
                  'headers': self._to_pretty_dict_string(headers)}
        LOG.debug("Invoking ESeries Rest API, Request:\n"
                  "HTTP Verb: %(verb)s\n"
                  "URL Path: %(path)s\n"
                  "HTTP Headers:\n"
                  "%(headers)s\n"
                  "Body:\n"
                  "%(body)s\n", (params))

    def _log_http_response(self, status, headers, body):
        params = {'status': status,
                  'body': self._to_pretty_dict_string(body) or "",
                  'headers': self._to_pretty_dict_string(headers)}
        LOG.debug("ESeries Rest API, Response:\n"
                  "HTTP Status Code: %(status)s\n"
                  "HTTP Headers:\n"
                  "%(headers)s\n"
                  "Body:\n"
                  "%(body)s\n", (params))

    def _eval_response(self, response):
        """Evaluates response before passing result to invoker."""
        status_code = int(response.status_code)
        # codes >= 300 are not ok and to be treated as errors
        if status_code >= 300:
            # Response code 422 returns error code and message
            if status_code == 422:
                msg = _("Response error - %s.") % response.text
                json_response = response.json()
                if json_response is not None:
                    ret_code = json_response.get('retcode', '')
                    if ret_code == '30' or ret_code == 'authFailPassword':
                        msg = _("The storage array password for %s is "
                                "incorrect, please update the configured "
                                "password.") % self._system_id
            elif status_code == 424:
                msg = _("Response error - The storage-system is offline.")
            else:
                msg = _("Response error code - %s.") % status_code
            raise es_exception.WebServiceException(msg,
                                                   status_code=status_code)

    def _get_volume_api_path(self, path_key):
        """Retrieve the correct API path based on API availability

        :param path_key: The volume API to request (volume or volumes)
        :raise KeyError: If the path_key is not valid
        """
        if self.features.SSC_API_V2:
            path_key = 'ssc_' + path_key
        return self.RESOURCE_PATHS[path_key]

    def create_volume(self, pool, label, size, unit='gb', seg_size=0,
                      read_cache=None, write_cache=None, flash_cache=None,
                      data_assurance=None, thin_provision=False):
        """Creates a volume on array with the configured attributes

        Note: if read_cache, write_cache, flash_cache, or data_assurance
        are not provided, the default will be utilized by the Webservice.

        :param pool: The pool unique identifier
        :param label: The unqiue label for the volume
        :param size: The capacity in units
        :param unit: The unit for capacity
        :param seg_size: The segment size for the volume, expressed in KB.
                         Default will allow the Webservice to choose.
        :param read_cache: If true, enable read caching, if false,
                           explicitly disable it.
        :param write_cache: If true, enable write caching, if false,
                            explicitly disable it.
        :param flash_cache: If true, add the volume to a Flash Cache
        :param data_assurance: If true, enable the Data Assurance capability
        :returns: The created volume
        """

        # Utilize the new API if it is available
        if self.features.SSC_API_V2:
            path = "/storage-systems/{system-id}/ssc/volumes"
            data = {'poolId': pool, 'name': label, 'sizeUnit': unit,
                    'size': int(size), 'dataAssuranceEnable': data_assurance,
                    'flashCacheEnable': flash_cache,
                    'readCacheEnable': read_cache,
                    'writeCacheEnable': write_cache,
                    'thinProvision': thin_provision}
        # Use the old API
        else:
            # Determine if there are were extra specs provided that are not
            # supported
            extra_specs = [read_cache, write_cache]
            unsupported_spec = any([spec is not None for spec in extra_specs])
            if(unsupported_spec):
                msg = _("E-series proxy API version %(current_version)s does "
                        "not support full set of SSC extra specs. The proxy"
                        " version must be at at least %(min_version)s.")
                min_version = self.features.SSC_API_V2.minimum_version
                raise exception.NetAppDriverException(msg %
                                                      {'current_version':
                                                       self.api_version,
                                                       'min_version':
                                                       min_version})

            path = "/storage-systems/{system-id}/volumes"
            data = {'poolId': pool, 'name': label, 'sizeUnit': unit,
                    'size': int(size), 'segSize': seg_size}
        return self._invoke('POST', path, data)

    def delete_volume(self, object_id):
        """Deletes given volume from array."""
        if self.features.SSC_API_V2:
            path = self.RESOURCE_PATHS.get('ssc_volume')
        else:
            path = self.RESOURCE_PATHS.get('volume')
        return self._invoke('DELETE', path, **{'object-id': object_id})

    def list_volumes(self):
        """Lists all volumes in storage array."""
        if self.features.SSC_API_V2:
            path = self.RESOURCE_PATHS.get('ssc_volumes')
        else:
            path = self.RESOURCE_PATHS.get('volumes')

        return self._invoke('GET', path)

    def list_volume(self, object_id):
        """Retrieve the given volume from array.

        :param object_id: The volume id, label, or wwn
        :returns: The volume identified by object_id
        :raise: VolumeNotFound if the volume could not be found
        """

        if self.features.SSC_API_V2:
            return self._list_volume_v2(object_id)
        # The new API is not available,
        else:
            # Search for the volume with label, id, or wwn.
            return self._list_volume_v1(object_id)

    def _list_volume_v1(self, object_id):
        # Search for the volume with label, id, or wwn.
        for vol in self.list_volumes():
            if (object_id == vol.get(self.NAME) or object_id == vol.get(
                    self.WWN) or object_id == vol.get(self.ID)):
                return vol
        # The volume could not be found
        raise exception.VolumeNotFound(volume_id=object_id)

    def _list_volume_v2(self, object_id):
        path = self.RESOURCE_PATHS.get('ssc_volume')
        try:
            return self._invoke('GET', path, **{'object-id': object_id})
        except es_exception.WebServiceException as e:
            if 404 == e.status_code:
                raise exception.VolumeNotFound(volume_id=object_id)
            else:
                raise

    def update_volume(self, object_id, label):
        """Renames given volume on array."""
        if self.features.SSC_API_V2:
            path = self.RESOURCE_PATHS.get('ssc_volume')
        else:
            path = self.RESOURCE_PATHS.get('volume')
        data = {'name': label}
        return self._invoke('POST', path, data, **{'object-id': object_id})

    def create_consistency_group(self, name, warn_at_percent_full=75,
                                 rollback_priority='medium',
                                 full_policy='failbasewrites'):
        """Define a new consistency group"""
        path = self.RESOURCE_PATHS.get('cgroups')
        data = {
            'name': name,
            'fullWarnThresholdPercent': warn_at_percent_full,
            'repositoryFullPolicy': full_policy,
            # A non-zero threshold enables auto-deletion
            'autoDeleteThreshold': 0,
            'rollbackPriority': rollback_priority,
        }

        return self._invoke('POST', path, data)

    def get_consistency_group(self, object_id):
        """Retrieve the consistency group identified by object_id"""
        path = self.RESOURCE_PATHS.get('cgroup')

        return self._invoke('GET', path, **{'object-id': object_id})

    def list_consistency_groups(self):
        """Retrieve all consistency groups defined on the array"""
        path = self.RESOURCE_PATHS.get('cgroups')

        return self._invoke('GET', path)

    def delete_consistency_group(self, object_id):
        path = self.RESOURCE_PATHS.get('cgroup')

        self._invoke('DELETE', path, **{'object-id': object_id})

    def add_consistency_group_member(self, volume_id, cg_id,
                                     repo_percent=20.0):
        """Add a volume to a consistency group

        :param volume_id the eseries volume id
        :param cg_id: the eseries cg id
        :param repo_percent: percentage capacity of the volume to use for
        capacity of the copy-on-write repository
        """
        path = self.RESOURCE_PATHS.get('cgroup_members')
        data = {'volumeId': volume_id, 'repositoryPercent': repo_percent}

        return self._invoke('POST', path, data, **{'object-id': cg_id})

    def remove_consistency_group_member(self, volume_id, cg_id):
        """Remove a volume from a consistency group"""
        path = self.RESOURCE_PATHS.get('cgroup_member')

        self._invoke('DELETE', path, **{'object-id': cg_id,
                                        'vol-id': volume_id})

    def create_consistency_group_snapshot(self, cg_id):
        """Define a consistency group snapshot"""
        path = self.RESOURCE_PATHS.get('cgroup_snapshots')

        return self._invoke('POST', path, **{'object-id': cg_id})

    def delete_consistency_group_snapshot(self, cg_id, seq_num):
        """Define a consistency group snapshot"""
        path = self.RESOURCE_PATHS.get('cgroup_snapshot')

        return self._invoke('DELETE', path, **{'object-id': cg_id,
                                               'seq-num': seq_num})

    def get_consistency_group_snapshots(self, cg_id):
        """Retrieve all snapshots defined for a consistency group"""
        path = self.RESOURCE_PATHS.get('cgroup_snapshots')

        return self._invoke('GET', path, **{'object-id': cg_id})

    def create_cg_snapshot_view(self, cg_id, name, snap_id):
        """Define a snapshot view for the cgsnapshot

        In order to define a snapshot view for a snapshot defined under a
        consistency group, the view must be defined at the cgsnapshot
        level.

        :param cg_id: E-Series cg identifier
        :param name: the label for the view
        :param snap_id: E-Series snapshot view to locate
        :raise NetAppDriverException: if the snapshot view cannot be
                                      located for the snapshot identified
                                      by snap_id
        :return: snapshot view for snapshot identified by snap_id
        """
        path = self.RESOURCE_PATHS.get('cgroup_cgsnap_views')

        data = {
            'name': name,
            'accessMode': 'readOnly',
            # Only define a view for this snapshot
            'pitId': snap_id,
        }
        # Define a view for the cgsnapshot
        cgsnapshot_view = self._invoke(
            'POST', path, data, **{'object-id': cg_id})

        # Retrieve the snapshot views associated with our cgsnapshot view
        views = self.list_cg_snapshot_views(cg_id, cgsnapshot_view[
            'cgViewRef'])
        # Find the snapshot view defined for our snapshot
        for view in views:
            if view['basePIT'] == snap_id:
                return view
        else:
            try:
                self.delete_cg_snapshot_view(cg_id, cgsnapshot_view['id'])
            finally:
                raise exception.NetAppDriverException(
                    'Unable to create snapshot view.')

    def list_cg_snapshot_views(self, cg_id, view_id):
        path = self.RESOURCE_PATHS.get('cgroup_snapshot_views')

        return self._invoke('GET', path, **{'object-id': cg_id,
                                            'view-id': view_id})

    def delete_cg_snapshot_view(self, cg_id, view_id):
        path = self.RESOURCE_PATHS.get('cgroup_snap_view')

        return self._invoke('DELETE', path, **{'object-id': cg_id,
                                               'view-id': view_id})

    def get_pool_operation_progress(self, object_id):
        """Retrieve the progress long-running operations on a storage pool

        Example:

        .. code-block:: python

          [
              {
                  "volumeRef": "3232....", # Volume being referenced
                  "progressPercentage": 0, # Approxmate percent complete
                  "estimatedTimeToCompletion": 0, # ETA in minutes
                  "currentAction": "none" # Current volume action
              }
              ...
          ]

        :param object_id: A pool id
        :returns: A dict representing the action progress
        """
        path = self.RESOURCE_PATHS.get('pool_operation_progress')
        return self._invoke('GET', path, **{'object-id': object_id})

    def expand_volume(self, object_id, new_capacity, thin_provisioned,
                      capacity_unit='gb'):
        """Increase the capacity of a volume"""
        if thin_provisioned:
            path = self.RESOURCE_PATHS.get('thin_volume_expand')
            data = {'newVirtualSize': new_capacity, 'sizeUnit': capacity_unit,
                    'newRepositorySize': new_capacity}
            return self._invoke('POST', path, data, **{'object-id': object_id})
        else:
            path = self.RESOURCE_PATHS.get('volume_expand')
            data = {'expansionSize': new_capacity, 'sizeUnit': capacity_unit}
            return self._invoke('POST', path, data, **{'object-id': object_id})

    def get_volume_mappings(self):
        """Creates volume mapping on array."""
        path = "/storage-systems/{system-id}/volume-mappings"
        return self._invoke('GET', path)

    def get_volume_mappings_for_volume(self, volume):
        """Gets all host mappings for given volume from array."""
        mappings = self.get_volume_mappings() or []
        return [x for x in mappings
                if x.get('volumeRef') == volume['volumeRef']]

    def get_volume_mappings_for_host(self, host_ref):
        """Gets all volume mappings for given host from array."""
        mappings = self.get_volume_mappings() or []
        return [x for x in mappings if x.get('mapRef') == host_ref]

    def get_volume_mappings_for_host_group(self, hg_ref):
        """Gets all volume mappings for given host group from array."""
        mappings = self.get_volume_mappings() or []
        return [x for x in mappings if x.get('mapRef') == hg_ref]

    def create_volume_mapping(self, object_id, target_id, lun):
        """Creates volume mapping on array."""
        path = "/storage-systems/{system-id}/volume-mappings"
        data = {'mappableObjectId': object_id, 'targetId': target_id,
                'lun': lun}
        return self._invoke('POST', path, data)

    def delete_volume_mapping(self, map_object_id):
        """Deletes given volume mapping from array."""
        path = "/storage-systems/{system-id}/volume-mappings/{object-id}"
        return self._invoke('DELETE', path, **{'object-id': map_object_id})

    def move_volume_mapping_via_symbol(self, map_ref, to_ref, lun_id):
        """Moves a map from one host/host_group object to another."""

        path = "/storage-systems/{system-id}/symbol/moveLUNMapping"
        data = {'lunMappingRef': map_ref,
                'lun': int(lun_id),
                'mapRef': to_ref}
        return_code = self._invoke('POST', path, data)
        if return_code == 'ok':
            return {'lun': lun_id}
        msg = _("Failed to move LUN mapping.  Return code: %s") % return_code
        raise exception.NetAppDriverException(msg)

    def list_hardware_inventory(self):
        """Lists objects in the hardware inventory."""
        path = "/storage-systems/{system-id}/hardware-inventory"
        return self._invoke('GET', path)

    def list_target_wwpns(self):
        """Lists the world-wide port names of the target."""
        inventory = self.list_hardware_inventory()
        fc_ports = inventory.get("fibrePorts", [])
        wwpns = [port['portName'] for port in fc_ports]
        return wwpns

    def create_host_group(self, label):
        """Creates a host group on the array."""
        path = "/storage-systems/{system-id}/host-groups"
        data = {'name': label}
        return self._invoke('POST', path, data)

    def get_host_group(self, host_group_ref):
        """Gets a single host group from the array."""
        path = "/storage-systems/{system-id}/host-groups/{object-id}"
        try:
            return self._invoke('GET', path, **{'object-id': host_group_ref})
        except exception.NetAppDriverException:
            raise exception.NotFound(_("Host group with ref %s not found") %
                                     host_group_ref)

    def get_host_group_by_name(self, name):
        """Gets a single host group by name from the array."""
        host_groups = self.list_host_groups()
        matching = [host_group for host_group in host_groups
                    if host_group['label'] == name]
        if len(matching):
            return matching[0]
        raise exception.NotFound(_("Host group with name %s not found") % name)

    def list_host_groups(self):
        """Lists host groups on the array."""
        path = "/storage-systems/{system-id}/host-groups"
        return self._invoke('GET', path)

    def list_hosts(self):
        """Lists host objects in the system."""
        path = "/storage-systems/{system-id}/hosts"
        return self._invoke('GET', path)

    def create_host(self, label, host_type, ports=None, group_id=None):
        """Creates host on array."""
        path = "/storage-systems/{system-id}/hosts"
        data = {'name': label, 'hostType': host_type}
        data.setdefault('groupId', group_id if group_id else None)
        data.setdefault('ports', ports if ports else None)
        return self._invoke('POST', path, data)

    def create_host_with_ports(self, label, host_type, port_ids,
                               port_type='iscsi', group_id=None):
        """Creates host on array with given port information."""
        if port_type == 'fc':
            port_ids = [six.text_type(wwpn).replace(':', '')
                        for wwpn in port_ids]
        ports = []
        for port_id in port_ids:
            port_label = utils.convert_uuid_to_es_fmt(uuid.uuid4())
            port = {'type': port_type, 'port': port_id, 'label': port_label}
            ports.append(port)
        return self.create_host(label, host_type, ports, group_id)

    def update_host(self, host_ref, data):
        """Updates host type for a given host."""
        path = "/storage-systems/{system-id}/hosts/{object-id}"
        return self._invoke('POST', path, data, **{'object-id': host_ref})

    def get_host(self, host_ref):
        """Gets a single host from the array."""
        path = "/storage-systems/{system-id}/hosts/{object-id}"
        return self._invoke('GET', path, **{'object-id': host_ref})

    def update_host_type(self, host_ref, host_type):
        """Updates host type for a given host."""
        data = {'hostType': host_type}
        return self.update_host(host_ref, data)

    def set_host_group_for_host(self, host_ref, host_group_ref=utils.NULL_REF):
        """Sets or clears which host group a host is in."""
        data = {'groupId': host_group_ref}
        self.update_host(host_ref, data)

    def list_host_types(self):
        """Lists host types in storage system."""
        path = "/storage-systems/{system-id}/host-types"
        return self._invoke('GET', path)

    def list_snapshot_groups(self):
        """Lists snapshot groups."""
        path = self.RESOURCE_PATHS['snapshot_groups']
        return self._invoke('GET', path)

    def list_snapshot_group(self, object_id):
        """Retrieve given snapshot group from the array."""
        path = self.RESOURCE_PATHS['snapshot_group']
        return self._invoke('GET', path, **{'object-id': object_id})

    def create_snapshot_group(self, label, object_id, storage_pool_id=None,
                              repo_percent=99, warn_thres=99, auto_del_limit=0,
                              full_policy='failbasewrites'):
        """Creates snapshot group on array."""
        path = self.RESOURCE_PATHS['snapshot_groups']
        data = {'baseMappableObjectId': object_id, 'name': label,
                'storagePoolId': storage_pool_id,
                'repositoryPercentage': repo_percent,
                'warningThreshold': warn_thres,
                'autoDeleteLimit': auto_del_limit, 'fullPolicy': full_policy}
        return self._invoke('POST', path, data)

    def update_snapshot_group(self, group_id, label):
        """Modify a snapshot group on the array."""
        path = self.RESOURCE_PATHS['snapshot_group']
        data = {'name': label}
        return self._invoke('POST', path, data, **{'object-id': group_id})

    def delete_snapshot_group(self, object_id):
        """Deletes given snapshot group from array."""
        path = self.RESOURCE_PATHS['snapshot_group']
        return self._invoke('DELETE', path, **{'object-id': object_id})

    def create_snapshot_image(self, group_id):
        """Creates snapshot image in snapshot group."""
        path = self.RESOURCE_PATHS['snapshot_images']
        data = {'groupId': group_id}
        return self._invoke('POST', path, data)

    def delete_snapshot_image(self, object_id):
        """Deletes given snapshot image in snapshot group."""
        path = self.RESOURCE_PATHS['snapshot_image']
        return self._invoke('DELETE', path, **{'object-id': object_id})

    def list_snapshot_image(self, object_id):
        """Retrieve given snapshot image from the array."""
        path = self.RESOURCE_PATHS['snapshot_image']
        return self._invoke('GET', path, **{'object-id': object_id})

    def list_snapshot_images(self):
        """Lists snapshot images."""
        path = self.RESOURCE_PATHS['snapshot_images']
        return self._invoke('GET', path)

    def create_snapshot_volume(self, image_id, label, base_object_id,
                               storage_pool_id=None,
                               repo_percent=99, full_thres=99,
                               view_mode='readOnly'):
        """Creates snapshot volume."""
        path = self.RESOURCE_PATHS['snapshot_volumes']
        data = {'snapshotImageId': image_id, 'fullThreshold': full_thres,
                'storagePoolId': storage_pool_id,
                'name': label, 'viewMode': view_mode,
                'repositoryPercentage': repo_percent,
                'baseMappableObjectId': base_object_id,
                'repositoryPoolId': storage_pool_id}
        return self._invoke('POST', path, data)

    def update_snapshot_volume(self, snap_vol_id, label=None, full_thres=None):
        """Modify an existing snapshot volume."""
        path = self.RESOURCE_PATHS['snapshot_volume']
        data = {'name': label, 'fullThreshold': full_thres}
        return self._invoke('POST', path, data, **{'object-id': snap_vol_id})

    def delete_snapshot_volume(self, object_id):
        """Deletes given snapshot volume."""
        path = self.RESOURCE_PATHS['snapshot_volume']
        return self._invoke('DELETE', path, **{'object-id': object_id})

    def list_snapshot_volumes(self):
        """Lists snapshot volumes/views defined on the array."""
        path = self.RESOURCE_PATHS['snapshot_volumes']
        return self._invoke('GET', path)

    def list_ssc_storage_pools(self):
        """Lists pools and their service quality defined on the array."""
        path = "/storage-systems/{system-id}/ssc/pools"
        return self._invoke('GET', path)

    def get_ssc_storage_pool(self, volume_group_ref):
        """Get storage pool service quality information from the array."""
        path = "/storage-systems/{system-id}/ssc/pools/{object-id}"
        return self._invoke('GET', path, **{'object-id': volume_group_ref})

    def list_storage_pools(self):
        """Lists storage pools in the array."""
        path = "/storage-systems/{system-id}/storage-pools"
        return self._invoke('GET', path)

    def get_storage_pool(self, volume_group_ref):
        """Get storage pool information from the array."""
        path = "/storage-systems/{system-id}/storage-pools/{object-id}"
        return self._invoke('GET', path, **{'object-id': volume_group_ref})

    def list_drives(self):
        """Lists drives in the array."""
        path = "/storage-systems/{system-id}/drives"
        return self._invoke('GET', path)

    def list_storage_systems(self):
        """Lists managed storage systems registered with web service."""
        path = "/storage-systems"
        return self._invoke('GET', path, use_system=False)

    def list_storage_system(self):
        """List current storage system registered with web service."""
        path = "/storage-systems/{system-id}"
        return self._invoke('GET', path)

    def register_storage_system(self, controller_addresses, password=None,
                                wwn=None):
        """Registers storage system with web service."""
        path = "/storage-systems"
        data = {'controllerAddresses': controller_addresses}
        data.setdefault('wwn', wwn if wwn else None)
        data.setdefault('password', password if password else None)
        return self._invoke('POST', path, data, use_system=False)

    def update_stored_system_password(self, password):
        """Update array password stored on web service."""
        path = "/storage-systems/{system-id}"
        data = {'storedPassword': password}
        return self._invoke('POST', path, data)

    def create_volume_copy_job(self, src_id, tgt_id, priority='priority4',
                               tgt_wrt_protected='true'):
        """Creates a volume copy job."""
        path = "/storage-systems/{system-id}/volume-copy-jobs"
        data = {'sourceId': src_id, 'targetId': tgt_id,
                'copyPriority': priority,
                'targetWriteProtected': tgt_wrt_protected}
        return self._invoke('POST', path, data)

    def control_volume_copy_job(self, obj_id, control='start'):
        """Controls a volume copy job."""
        path = ("/storage-systems/{system-id}/volume-copy-jobs-control"
                "/{object-id}?control={String}")
        return self._invoke('PUT', path, **{'object-id': obj_id,
                                            'String': control})

    def list_vol_copy_job(self, object_id):
        """List volume copy job."""
        path = "/storage-systems/{system-id}/volume-copy-jobs/{object-id}"
        return self._invoke('GET', path, **{'object-id': object_id})

    def delete_vol_copy_job(self, object_id):
        """Delete volume copy job."""
        path = "/storage-systems/{system-id}/volume-copy-jobs/{object-id}"
        return self._invoke('DELETE', path, **{'object-id': object_id})

    def set_chap_authentication(self, target_iqn, chap_username,
                                chap_password):
        """Configures CHAP credentials for target IQN from backend."""
        path = "/storage-systems/{system-id}/iscsi/target-settings/"
        data = {
            'iqn': target_iqn,
            'enableChapAuthentication': True,
            'alias': chap_username,
            'authMethod': 'chap',
            'chapSecret': chap_password,
        }
        return self._invoke('POST', path, data)

    def add_autosupport_data(self, key, data):
        """Register driver statistics via autosupport log."""
        path = ('/key-values/%s' % key)
        self._invoke('POST', path, json.dumps(data))

    def set_counter(self, key, value):
        path = ('/counters/%s/setCounter?value=%d' % (key, value))
        self._invoke('POST', path)

    def get_asup_info(self):
        """Returns a dictionary of relevant autosupport information.

        Currently returned fields are:
        model -- E-series model name
        serial_numbers -- Serial number for each controller
        firmware_version -- Version of active firmware
        chassis_sn -- Serial number for whole chassis
        """
        asup_info = {}

        controllers = self.list_hardware_inventory().get('controllers')
        if controllers:
            asup_info['model'] = controllers[0].get('modelName', 'unknown')
            serial_numbers = [value['serialNumber'].rstrip()
                              for __, value in enumerate(controllers)]
            serial_numbers.sort()
            for index, value in enumerate(serial_numbers):
                if not value:
                    serial_numbers[index] = 'unknown'
            asup_info['serial_numbers'] = serial_numbers
        else:
            asup_info['model'] = 'unknown'
            asup_info['serial_numbers'] = ['unknown', 'unknown']

        system_info = self.list_storage_system()
        if system_info:
            asup_info['firmware_version'] = system_info['fwVersion']
            asup_info['chassis_sn'] = system_info['chassisSerialNumber']
        else:
            asup_info['firmware_version'] = 'unknown'
            asup_info['chassis_sn'] = 'unknown'

        return asup_info

    def get_eseries_api_info(self, verify=False):
        """Get E-Series API information from the array."""
        api_operating_mode = 'embedded'
        path = 'devmgr/utils/about'
        headers = {'Content-Type': 'application/json',
                   'Accept': 'application/json'}
        url = self._get_resource_url(path, True).replace(
            '/devmgr/v2', '', 1)
        result = self.invoke_service(method='GET', url=url,
                                     headers=headers,
                                     verify=verify)
        about_response_dict = result.json()
        mode_is_proxy = about_response_dict['runningAsProxy']
        if mode_is_proxy:
            api_operating_mode = 'proxy'
        return api_operating_mode, about_response_dict['version']

    def list_backend_store(self, key):
        """Retrieve data by key from the persistent store on the backend.

        Example response: {"key": "cinder-snapshots", "value": "[]"}

        :param key: the persistent store to retrieve
        :returns: a json body representing the value of the store,
                  or an empty json object
        """
        path = self.RESOURCE_PATHS.get('persistent-store')
        try:
            resp = self._invoke('GET', path, **{'key': key})
        except exception.NetAppDriverException:
            return dict()
        else:
            data = resp['value']
            if data:
                return json.loads(data)
            return dict()

    def save_backend_store(self, key, store_data):
        """Save a json value to the persistent storage on the backend.

        The storage backend provides a small amount of persistent storage
        that we can utilize for storing driver information.

        :param key: The key utilized for storing/retrieving the data
        :param store_data: a python data structure that will be stored as a
                           json value
        """
        path = self.RESOURCE_PATHS.get('persistent-stores')
        store_data = json.dumps(store_data, separators=(',', ':'))

        data = {
            'key': key,
            'value': store_data
        }

        self._invoke('POST', path, data)
