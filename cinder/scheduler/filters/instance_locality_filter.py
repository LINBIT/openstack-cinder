# -*- coding: utf-8 -*-
# Copyright 2014, Adrien Vergé <adrien.verge@numergy.com>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from oslo_log import log as logging
from oslo_utils import uuidutils

from cinder.compute import nova
from cinder import exception
from cinder.i18n import _
from cinder.scheduler import filters
from cinder.volume import volume_utils


LOG = logging.getLogger(__name__)

HINT_KEYWORD = 'local_to_instance'
INSTANCE_HOST_PROP = 'OS-EXT-SRV-ATTR:host'
REQUESTS_TIMEOUT = 5


class InstanceLocalityFilter(filters.BaseBackendFilter):
    """Schedule volume on the same host as a given instance.

    This filter enables selection of a storage back-end located on the host
    where the instance's hypervisor is running. This provides data locality:
    the instance and the volume are located on the same physical machine.

    In order to work:

    - The Extended Server Attributes extension needs to be active in Nova (this
      is by default), so that the 'OS-EXT-SRV-ATTR:host' property is returned
      when requesting instance info.
    - Either an account with privileged rights for Nova must be configured in
      Cinder configuration (configure a keystone authentication plugin in the
      [nova] section), or the user making the call needs to have sufficient
      rights (see 'extended_server_attributes' in Nova policy).

    """

    def __init__(self):
        # Cache Nova API answers directly into the Filter object.
        # Since a BaseBackendFilter instance lives only during the volume's
        # scheduling, the cache is re-created for every new volume creation.
        self._cache = {}
        super(InstanceLocalityFilter, self).__init__()

    def backend_passes(self, backend_state, filter_properties):
        context = filter_properties['context']
        backend = volume_utils.extract_host(backend_state.backend_id, 'host')

        scheduler_hints = filter_properties.get('scheduler_hints') or {}
        instance_uuid = scheduler_hints.get(HINT_KEYWORD, None)

        # Without 'local_to_instance' hint
        if not instance_uuid:
            return True

        if not uuidutils.is_uuid_like(instance_uuid):
            raise exception.InvalidUUID(uuid=instance_uuid)

        # TODO(adrienverge): Currently it is not recommended to allow instance
        # migrations for hypervisors where this hint will be used. In case of
        # instance migration, a previously locally-created volume will not be
        # automatically migrated. Also in case of instance migration during the
        # volume's scheduling, the result is unpredictable. A future
        # enhancement would be to subscribe to Nova migration events (e.g. via
        # Ceilometer).

        # First, lookup for already-known information in local cache
        if instance_uuid in self._cache:
            return self._cache[instance_uuid] == backend

        server = nova.API().get_server(context, instance_uuid,
                                       privileged_user=True,
                                       timeout=REQUESTS_TIMEOUT)

        if not hasattr(server, INSTANCE_HOST_PROP):
            LOG.warning('Hint "%s" dropped because Nova did not return '
                        'enough information. Either Nova policy needs to '
                        'be changed or a privileged account for Nova '
                        'should be specified in conf.', HINT_KEYWORD)
            raise exception.CinderException(_('Hint "%s" not supported.') %
                                            HINT_KEYWORD)

        self._cache[instance_uuid] = getattr(server, INSTANCE_HOST_PROP)

        # Match if given instance is hosted on backend
        return self._cache[instance_uuid] == backend
