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
import collections
import copy

from cinder.volume.drivers import linstordrv as drv

ControllerVersion = collections.namedtuple(
    'ControllerVersion', ('rest_api_version',)
)
FakeNodeList = collections.namedtuple('FakeNodeList', ('nodes',))
FakeVolumeList = collections.namedtuple('FakeVolumeList', ('resources',))
FakeResource = collections.namedtuple('FakeResource', ('volumes',))
FakeResourceList = collections.namedtuple('FakeResourceList', ('resources',))
FakeStoragePoolList = collections.namedtuple(
    'FakeStoragePoolList', ('storage_pools',)
)
FakeResourceDefinitionList = collections.namedtuple(
    'FakeResourceDefinitionList', ('resource_definitions',)
)
FakeResourceGroupList = collections.namedtuple(
    'FakeResourceGroupList', ('resource_groups',)
)


class FakeVolume(object):
    def __init__(self, device_path, properties, size):
        self.device_path = device_path
        self.properties = properties
        self.size = int(size)

    def __repr__(self):
        return repr(self.__dict__)

    def __eq__(self, other):
        return self.device_path == other.device_path \
            and self.size == other.size


class MultiLinstor(object):
    def __init__(self, nodes, resources, version, uri_list, timeout=None):
        self.__nodes = nodes
        self.__resources = resources
        self.__version = version
        self.uri_list = uri_list
        self._timeout = timeout

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def controller_version(self):
        return self.__version

    def node_list_raise(self, filter_by_nodes=None):
        return FakeNodeList([
            node for node in self.__nodes
            if not filter_by_nodes or node in filter_by_nodes
        ])

    def storage_pool_list_raise(self):
        return FakeStoragePoolList([])

    def resource_dfn_list_raise(self):
        return FakeResourceDefinitionList([])

    def resource_list_raise(self):
        return FakeResourceList([])

    def resource_group_list_raise(self):
        return FakeResourceGroupList([])

    def volume_list_raise(self, filter_by_resources, filter_by_nodes):
        if len(filter_by_resources) != 1:
            raise NotImplementedError()
        if len(filter_by_nodes) != 1:
            raise NotImplementedError()
        rscname = filter_by_resources[0]
        nodename = filter_by_nodes[0]
        if rscname not in self.__resources:
            return FakeVolumeList([])
        rsc = self.__resources[rscname]
        if nodename not in rsc['nodes']:
            return FakeVolumeList([])
        return FakeVolumeList([FakeResource(rsc['volumes'])])


class LinstorError(Exception):
    pass


class _Placement(object):
    def __init__(self, storage_pool=None, autoplace=None):
        self.storage_pool = storage_pool
        self.autoplace = autoplace


class _VolumeDictShim(object):
    def __init__(self, resources, d):
        self.__resources = resources
        self._d = d

    def __getitem__(self, item):
        return self._d[item]

    def __setitem__(self, key, value):
        if isinstance(value, int):
            device_paths = (
                '/dev/drbd%4d' % x for x in range(1000, 2000)
            )
            existing_paths = set(
                (v.device_path for r in self.__resources.values()
                 for v in r['volumes'].values())
            )
            free_device = next(
                dpath for dpath in device_paths
                if dpath not in existing_paths
            )
            self._d[key] = FakeVolume(free_device, {}, value)
        else:
            self._d[key] = value


class Resource(object):
    def __init__(self, nodes, resources, name, uri=None, existing_client=None):
        self.__nodes = nodes
        self.__resources = resources
        self.name = name
        self.existing = resources.get(name, {'nodes': {}})
        self.volumes = _VolumeDictShim(
            self.__resources,
            self.existing.setdefault('volumes', {})
        )
        self.placement = self.existing.get(
            'placement', _Placement()
        )
        self.client = MultiLinstor(nodes, resources, None, uri)

    @property
    def defined(self):
        return self.name in self.__resources

    @property
    def resource_group_name(self):
        return self.existing.get('resource_group_name', 'DfltRscGrp')

    def activate(self, nodename):
        if nodename not in self.__nodes:
            raise LinstorError()
        if nodename in self.existing['nodes']:
            return
        if self.name not in self.__resources:
            self.__resources[self.name] = self.existing
        self.existing['nodes'][nodename] = False

    def deactivate(self, nodename):
        if nodename not in self.__nodes:
            raise LinstorError()
        if self.existing['nodes'].get(nodename, True):
            return
        del self.existing['nodes'][nodename]

    @property
    def allow_two_primaries(self):
        return self.existing.get('allow_two_primaries', False)

    @allow_two_primaries.setter
    def allow_two_primaries(self, value):
        self.existing['allow_two_primaries'] = value

    def autoplace(self):
        n_replicas = self.placement.autoplace or 3
        self.existing = {
            'nodes': sorted(self.__nodes)[:n_replicas],
            'volumes': self.volumes,
            'allow_two_primaries': self.allow_two_primaries
        }
        self.__resources[self.name] = self.existing

    def restore_from_snapshot(self, snapname, restored_name):
        if snapname not in self.existing.get('snapshots', {}):
            raise LinstorError()
        self.__resources[restored_name] = copy.deepcopy(
            self.existing['snapshots'][snapname]
        )
        r = Resource(self.__nodes, self.__resources, restored_name)
        r.volumes[0] = r.volumes[0].size
        return r

    def delete(self, snapshots=True):
        if not snapshots and self.existing.get('snapshots'):
            raise LinstorError()
        if self.name in self.__resources:
            del self.__resources[self.name]

    def snapshot_delete(self, snapname):
        snaps = self.existing.get('snapshots', {})
        if snapname in snaps:
            del snaps[snapname]

    def snapshot_create(self, snapname):
        snaps = self.existing.get('snapshots', {})
        snap = copy.deepcopy(self.existing)
        snap['snapshots'] = {}
        snaps[snapname] = snap
        self.existing['snapshots'] = snaps

    def snapshot_rollback(self, snapname):
        snaps = self.existing.get('snapshots', {})
        if snapname not in snaps:
            raise LinstorError()
        self.existing = snaps[snapname]
        self.existing['snapshots'] = snaps
        self.volumes[0] = snaps[snapname]['volumes'][0].size


class ResourceGroup(object):
    def __init__(self, resources, resource_groups, name, existing_client=None):
        self.__dict__['resources'] = resources
        self.__dict__['existing'] = resource_groups
        self.__dict__['name'] = name
        if name not in resource_groups:
            resource_groups[name] = {
                'nr_volumes': 1,
                'property_dict': {}
            }

    def delete(self):
        name = self.__dict__['name']
        for k, v in self.__dict__['resources'].items():
            if v.get('resource_group_name') == name:
                raise LinstorError()

        if name in self.__dict__['existing']:
            del self.__dict__['existing'][name]

    def __getattr__(self, item):
        name = self.__dict__['name']
        return self.__dict__['existing'][name].get(item)

    def __setattr__(self, key, value):
        name = self.__dict__['name']
        self.__dict__['existing'][name][key] = value


class SizeCalc(object):
    UNIT_KiB = "KiB"
    UNIT_GiB = "GiB"

    @classmethod
    def convert_round_up(cls, kib, src, dest):
        if src != cls.UNIT_KiB or dest != cls.UNIT_GiB:
            raise ValueError("Unsupported mock")
        return kib >> 20


class FakeLinstorClientGetter(drv.ThreadSafeLinstorClient):
    def __init__(self, linstor):
        super(FakeLinstorClientGetter, self).__init__(None)
        self.linstor = linstor

    def get(self):
        return self.linstor


def make_multilinstor(nodes, resources, version):
    def f(uri_list, timeout=0):
        return MultiLinstor(
            nodes, resources, version, uri_list, timeout,
        )
    return f


def make_resource(nodes, resources):
    def f(name, uri=None, existing_client=None):
        return Resource(
            nodes, resources, name, uri, existing_client,
        )
    def from_rg(uri, resource_group_name, resource_name, vlm_sizes,
                existing_client):
        r = Resource(
            nodes, resources, resource_name, uri, existing_client,
        )
        for i, v in enumerate(vlm_sizes):
            r.volumes[i] = v * 1024
        r.autoplace()
        return r

    f.from_resource_group = from_rg
    return f


def make_resource_group(resources, resource_groups):
    def f(name, existing_client=None):
        return ResourceGroup(resources, resource_groups, name, existing_client)
    return f


class FakeLinstorMod(object):
    def __init__(self, nodes, resources, resource_groups, controller_version):
        self.resources = resources
        self.resource_groups = resource_groups
        self.LinstorError = LinstorError
        self.Volume = int
        self.SizeCalc = SizeCalc
        self.MultiLinstor = make_multilinstor(
            nodes, resources, controller_version,
        )
        self.ResourceGroup = make_resource_group(resources, resource_groups)
        self.Resource = make_resource(nodes, resources)
