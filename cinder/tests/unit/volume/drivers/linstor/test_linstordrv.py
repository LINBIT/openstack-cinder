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
import copy
from unittest import mock

from oslo_config import cfg

from cinder import exception
from cinder import test
from cinder.tests.unit.volume.drivers.linstor import fake_linstor
from cinder.volume import configuration
from cinder.volume.drivers import linstordrv as drv

CONF = cfg.CONF

KNOWN_NODES = {
    'test-1',
    'test-2',
    'test-3',
    'test-4',
    'test-5',
}

BASIC_VOLUME_PROPS = {
    'Satellite/Device/Symlinks/0': '/dev/symlink0',
    'Satellite/Device/Symlinks/1': '/dev/drbd/by-res/basic-volume/0',
}

EXISTING_RESOURCE_GROUPS = {
    'cinder-123-456': {
        'nr_volumes': 0,
        'property_dict': {},
    }
}

GIB = 1024 * 1024 * 1024
EXISTING_RESOURCES = {
    'basic-volume': {
        'nodes': {'test-2': True},
        'resource_group_name': 'cinder-123-456',
        'volumes': {
            0: fake_linstor.FakeVolume(
                '/dev/drbd1000', BASIC_VOLUME_PROPS, 5 * GIB
            )
        },
        'allow_two_primaries': False,
        'snapshots': {
            'snap1': {
                'nodes': {'test-2': True},
                'resource_group_name': 'cinder-123-456',
                'volumes': {
                    0: fake_linstor.FakeVolume(
                        '/dev/drbd1000', BASIC_VOLUME_PROPS, 1 * GIB
                    )
                },
                'allow_two_primaries': False,
            },
            'SN_snap2': {
                'nodes': {'test-2': True},
                'resource_group_name': 'cinder-123-456',
                'volumes': {
                    0: fake_linstor.FakeVolume(
                        '/dev/drbd1000', BASIC_VOLUME_PROPS, 5 * GIB
                    )
                },
                'allow_two_primaries': False,
            },
        },
    },
    'attached-volume': {
        'nodes': {'test-2': True, 'test-1': False},
        'resource_group_name': 'cinder-123-456',
        'volumes': {0: fake_linstor.FakeVolume('/dev/drbd1002', {}, 5 * GIB)},
        'allow_two_primaries': False,
    },
    'attached-live-migration-volume': {
        'nodes': {'test-2': True, 'test-1': False},
        'resource_group_name': 'cinder-123-456',
        'volumes': {0: fake_linstor.FakeVolume('/dev/drbd1004', {}, 5 * GIB)},
        'allow_two_primaries': True,
    },
    'CV_[some-id]': {
        'nodes': {'test-2': True},
        'resource_group_name': 'cinder-123-456',
        'volumes': {0: fake_linstor.FakeVolume('/dev/drbd1001', {}, 5 * GIB)},
        'allow_two_primaries': False,
    }
}

DEFAULT_EXISTING_VOLUME_WITH_SNAP = {
    'id': 'basic-volume',
    'name': 'basic-volume',
    'size': 5,
    'host': 'test-1@fake-linstor',
    'volume_type': {
        'id': 'default',
        'name': 'default-vt',
        'extra_specs': {},
    },
}

DEFAULT_NEW_VOLUME = {
    'id': 'default-new',
    'name': 'volume-default-new',
    'size': 5,
    'host': 'test-1@fake-linstor',
    'volume_type': {
        'id': '123-456',
        'name': 'fake-vt',
        'extra_specs': {},
    },
}

DEFAULT_NEW_VOLUME2 = {
    'id': 'default-new-2',
    'name': 'volume-default-new-2',
    'size': 5,
    'host': 'test-1@fake-linstor',
    'volume_type': {
        'id': '123-456',
        'name': 'fake-vt',
        'extra_specs': {},
    },
}

DEFAULT_NEW_SNAP = {
    'id': 'snap3',
    'name': 'snap3',
    'volume_id': 'something-something',
    'volume': {
        'name': 'basic-volume',
        'id': 'something-something',
    }
}

EXISTING_SNAPSHOT = {
    'id': 'snap1',
    'name': 'snap1',
    'volume_id': 'something-something',
    'volume': {
        'name': 'basic-volume',
        'id': 'something-something',
    }
}

EXISTING_OLD_SNAPSHOT = {
    'id': 'snap2',
    'name': 'snap2',
    'volume_id': 'something-something',
    'volume': {
        'name': 'basic-volume',
        'id': 'something-something',
    }
}

BASIC_VOLUME = {
    'name': 'basic-volume',
    'id': 'basic-volume-00001',
    'status': 'detached',
    'volume_attachment': [],
}

ATTACHED_VOLUME = {
    'name': 'attached-volume',
    'id': 'attached-volume-00001',
    'status': 'in-use',
    'volume_attachment': [{'id': 1, 'attached_host': 'test-1'}],
}

LIVE_MIGRATION_VOLUME = {
    'name': 'basic-volume',
    'id': 'basic-volume-00001',
    'status': 'in-use',
    'volume_attachment': [{'id': 1, 'attached_host': 'test-1'}],
}

ATTACHED_LIVE_MIGRATION_VOLUME = {
    'name': 'attached-live-migration-volume',
    'id': 'attached-live-migration-volume-00001',
    'status': 'in-use',
    'volume_attachment': [{'id': 1, 'attached_host': 'test-1'},
                          {'id': 2, 'attached_host': 'test-2'}],
}


def make_mock_linstor(controller_version=None):
    resources = copy.deepcopy(EXISTING_RESOURCES)
    resource_groups = copy.deepcopy(EXISTING_RESOURCE_GROUPS)
    controller_version = fake_linstor.ControllerVersion(
        controller_version or '1.4.1'
    )
    return fake_linstor.FakeLinstorMod(
        KNOWN_NODES, resources, resource_groups, controller_version,
    )


def configured_driver():
    conf = configuration.Configuration(None)
    driver = drv.LinstorDriver(configuration=conf, host='test-1')
    driver.check_for_setup_error()
    driver.init_capabilities()
    return driver


class LinstorDriverTestCase(test.TestCase):
    def __init__(self, *args, **kwargs):
        super(LinstorDriverTestCase, self).__init__(*args, **kwargs)

    def setUp(self):
        super(LinstorDriverTestCase, self).setUp()

    @mock.patch.object(drv, attribute='linstor', new=make_mock_linstor())
    def test_check_for_setup_error(self):
        conf = configuration.Configuration(None)
        driver = drv.LinstorDriver(configuration=conf, host='test-1')
        driver.check_for_setup_error()
        self.assertEqual('iSCSI', driver.protocol)

    @mock.patch.object(drv, attribute='linstor', new=make_mock_linstor())
    def test_check_for_setup_error_direct(self):
        conf = configuration.Configuration(None)
        conf.conf.linstor_direct = True
        driver = drv.LinstorDriver(configuration=conf, host='test-1')
        driver.check_for_setup_error()
        self.assertEqual('DRBD', driver.protocol)

    @mock.patch.object(drv, attribute='linstor', new=make_mock_linstor())
    def test_check_for_setup_error_no_multilinstor(self):
        delattr(drv.linstor, 'MultiLinstor')
        conf = configuration.Configuration(None)
        driver = drv.LinstorDriver(configuration=conf, host='test-1')
        self.assertRaisesRegex(
            drv.LinstorDriverException,
            r'Package python-linstor does not support MultiLinstor',
            driver.check_for_setup_error,
        )

    @mock.patch.object(drv, attribute='linstor', new=make_mock_linstor())
    def test_check_for_setup_error_old_resouce_type(self):
        def fake_resource_init(self, name, uri=None, timeout=None):
            pass

        drv.linstor.Resource = fake_resource_init
        conf = configuration.Configuration(None)
        driver = drv.LinstorDriver(configuration=conf, host='test-1')
        self.assertRaisesRegex(
            drv.LinstorDriverException,
            r'Package python-linstor does not support passing clients',
            driver.check_for_setup_error,
        )

    @mock.patch.object(drv, attribute='linstor',
                       new=make_mock_linstor(controller_version='1.3.9'))
    def test_check_for_setup_error_rest_version(self):
        conf = configuration.Configuration(None)
        driver = drv.LinstorDriver(configuration=conf, host='test-1')
        self.assertRaisesRegex(
            drv.LinstorDriverException,
            r'Linstor API not supported',
            driver.check_for_setup_error,
        )

    @mock.patch.object(drv, attribute='linstor', new=make_mock_linstor())
    def test_create_volume(self):
        driver = configured_driver()

        driver.create_volume(DEFAULT_NEW_VOLUME)

        self.assertIn('volume-default-new', drv.linstor.resources)
        self.assertIn('cinder-123-456', drv.linstor.resource_groups)
        rsc = drv.linstor.resources['volume-default-new']
        self.assertEqual(sorted(KNOWN_NODES)[:3], rsc['nodes'])
        self.assertEqual(
            fake_linstor.FakeVolume('/dev/drbd1003', {}, 5 * GIB),
            rsc['volumes'][0],
        )

    @mock.patch.object(drv, attribute='linstor', new=make_mock_linstor())
    def test_create_volume_extra_specs(self):
        driver = configured_driver()

        driver.create_volume({
            'id': 'some-fake-id',
            'name': 'volume-with-all-props',
            'size': 25,
            'host': 'test-1@fake-linstor#DfltRscGrp',
            'volume_type': {
                'id': 'all-props',
                'name': 'fake-vt',
                'extra_specs': {
                    'linstor:storage_pool': 'storpool',
                    'linstor:diskless_on_remaining': True,
                    'linstor:do_not_place_with_regex': 'Aux/other',
                    'linstor:layer_list': 'drbd,cache,storage',
                    'linstor:provider_list': 'lvmthin,lvm',
                    'linstor:redundancy': 1,
                    'linstor:replicas_on_different': 'diff',
                    'linstor:replicas_on_same': 'zone=A',
                    'linstor:property:DrbdOptions/auto-quorum': 'disabled'
                },
            },
        })

        self.assertIn('volume-with-all-props', drv.linstor.resources)
        rg = drv.linstor.resource_groups['cinder-all-props']
        self.assertEqual(['storpool'], rg['storage_pool'])
        self.assertTrue(rg['diskless_on_remaining'])
        self.assertEqual('Aux/other', rg['do_not_place_with_regex'])
        self.assertEqual(['drbd', 'cache', 'storage'], rg['layer_list'])
        self.assertEqual(['lvmthin', 'lvm'], rg['provider_list'])
        self.assertEqual(1, rg['redundancy'])
        self.assertEqual(['Aux/diff'], rg['replicas_on_different'])
        self.assertEqual(['Aux/zone=A'], rg['replicas_on_same'])
        self.assertEqual(
            {'DrbdOptions/auto-quorum': 'disabled'}, rg['property_dict'],
        )

    @mock.patch.object(drv, attribute='linstor', new=make_mock_linstor())
    def test_create_volume_from_snapshot(self):
        driver = configured_driver()

        driver.create_volume_from_snapshot(
            DEFAULT_NEW_VOLUME, EXISTING_SNAPSHOT,
        )
        self.assertIn('volume-default-new', drv.linstor.resources)
        driver.create_volume_from_snapshot(
            DEFAULT_NEW_VOLUME2, EXISTING_OLD_SNAPSHOT,
        )
        self.assertIn('volume-default-new-2', drv.linstor.resources)

    @mock.patch.object(drv, attribute='linstor', new=make_mock_linstor())
    def test_delete_volume(self):
        driver = configured_driver()
        self.assertRaises(
            exception.CinderException,
            driver.delete_volume,
            DEFAULT_EXISTING_VOLUME_WITH_SNAP,
        )
        driver.delete_snapshot(EXISTING_SNAPSHOT)
        driver.delete_snapshot(EXISTING_OLD_SNAPSHOT)
        driver.delete_volume(DEFAULT_EXISTING_VOLUME_WITH_SNAP)
        self.assertNotIn('basic-volume', drv.linstor.resources)

    @mock.patch.object(drv, attribute='linstor', new=make_mock_linstor())
    def test_create_snap(self):
        driver = configured_driver()
        driver.create_snapshot(DEFAULT_NEW_SNAP)
        self.assertIn(
            'snap3', drv.linstor.resources['basic-volume']['snapshots'],
        )

    @mock.patch.object(drv, attribute='linstor', new=make_mock_linstor())
    def test_clone_volume(self):
        driver = configured_driver()
        driver.create_cloned_volume(
            DEFAULT_NEW_VOLUME, DEFAULT_EXISTING_VOLUME_WITH_SNAP,
        )
        self.assertIn('volume-default-new', drv.linstor.resources)
        self.assertEqual(
            {}, drv.linstor.resources['volume-default-new']['snapshots'],
        )

    @mock.patch.object(drv, attribute='linstor', new=make_mock_linstor())
    def test_rollback(self):
        driver = configured_driver()
        driver.revert_to_snapshot(
            None, DEFAULT_EXISTING_VOLUME_WITH_SNAP, EXISTING_SNAPSHOT,
        )
        self.assertEqual(
            5 * GIB, drv.linstor.resources['basic-volume']['volumes'][0].size
        )


class LinstorDrbdDriverTestCase(test.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def setUp(self):
        super().setUp()

    @mock.patch.object(drv, attribute='linstor', new=make_mock_linstor())
    def test_driver_use_direct_connection(self):
        conf = configuration.Configuration(None)
        driver = drv.LinstorDrbdDriver(configuration=conf)
        self.assertTrue(driver._use_direct_connection())


class LinstorIscsiDriverTestCase(test.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def setUp(self):
        super().setUp()

    @mock.patch.object(drv, attribute='linstor', new=make_mock_linstor())
    def test_driver_use_direct_connection(self):
        conf = configuration.Configuration(None)
        driver = drv.LinstorIscsiDriver(configuration=conf)
        self.assertFalse(driver._use_direct_connection())


class LinstorDirectTargetTestCase(test.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def setUp(self):
        super().setUp()

    @mock.patch.object(drv, attribute='linstor', new=make_mock_linstor())
    def test_initialize_connection(self):
        connector = {'host': 'test-1'}
        target_helper = drv.LinstorDirectTarget(
            fake_linstor.FakeLinstorClientGetter(drv.linstor.MultiLinstor([])),
        )
        actual = target_helper.initialize_connection(BASIC_VOLUME, connector)
        expected = {
            'data': {'device_path': '/dev/drbd/by-res/basic-volume/0'},
            'driver_volume_type': 'local'
        }
        self.assertEqual(expected, actual)
        self.assertIn('test-1', drv.linstor.resources['basic-volume']['nodes'])

    @mock.patch.object(drv, attribute='linstor', new=make_mock_linstor())
    def test_initialize_live_migration(self):
        connector = {'host': 'test-2'}
        target_helper = drv.LinstorDirectTarget(
            fake_linstor.FakeLinstorClientGetter(drv.linstor.MultiLinstor([])),
        )
        actual = target_helper.initialize_connection(
            LIVE_MIGRATION_VOLUME,
            connector,
        )
        expected = {'data': {'device_path': '/dev/drbd/by-res/basic-volume/0'},
                    'driver_volume_type': 'local'}
        self.assertEqual(expected, actual)
        self.assertTrue(drv.linstor.resources['basic-volume']
                        ['allow_two_primaries'])
        self.assertIn('test-2', drv.linstor.resources['basic-volume']['nodes'])

    @mock.patch.object(drv, attribute='linstor', new=make_mock_linstor())
    def test_initialize_connection_unknown_host(self):
        connector = {'host': 'unknown-host'}
        target_helper = drv.LinstorDirectTarget(
            fake_linstor.FakeLinstorClientGetter(drv.linstor.MultiLinstor([])),
        )
        self.assertRaises(
            exception.CinderException,
            target_helper.initialize_connection,
            BASIC_VOLUME,
            connector,
        )

    @mock.patch.object(drv, attribute='linstor', new=make_mock_linstor())
    def test_terminate_connection(self):
        connector = {'host': 'test-1'}
        target_helper = drv.LinstorDirectTarget(
            fake_linstor.FakeLinstorClientGetter(drv.linstor.MultiLinstor([])),
        )
        target_helper.terminate_connection(
            ATTACHED_VOLUME,
            connector
        )
        self.assertNotIn(
            'test-1', drv.linstor.resources['attached-volume']['nodes']
        )

    @mock.patch.object(drv, attribute='linstor', new=make_mock_linstor())
    def test_terminate_connection_live_migration(self):
        connector = {'host': 'test-2'}
        target_helper = drv.LinstorDirectTarget(
            fake_linstor.FakeLinstorClientGetter(drv.linstor.MultiLinstor([])),
        )
        target_helper.terminate_connection(
            ATTACHED_LIVE_MIGRATION_VOLUME,
            connector
        )
        self.assertFalse(drv.linstor.resources
                         ['attached-live-migration-volume']
                         ['allow_two_primaries'])

    @mock.patch.object(drv, attribute='linstor', new=make_mock_linstor())
    def test_terminate_connection_force(self):
        connector = None
        target_helper = drv.LinstorDirectTarget(
            fake_linstor.FakeLinstorClientGetter(drv.linstor.MultiLinstor([])),
        )
        target_helper.terminate_connection(
            ATTACHED_LIVE_MIGRATION_VOLUME,
            connector
        )
        self.assertFalse(drv.linstor.resources
                         ['attached-live-migration-volume']
                         ['allow_two_primaries'])

    @mock.patch.object(drv, attribute='linstor', new=make_mock_linstor())
    def test_terminate_connection_unknown_host(self):
        connector = {'host': 'unknown-host'}
        target_helper = drv.LinstorDirectTarget(
            fake_linstor.FakeLinstorClientGetter(drv.linstor.MultiLinstor([]))
        )
        self.assertRaises(
            exception.CinderException,
            target_helper.terminate_connection,
            BASIC_VOLUME,
            connector,
        )


class LinstorUtilsTestCase(test.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def setUp(self):
        super().setUp()

    @mock.patch.object(drv, attribute='linstor', new=make_mock_linstor())
    def test_get_existing_resource_v2_name(self):
        client = drv.linstor.MultiLinstor([])
        actual = drv._get_existing_resource(
            client,
            'basic-volume',
            '[some-id]',
        )
        self.assertEqual('basic-volume', actual.name)

    @mock.patch.object(drv, attribute='linstor', new=make_mock_linstor())
    def test_get_existing_resource_v1_name(self):
        client = drv.linstor.MultiLinstor([])
        actual = drv._get_existing_resource(
            client,
            'some-unknown-name',
            '[some-id]',
        )
        self.assertEqual('CV_[some-id]', actual.name)

    @mock.patch.object(drv, attribute='linstor', new=make_mock_linstor())
    def test_get_existing_resource_negative(self):
        client = drv.linstor.MultiLinstor([])
        self.assertRaises(
            exception.CinderException,
            drv._get_existing_resource,
            client,
            'some-unknown-name',
            '[some-unknown-id]',
        )

    @mock.patch.object(drv, attribute='linstor', new=make_mock_linstor())
    def test_local_resource_path(self):
        client = drv.linstor.MultiLinstor([])
        rsc = drv.linstor.Resource('basic-volume', client)

        with drv._temp_resource_path(client, rsc, 'test-1') as path:
            self.assertIn(
                'test-1', drv.linstor.resources['basic-volume']['nodes']
            )
            self.assertEqual('/dev/drbd/by-res/basic-volume/0', path)

        self.assertNotIn(
            'test-1', drv.linstor.resources['basic-volume']['nodes']
        )

    @mock.patch.object(drv, attribute='linstor', new=make_mock_linstor())
    def test_local_resource_path_no_preferred_link(self):
        volume = fake_linstor.FakeVolume(
            '/dev/drbd1000',
            {
                'Satellite/Device/Symlinks/0': '/dev/symlink0',
                'Satellite/Device/Symlinks/1': '/dev/symlink1',
            },
            1 * GIB,
        )
        drv.linstor.resources['basic-volume']["volumes"][0] = volume
        client = drv.linstor.MultiLinstor([])
        rsc = drv.linstor.Resource('basic-volume', client)

        with drv._temp_resource_path(client, rsc, 'test-1') as path:
            self.assertIn(path, ['/dev/symlink0', '/dev/symlink1'])

    @mock.patch.object(drv, attribute='linstor', new=make_mock_linstor())
    def test_local_resource_path_no_symlink(self):
        client = drv.linstor.MultiLinstor([])
        rsc = drv.linstor.Resource('attached-volume', existing_client=client)

        with drv._temp_resource_path(client, rsc, 'test-1') as path:
            self.assertIn(
                'test-1', drv.linstor.resources['attached-volume']['nodes']
            )
            self.assertEqual('/dev/drbd1002', path)

        self.assertNotIn(
            'test-1', drv.linstor.resources['attached-volume']['nodes']
        )

    @mock.patch.object(drv, attribute='linstor', new=make_mock_linstor())
    def test_local_resource_path_on_exception(self):
        client = drv.linstor.MultiLinstor([])
        rsc = drv.linstor.Resource('basic-volume', existing_client=client)
        try:
            with drv._temp_resource_path(client, rsc, 'test-1'):
                self.assertIn(
                    'test-1', drv.linstor.resources['basic-volume']['nodes']
                )
                raise ValueError()
        except ValueError:
            pass

        self.assertNotIn(
            'test-1', drv.linstor.resources['basic-volume']['nodes']
        )

    @mock.patch.object(drv, attribute='linstor', new=make_mock_linstor())
    def test_local_resource_path_negative(self):
        client = drv.linstor.MultiLinstor([])
        rsc = drv.linstor.Resource('basic-volume', existing_client=client)
        try:
            with drv._temp_resource_path(client, rsc, 'unknown-host'):
                pass
        except drv.linstor.LinstorError:
            pass
        else:
            self.fail("local path on unknown host should fail")
