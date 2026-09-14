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

from cinder import exception
from cinder.tests.unit import test
from cinder.tests.unit.volume.drivers.linstor import fake_linstor
from cinder.volume import configuration
from cinder.volume.drivers import linstordrv as drv

# LINSTOR node name -> node properties and network interface addresses
KNOWN_NODES = {
    'test-1': {},
    'test-2': {},
    'test-3': {
        'props': {
            'Aux/openstack-host': 'a30aa14d-56ac-49b6-adcd-f8027baf2da2',
        },
    },
    'test-4': {'props': {'Aux/openstack-host': 'duplicate-host'}},
    'test-5': {'props': {'Aux/openstack-host': 'duplicate-host'}},
    'test-6': {
        'props': {'NodeUname': 'compute-6.example.com'},
        'addresses': ['192.0.2.6', '2001:db8::6'],
    },
    'test-7': {'addresses': ['192.0.2.77']},
    'test-8': {'addresses': ['192.0.2.77']},
    'test-9': {'props': {'Aux/openstack-host': 'test-1'}},
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
    'migrating-volume': {
        'nodes': {'test-1': False, 'test-3': True, 'test-4': 'tiebreaker'},
        'in_use': {'test-1'},
        'resource_group_name': 'cinder-123-456',
        'volumes': {
            0: fake_linstor.FakeVolume('/dev/drbd1999', {}, 5 * GIB)
        },
        'allow_two_primaries': False,
    },
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

ATTACHED_LIVE_MIGRATION_VOLUME = {
    'name': 'attached-live-migration-volume',
    'id': 'attached-live-migration-volume-00001',
    'status': 'in-use',
    'volume_attachment': [{'id': 1, 'attached_host': 'test-1'},
                          {'id': 2, 'attached_host': 'test-2'}],
}

MIGRATING_VOLUME = {
    'name': 'migrating-volume',
    'id': 'migrating-volume-00001',
    'status': 'in-use',
    'volume_attachment': [{'id': 1, 'attached_host': 'test-1'}],
}

MULTIATTACH_VOLUME = {
    'name': 'attached-volume',
    'id': 'attached-volume-00001',
    'status': 'in-use',
    'multiattach': True,
    'volume_attachment': [],
}

INITIATOR_1 = {'initiator': 'iqn.2026-09.test:test-1'}
INITIATOR_2 = {'initiator': 'iqn.2026-09.test:test-2'}


def make_mock_linstor(controller_version=None):
    resources = copy.deepcopy(EXISTING_RESOURCES)
    resource_groups = copy.deepcopy(EXISTING_RESOURCE_GROUPS)
    controller_version = fake_linstor.ControllerVersion(
        controller_version or '1.29.1'
    )
    return fake_linstor.FakeLinstorMod(
        KNOWN_NODES, resources, resource_groups, controller_version,
    )


def configured_driver(direct=False):
    conf = configuration.Configuration(None)
    conf.conf.linstor_direct = direct
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
    def test_check_for_setup_error_direct_connector_host_property(self):
        conf = configuration.Configuration(None)
        conf.conf.linstor_direct = True
        conf.conf.linstor_connector_host_property = 'Aux/openstack-host'
        driver = drv.LinstorDriver(configuration=conf, host='test-1')
        driver.check_for_setup_error()
        self.assertEqual(
            'Aux/openstack-host',
            driver.target_driver._connector_host_property,
        )

    @mock.patch.object(drv, attribute='linstor',
                       new=make_mock_linstor(controller_version='1.28.2'))
    def test_check_for_setup_error_rest_version_no_unmake_available(self):
        conf = configuration.Configuration(None)
        driver = drv.LinstorDriver(configuration=conf, host='test-1')
        self.assertRaisesRegex(
            drv.LinstorDriverException,
            r'Linstor API not supported: \(1, 28, 2\) < \(1, 29, 0\)',
            driver.check_for_setup_error,
        )

    @mock.patch.object(drv, attribute='linstor', new=make_mock_linstor())
    def test_check_for_setup_error_no_unmake_available(self):
        unmake = fake_linstor.MultiLinstor.resource_unmake_available
        delattr(fake_linstor.MultiLinstor, 'resource_unmake_available')
        self.addCleanup(setattr, fake_linstor.MultiLinstor,
                        'resource_unmake_available', unmake)
        conf = configuration.Configuration(None)
        driver = drv.LinstorDriver(configuration=conf, host='test-1')
        self.assertRaisesRegex(
            drv.LinstorDriverException,
            r'Package python-linstor does not support unmake-available',
            driver.check_for_setup_error,
        )

    @mock.patch.object(drv, attribute='linstor', new=make_mock_linstor())
    def test_check_for_setup_error_host_reported_host_name(self):
        # The Cinder host is not a node name, but a satellite reported it as
        # its host name; the backend suffix is ignored
        conf = configuration.Configuration(None)
        driver = drv.LinstorDriver(
            configuration=conf, host='compute-6.example.com@linstor',
        )
        driver.check_for_setup_error()
        self.assertEqual('test-6', driver._hostname)

    @mock.patch.object(drv, attribute='linstor', new=make_mock_linstor())
    @mock.patch.object(drv.socket, 'gethostname',
                       return_value='COMPUTE-6.example.com')
    def test_check_for_setup_error_system_host_name(self, _gethostname):
        # Neither the Cinder host nor the system host name is a node name,
        # but a satellite reported the system host name (case does not
        # matter)
        conf = configuration.Configuration(None)
        driver = drv.LinstorDriver(
            configuration=conf, host='fea4e49b-b215-470c-83ae-e89d6f3e0996',
        )
        driver.check_for_setup_error()
        self.assertEqual('test-6', driver._hostname)

    @mock.patch.object(drv, attribute='linstor', new=make_mock_linstor())
    def test_check_for_setup_error_host_by_address(self):
        # Cinder host unrelated to LINSTOR, but my_ip is a LINSTOR network
        # interface of a node
        self.flags(my_ip='192.0.2.6')
        conf = configuration.Configuration(None)
        driver = drv.LinstorDriver(
            configuration=conf, host='fea4e49b-b215-470c-83ae-e89d6f3e0996',
        )
        driver.check_for_setup_error()
        self.assertEqual('test-6', driver._hostname)

    @mock.patch.object(drv, attribute='linstor', new=make_mock_linstor())
    def test_check_for_setup_error_unknown_host(self):
        self.flags(my_ip='198.51.100.1')
        conf = configuration.Configuration(None)
        driver = drv.LinstorDriver(
            configuration=conf, host='fea4e49b-b215-470c-83ae-e89d6f3e0996',
        )
        self.assertRaisesRegex(
            drv.LinstorDriverException,
            r'Cinder host fea4e49b-b215-470c-83ae-e89d6f3e0996 matches no '
            r'LINSTOR node',
            driver.check_for_setup_error,
        )

    @mock.patch.object(drv, attribute='linstor', new=make_mock_linstor())
    @mock.patch.object(drv, 'open', create=True,
                       new=mock.mock_open(read_data='test-2\n'))
    def test_check_for_setup_error_hostname_file(self):
        # The hostname file wins over a node named like the Cinder host
        conf = configuration.Configuration(None)
        driver = drv.LinstorDriver(configuration=conf, host='test-1')
        self.flags(linstor_hostname_file='/etc/cinder/linstor-node')
        driver.check_for_setup_error()
        self.assertEqual('test-2', driver._hostname)

    @mock.patch.object(drv, attribute='linstor', new=make_mock_linstor())
    @mock.patch.object(drv, 'open', create=True,
                       new=mock.mock_open(read_data='unknown-node\n'))
    def test_check_for_setup_error_hostname_file_unknown(self):
        # An explicitly configured node name is not replaced by lookups
        self.flags(my_ip='192.0.2.6')
        conf = configuration.Configuration(None)
        driver = drv.LinstorDriver(configuration=conf, host='test-1')
        self.flags(linstor_hostname_file='/etc/cinder/linstor-node')
        self.assertRaisesRegex(
            drv.LinstorDriverException,
            r'Cinder host unknown-node matches no LINSTOR node',
            driver.check_for_setup_error,
        )

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
    def test_delete_snapshot(self):
        driver = configured_driver()
        snapshots = drv.linstor.resources['basic-volume']['snapshots']

        driver.delete_snapshot(EXISTING_SNAPSHOT)
        self.assertNotIn('snap1', snapshots)
        self.assertIn('SN_snap2', snapshots)

        driver.delete_snapshot(EXISTING_OLD_SNAPSHOT)
        self.assertNotIn('SN_snap2', snapshots)

        driver.delete_snapshot(DEFAULT_NEW_SNAP)
        self.assertEqual({}, snapshots)

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
            {},
            drv.linstor.resources['volume-default-new'].get('snapshots', {}),
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

    @mock.patch.object(drv, attribute='linstor', new=make_mock_linstor())
    def test_terminate_connection_force_detach(self):
        driver = configured_driver()
        attachment = mock.Mock(connector=INITIATOR_1)
        volume = dict(
            MULTIATTACH_VOLUME, volume_attachment=[attachment, attachment],
        )
        with mock.patch.object(
                driver.target_driver, 'terminate_connection') as terminate:
            self.assertFalse(driver.terminate_connection(volume, None))
        terminate.assert_called_once_with(volume, None)

    @mock.patch.object(drv, attribute='linstor', new=make_mock_linstor())
    def test_terminate_connection_force_detach_direct(self):
        driver = configured_driver(direct=True)
        volume = dict(ATTACHED_LIVE_MIGRATION_VOLUME, multiattach=False)
        self.assertFalse(driver.terminate_connection(volume, None))
        self.assertFalse(drv.linstor.resources
                         ['attached-live-migration-volume']
                         ['allow_two_primaries'])

    @mock.patch.object(drv, attribute='linstor', new=make_mock_linstor())
    def test_terminate_connection_multiattach_same_host(self):
        driver = configured_driver()
        attachment = mock.Mock(connector=INITIATOR_1)
        volume = dict(
            MULTIATTACH_VOLUME, volume_attachment=[attachment, attachment],
        )
        with mock.patch.object(
                driver.target_driver, 'terminate_connection') as terminate:
            self.assertTrue(driver.terminate_connection(volume, INITIATOR_1))
        terminate.assert_not_called()

    @mock.patch.object(drv, attribute='linstor', new=make_mock_linstor())
    def test_terminate_connection_multiattach_other_host(self):
        driver = configured_driver()
        volume = dict(MULTIATTACH_VOLUME, volume_attachment=[
            mock.Mock(connector=INITIATOR_1),
            mock.Mock(connector=INITIATOR_2),
        ])
        with mock.patch.object(
                driver.target_driver, 'terminate_connection') as terminate:
            self.assertTrue(driver.terminate_connection(volume, INITIATOR_1))
        terminate.assert_called_once_with(volume, INITIATOR_1)

    @mock.patch.object(drv, attribute='linstor', new=make_mock_linstor())
    def test_terminate_connection_multiattach_last_attachment(self):
        driver = configured_driver()
        volume = dict(
            MULTIATTACH_VOLUME,
            volume_attachment=[mock.Mock(connector=INITIATOR_1)],
        )
        with mock.patch.object(
                driver.target_driver, 'terminate_connection') as terminate:
            self.assertFalse(driver.terminate_connection(volume, INITIATOR_1))
        terminate.assert_called_once_with(volume, INITIATOR_1)


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
    def test_initialize_connection_mapped_host(self):
        # Nova reports a host name that is not a LINSTOR node name, but a
        # node carries it as property value (case does not matter)
        connector = {'host': 'A30AA14D-56AC-49B6-ADCD-F8027BAF2DA2'}
        target_helper = drv.LinstorDirectTarget(
            fake_linstor.FakeLinstorClientGetter(drv.linstor.MultiLinstor([])),
            connector_host_property='Aux/openstack-host',
        )
        actual = target_helper.initialize_connection(BASIC_VOLUME, connector)
        expected = {
            'data': {'device_path': '/dev/drbd/by-res/basic-volume/0'},
            'driver_volume_type': 'local'
        }
        self.assertEqual(expected, actual)
        self.assertIn('test-3', drv.linstor.resources['basic-volume']['nodes'])

    @mock.patch.object(drv, attribute='linstor', new=make_mock_linstor())
    def test_initialize_connection_mapped_host_fallback(self):
        # Property configured, but no node carries it for this host: the
        # node named like the connector host is used, like without the option
        connector = {'host': 'test-2'}
        target_helper = drv.LinstorDirectTarget(
            fake_linstor.FakeLinstorClientGetter(drv.linstor.MultiLinstor([])),
            connector_host_property='Aux/openstack-host',
        )
        target_helper.initialize_connection(BASIC_VOLUME, connector)
        self.assertIn('test-2', drv.linstor.resources['basic-volume']['nodes'])

    @mock.patch.object(drv, attribute='linstor', new=make_mock_linstor())
    def test_initialize_connection_mapped_host_precedes_name(self):
        # The configured property wins over a node named like the host
        connector = {'host': 'test-1'}
        target_helper = drv.LinstorDirectTarget(
            fake_linstor.FakeLinstorClientGetter(drv.linstor.MultiLinstor([])),
            connector_host_property='Aux/openstack-host',
        )
        target_helper.initialize_connection(BASIC_VOLUME, connector)
        nodes = drv.linstor.resources['basic-volume']['nodes']
        self.assertIn('test-9', nodes)
        self.assertNotIn('test-1', nodes)

    @mock.patch.object(drv, attribute='linstor', new=make_mock_linstor())
    def test_initialize_connection_mapped_host_ambiguous(self):
        connector = {'host': 'duplicate-host'}
        target_helper = drv.LinstorDirectTarget(
            fake_linstor.FakeLinstorClientGetter(drv.linstor.MultiLinstor([])),
            connector_host_property='Aux/openstack-host',
        )
        self.assertRaises(
            drv.LinstorDriverException,
            target_helper.initialize_connection,
            BASIC_VOLUME,
            connector,
        )
        self.assertNotIn(
            'test-4', drv.linstor.resources['basic-volume']['nodes']
        )
        self.assertNotIn(
            'test-5', drv.linstor.resources['basic-volume']['nodes']
        )

    @mock.patch.object(drv, attribute='linstor', new=make_mock_linstor())
    def test_initialize_connection_mapped_host_unknown(self):
        connector = {'host': 'unknown-host'}
        target_helper = drv.LinstorDirectTarget(
            fake_linstor.FakeLinstorClientGetter(drv.linstor.MultiLinstor([])),
            connector_host_property='Aux/openstack-host',
        )
        self.assertRaises(
            exception.CinderException,
            target_helper.initialize_connection,
            BASIC_VOLUME,
            connector,
        )

    @mock.patch.object(drv, attribute='linstor', new=make_mock_linstor())
    def test_initialize_connection_name_case_insensitive(self):
        # LINSTOR node names are case-insensitive, the driver must use the
        # name as known to LINSTOR
        connector = {'host': 'TEST-2'}
        target_helper = drv.LinstorDirectTarget(
            fake_linstor.FakeLinstorClientGetter(drv.linstor.MultiLinstor([])),
        )
        target_helper.initialize_connection(BASIC_VOLUME, connector)
        self.assertIn('test-2', drv.linstor.resources['basic-volume']['nodes'])

    @mock.patch.object(drv, attribute='linstor', new=make_mock_linstor())
    def test_initialize_connection_reported_host_name(self):
        # No node is named like the connector host, but a satellite reported
        # it as its host name (case does not matter)
        connector = {'host': 'COMPUTE-6.example.com', 'ip': '198.51.100.1'}
        target_helper = drv.LinstorDirectTarget(
            fake_linstor.FakeLinstorClientGetter(drv.linstor.MultiLinstor([])),
        )
        target_helper.initialize_connection(BASIC_VOLUME, connector)
        self.assertIn('test-6', drv.linstor.resources['basic-volume']['nodes'])

    @mock.patch.object(drv, attribute='linstor', new=make_mock_linstor())
    def test_initialize_connection_by_address(self):
        # Nova host name unrelated to LINSTOR, but the connector IP is a
        # LINSTOR network interface of a node
        connector = {
            'host': 'a30aa14d-56ac-49b6-adcd-f8027baf2da2', 'ip': '192.0.2.6',
        }
        target_helper = drv.LinstorDirectTarget(
            fake_linstor.FakeLinstorClientGetter(drv.linstor.MultiLinstor([])),
        )
        target_helper.initialize_connection(BASIC_VOLUME, connector)
        self.assertIn('test-6', drv.linstor.resources['basic-volume']['nodes'])

    @mock.patch.object(drv, attribute='linstor', new=make_mock_linstor())
    def test_initialize_connection_by_ipv6_address(self):
        # Addresses are compared as IP addresses, not as strings
        connector = {'host': 'unknown-host', 'ip': '2001:DB8:0:0:0:0:0:6'}
        target_helper = drv.LinstorDirectTarget(
            fake_linstor.FakeLinstorClientGetter(drv.linstor.MultiLinstor([])),
        )
        target_helper.initialize_connection(BASIC_VOLUME, connector)
        self.assertIn('test-6', drv.linstor.resources['basic-volume']['nodes'])

    @mock.patch.object(drv, attribute='linstor', new=make_mock_linstor())
    def test_initialize_connection_by_address_ambiguous(self):
        connector = {'host': 'unknown-host', 'ip': '192.0.2.77'}
        target_helper = drv.LinstorDirectTarget(
            fake_linstor.FakeLinstorClientGetter(drv.linstor.MultiLinstor([])),
        )
        self.assertRaises(
            drv.LinstorDriverException,
            target_helper.initialize_connection,
            BASIC_VOLUME,
            connector,
        )
        nodes = drv.linstor.resources['basic-volume']['nodes']
        self.assertNotIn('test-7', nodes)
        self.assertNotIn('test-8', nodes)

    @mock.patch.object(drv, attribute='linstor', new=make_mock_linstor())
    def test_initialize_connection_unknown_host_and_address(self):
        connector = {'host': 'unknown-host', 'ip': '198.51.100.1'}
        target_helper = drv.LinstorDirectTarget(
            fake_linstor.FakeLinstorClientGetter(drv.linstor.MultiLinstor([])),
        )
        self.assertRaises(
            drv.LinstorDriverException,
            target_helper.initialize_connection,
            BASIC_VOLUME,
            connector,
        )

    @mock.patch.object(drv, attribute='linstor', new=make_mock_linstor())
    def test_initialize_connection_invalid_address(self):
        # A connector IP that is not an IP address is ignored
        connector = {'host': 'test-2', 'ip': 'not-an-address'}
        target_helper = drv.LinstorDirectTarget(
            fake_linstor.FakeLinstorClientGetter(drv.linstor.MultiLinstor([])),
        )
        target_helper.initialize_connection(BASIC_VOLUME, connector)
        self.assertIn('test-2', drv.linstor.resources['basic-volume']['nodes'])

    @mock.patch.object(drv, attribute='linstor', new=make_mock_linstor())
    def test_initialize_connection_not_in_use(self):
        # Not in use anywhere: a plain make-available
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
        rsc = drv.linstor.resources['basic-volume']
        self.assertIn('test-1', rsc['nodes'])
        self.assertFalse(rsc['allow_two_primaries'])

    @mock.patch.object(drv, attribute='linstor', new=make_mock_linstor())
    def test_initialize_connection_live_migration(self):
        # In use on test-1: LINSTOR opens the dual-primary window to test-2
        connector = {'host': 'test-2'}
        target_helper = drv.LinstorDirectTarget(
            fake_linstor.FakeLinstorClientGetter(drv.linstor.MultiLinstor([])),
        )
        actual = target_helper.initialize_connection(
            MIGRATING_VOLUME, connector,
        )
        expected = {
            'data': {'device_path': '/dev/drbd/by-res/migrating-volume/0'},
            'driver_volume_type': 'local'
        }
        self.assertEqual(expected, actual)
        rsc = drv.linstor.resources['migrating-volume']
        self.assertIn('test-2', rsc['nodes'])
        self.assertTrue(rsc['allow_two_primaries'])
        self.assertEqual(('test-1', 'test-2'), rsc['live_migration'])

    @mock.patch.object(drv, attribute='linstor', new=make_mock_linstor())
    def test_initialize_connection_error(self):
        connector = {'host': 'test-2'}
        target_helper = drv.LinstorDirectTarget(
            fake_linstor.FakeLinstorClientGetter(drv.linstor.MultiLinstor([])),
        )
        error = fake_linstor.FakeApiCallResponse('Refused', error=True)
        with mock.patch.object(fake_linstor.MultiLinstor,
                               'resource_make_available',
                               return_value=[error]):
            self.assertRaises(
                exception.VolumeBackendAPIException,
                target_helper.initialize_connection,
                MIGRATING_VOLUME,
                connector,
            )

    @mock.patch.object(drv, attribute='linstor', new=make_mock_linstor())
    def test_terminate_connection_live_migration_source(self):
        # Migration finished: the source is no longer in use, its diskless
        # resource goes away together with the dual-primary window
        rsc = drv.linstor.resources['migrating-volume']
        rsc['nodes']['test-2'] = False
        rsc['in_use'] = {'test-2'}
        rsc['allow_two_primaries'] = True
        connector = {'host': 'test-1'}
        target_helper = drv.LinstorDirectTarget(
            fake_linstor.FakeLinstorClientGetter(drv.linstor.MultiLinstor([])),
        )
        target_helper.terminate_connection(MIGRATING_VOLUME, connector)
        self.assertNotIn('test-1', rsc['nodes'])
        self.assertIn('test-2', rsc['nodes'])
        self.assertFalse(rsc['allow_two_primaries'])

    @mock.patch.object(drv, attribute='linstor', new=make_mock_linstor())
    def test_terminate_connection_keeps_replicas(self):
        # Diskful replicas and tiebreakers stay in place
        rsc = drv.linstor.resources['migrating-volume']
        rsc['in_use'] = set()
        target_helper = drv.LinstorDirectTarget(
            fake_linstor.FakeLinstorClientGetter(drv.linstor.MultiLinstor([])),
        )
        for host in ('test-3', 'test-4'):
            target_helper.terminate_connection(
                MIGRATING_VOLUME, {'host': host},
            )
        self.assertEqual(
            {'test-1': False, 'test-3': True, 'test-4': 'tiebreaker'},
            rsc['nodes'],
        )

    @mock.patch.object(drv, attribute='linstor', new=make_mock_linstor())
    def test_terminate_connection_not_deployed(self):
        # Not deployed on the node: a successful no-op
        rsc = drv.linstor.resources['migrating-volume']
        target_helper = drv.LinstorDirectTarget(
            fake_linstor.FakeLinstorClientGetter(drv.linstor.MultiLinstor([])),
        )
        target_helper.terminate_connection(
            MIGRATING_VOLUME, {'host': 'test-2'},
        )
        self.assertEqual(
            {'test-1': False, 'test-3': True, 'test-4': 'tiebreaker'},
            rsc['nodes'],
        )

    @mock.patch('time.sleep')
    @mock.patch.object(drv, attribute='linstor', new=make_mock_linstor())
    def test_terminate_connection_in_use(self, _sleep):
        # Still in use on the node: LINSTOR refuses, the driver gives up
        # after retrying
        rsc = drv.linstor.resources['migrating-volume']
        target_helper = drv.LinstorDirectTarget(
            fake_linstor.FakeLinstorClientGetter(drv.linstor.MultiLinstor([])),
        )
        self.assertRaises(
            exception.VolumeBackendAPIException,
            target_helper.terminate_connection,
            MIGRATING_VOLUME,
            {'host': 'test-1'},
        )
        self.assertIn('test-1', rsc['nodes'])

    @mock.patch.object(drv, attribute='linstor', new=make_mock_linstor())
    def test_terminate_connection_force_unknown_host(self):
        # Force detach: best-effort unmake-available on every attached host,
        # hosts that cannot be resolved are skipped
        rsc = drv.linstor.resources['migrating-volume']
        rsc['in_use'] = set()
        rsc['allow_two_primaries'] = True
        volume = dict(MIGRATING_VOLUME, volume_attachment=[
            {'id': 1, 'attached_host': 'test-1'},
            {'id': 2, 'attached_host': 'unknown-host'},
        ])
        target_helper = drv.LinstorDirectTarget(
            fake_linstor.FakeLinstorClientGetter(drv.linstor.MultiLinstor([])),
        )
        target_helper.terminate_connection(volume, None)
        self.assertNotIn('test-1', rsc['nodes'])
        self.assertFalse(rsc['allow_two_primaries'])

    @mock.patch.object(drv, attribute='linstor', new=make_mock_linstor())
    def test_terminate_connection_by_address(self):
        connector = {'host': 'unknown-host', 'ip': '192.0.2.6'}
        drv.linstor.resources['attached-volume']['nodes']['test-6'] = False
        target_helper = drv.LinstorDirectTarget(
            fake_linstor.FakeLinstorClientGetter(drv.linstor.MultiLinstor([])),
        )
        target_helper.terminate_connection(ATTACHED_VOLUME, connector)
        self.assertNotIn(
            'test-6', drv.linstor.resources['attached-volume']['nodes']
        )

    @mock.patch.object(drv, attribute='linstor', new=make_mock_linstor())
    def test_terminate_connection_mapped_host(self):
        connector = {'host': 'a30aa14d-56ac-49b6-adcd-f8027baf2da2'}
        drv.linstor.resources['attached-volume']['nodes']['test-3'] = False
        target_helper = drv.LinstorDirectTarget(
            fake_linstor.FakeLinstorClientGetter(drv.linstor.MultiLinstor([])),
            connector_host_property='Aux/openstack-host',
        )
        target_helper.terminate_connection(
            ATTACHED_VOLUME,
            connector
        )
        self.assertNotIn(
            'test-3', drv.linstor.resources['attached-volume']['nodes']
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

        with drv._temp_resource_path(client, rsc, 'test-1', False) as path:
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
        except exception.VolumeBackendAPIException:
            pass
        else:
            self.fail("local path on unknown host should fail")
