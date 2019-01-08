# Copyright (c) 2015 Tintri.  All rights reserved.
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
Volume driver test for Tintri storage.
"""

import ddt
import mock

from oslo_utils import units

from cinder import context
from cinder import exception
from cinder.tests.unit import fake_constants as fake
from cinder.tests.unit import fake_snapshot
from cinder.tests.unit import fake_volume
from cinder.tests.unit import utils as cinder_utils
from cinder.tests.unit.volume import test_driver
from cinder.volume.drivers.tintri import TClient
from cinder.volume.drivers.tintri import TintriDriver


class FakeImage(object):
    def __init__(self):
        self.id = 'image-id'
        self.name = 'image-name'
        self.properties = {'provider_location': 'nfs://share'}

    def __getitem__(self, key):
        return self.__dict__[key]


@ddt.ddt
class TintriDriverTestCase(test_driver.BaseDriverTestCase):

    driver_name = 'cinder.volume.drivers.tintri.TintriDriver'

    def setUp(self):
        super(TintriDriverTestCase, self).setUp()
        self.context = context.get_admin_context()
        self.configuration.nfs_mount_point_base = '/mnt/test'
        self.configuration.nfs_mount_options = None
        self.configuration.nas_mount_options = None
        self._driver = TintriDriver(configuration=self.configuration)
        self._driver._hostname = 'host'
        self._driver._username = 'user'
        self._driver._password = 'password'
        self._driver._api_version = 'v310'
        self._driver._image_cache_expiry = 30
        self._provider_location = 'localhost:/share'
        self._driver._mounted_shares = [self._provider_location]
        self.fake_stubs()

    def fake_stubs(self):
        self.mock_object(TClient, 'login', self.fake_login)
        self.mock_object(TClient, 'logout', self.fake_logout)
        self.mock_object(TClient, 'get_snapshot', self.fake_get_snapshot)
        self.mock_object(TClient, 'get_image_snapshots_to_date',
                         self.fake_get_image_snapshots_to_date)
        self.mock_object(TintriDriver, '_move_cloned_volume',
                         self.fake_move_cloned_volume)
        self.mock_object(TintriDriver, '_get_provider_location',
                         self.fake_get_provider_location)
        self.mock_object(TintriDriver, '_set_rw_permissions',
                         self.fake_set_rw_permissions)
        self.mock_object(TintriDriver, '_is_volume_present',
                         self.fake_is_volume_present)
        self.mock_object(TintriDriver, '_is_share_vol_compatible',
                         self.fake_is_share_vol_compatible)
        self.mock_object(TintriDriver, '_is_file_size_equal',
                         self.fake_is_file_size_equal)

    def fake_login(self, user_name, password):
        return 'session-id'

    def fake_logout(self):
        pass

    def fake_get_snapshot(self, volume_id):
        return fake.SNAPSHOT_ID

    def fake_get_image_snapshots_to_date(self, date):
        return [{'uuid': {'uuid': 'image_snapshot-id'}}]

    def fake_move_cloned_volume(self, clone_name, volume_id, share=None):
        pass

    def fake_get_provider_location(self, volume_path):
        return self._provider_location

    def fake_set_rw_permissions(self, path):
        pass

    def fake_is_volume_present(self, volume_path):
        return True

    def fake_is_share_vol_compatible(self, volume, share):
        return True

    def fake_is_file_size_equal(self, path, size):
        return True

    @mock.patch.object(TClient, 'create_snapshot',
                       mock.Mock(return_value=fake.PROVIDER_ID))
    def test_create_snapshot(self):
        snapshot = fake_snapshot.fake_snapshot_obj(self.context)
        volume = fake_volume.fake_volume_obj(self.context)
        provider_id = fake.PROVIDER_ID
        snapshot.volume = volume
        with mock.patch('cinder.objects.snapshot.Snapshot.save'):
            self.assertEqual({'provider_id': fake.PROVIDER_ID},
                             self._driver.create_snapshot(snapshot))
            self.assertEqual(provider_id, snapshot.provider_id)

    @mock.patch.object(TClient, 'create_snapshot', mock.Mock(
                       side_effect=exception.VolumeDriverException))
    def test_create_snapshot_failure(self):
        snapshot = fake_snapshot.fake_snapshot_obj(self.context)
        volume = fake_volume.fake_volume_obj(self.context)
        snapshot.volume = volume
        self.assertRaises(exception.VolumeDriverException,
                          self._driver.create_snapshot, snapshot)

    @mock.patch.object(TClient, 'delete_snapshot', mock.Mock())
    @mock.patch('oslo_service.loopingcall.FixedIntervalLoopingCall', new=
                cinder_utils.ZeroIntervalLoopingCall)
    def test_cleanup_cache(self):
        self.assertFalse(self._driver.cache_cleanup)
        timer = self._driver._initiate_image_cache_cleanup()
        # wait for cache cleanup to complete
        timer.wait()
        self.assertFalse(self._driver.cache_cleanup)

    @mock.patch.object(TClient, 'delete_snapshot', mock.Mock(
                       side_effect=exception.VolumeDriverException))
    @mock.patch('oslo_service.loopingcall.FixedIntervalLoopingCall', new=
                cinder_utils.ZeroIntervalLoopingCall)
    def test_cleanup_cache_delete_fail(self):
        self.assertFalse(self._driver.cache_cleanup)
        timer = self._driver._initiate_image_cache_cleanup()
        # wait for cache cleanup to complete
        timer.wait()
        self.assertFalse(self._driver.cache_cleanup)

    @mock.patch.object(TClient, 'delete_snapshot', mock.Mock())
    def test_delete_snapshot(self):
        snapshot = fake_snapshot.fake_snapshot_obj(self.context)
        snapshot.provider_id = fake.PROVIDER_ID
        self.assertIsNone(self._driver.delete_snapshot(snapshot))

    @mock.patch.object(TClient, 'delete_snapshot', mock.Mock(
                       side_effect=exception.VolumeDriverException))
    def test_delete_snapshot_failure(self):
        snapshot = fake_snapshot.fake_snapshot_obj(self.context)
        snapshot.provider_id = fake.PROVIDER_ID
        self.assertRaises(exception.VolumeDriverException,
                          self._driver.delete_snapshot, snapshot)

    @mock.patch.object(TClient, 'clone_volume', mock.Mock())
    def test_create_volume_from_snapshot(self):
        snapshot = fake_snapshot.fake_snapshot_obj(self.context)
        volume = fake_volume.fake_volume_obj(self.context)
        self.assertEqual({'provider_location': self._provider_location},
                         self._driver.create_volume_from_snapshot(
                         volume, snapshot))

    @mock.patch.object(TClient, 'clone_volume', mock.Mock(
                       side_effect=exception.VolumeDriverException))
    def test_create_volume_from_snapshot_failure(self):
        snapshot = fake_snapshot.fake_snapshot_obj(self.context)
        volume = fake_volume.fake_volume_obj(self.context)
        self.assertRaises(exception.VolumeDriverException,
                          self._driver.create_volume_from_snapshot,
                          volume, snapshot)

    @mock.patch.object(TClient, 'clone_volume', mock.Mock())
    @mock.patch.object(TClient, 'create_snapshot', mock.Mock())
    def test_create_cloned_volume(self):
        volume = fake_volume.fake_volume_obj(self.context)
        self.assertEqual({'provider_location': self._provider_location},
                         self._driver.create_cloned_volume(volume, volume))

    @mock.patch.object(TClient, 'clone_volume', mock.Mock(
                       side_effect=exception.VolumeDriverException))
    @mock.patch.object(TClient, 'create_snapshot', mock.Mock())
    def test_create_cloned_volume_failure(self):
        volume = fake_volume.fake_volume_obj(self.context)
        self.assertRaises(exception.VolumeDriverException,
                          self._driver.create_cloned_volume, volume, volume)

    @mock.patch.object(TClient, 'clone_volume', mock.Mock())
    def test_clone_image(self):
        volume = fake_volume.fake_volume_obj(self.context)
        self.assertEqual(({'provider_location': self._provider_location,
                           'bootable': True}, True),
                         self._driver.clone_image(
                         None, volume, 'image-name', FakeImage().__dict__,
                         None))

    @mock.patch.object(TClient, 'clone_volume', mock.Mock(
                       side_effect=exception.VolumeDriverException))
    def test_clone_image_failure(self):
        volume = fake_volume.fake_volume_obj(self.context)
        self.assertEqual(({'provider_location': None,
                           'bootable': False}, False),
                         self._driver.clone_image(
                         None, volume, 'image-name', FakeImage().__dict__,
                         None))

    def test_manage_existing(self):
        volume = fake_volume.fake_volume_obj(self.context)
        existing = {'source-name': self._provider_location + '/' +
                    volume.name}
        with mock.patch('os.path.isfile', return_value=True):
            self.assertEqual({'provider_location': self._provider_location},
                             self._driver.manage_existing(volume, existing))

    def test_manage_existing_invalid_ref(self):
        existing = fake_volume.fake_volume_obj(self.context)
        volume = fake_volume.fake_volume_obj(self.context)
        self.assertRaises(exception.ManageExistingInvalidReference,
                          self._driver.manage_existing, volume, existing)

    def test_manage_existing_not_found(self):
        volume = fake_volume.fake_volume_obj(self.context)
        existing = {'source-name': self._provider_location + '/' +
                    volume.name}
        with mock.patch('os.path.isfile', return_value=False):
            self.assertRaises(exception.ManageExistingInvalidReference,
                              self._driver.manage_existing, volume, existing)

    @mock.patch.object(TintriDriver, '_move_file', mock.Mock(
        return_value=False))
    def test_manage_existing_move_failure(self):
        volume = fake_volume.fake_volume_obj(self.context)
        existing = {'source-name': self._provider_location + '/source-volume'}
        with mock.patch('os.path.isfile', return_value=True):
            self.assertRaises(exception.VolumeDriverException,
                              self._driver.manage_existing,
                              volume, existing)

    @ddt.data((123, 123), (123.5, 124))
    @ddt.unpack
    def test_manage_existing_get_size(self, st_size, exp_size):
        volume = fake_volume.fake_volume_obj(self.context)
        existing = {'source-name': self._provider_location + '/' +
                    volume.name}
        file = mock.Mock(st_size=int(st_size * units.Gi))
        with mock.patch('os.path.isfile', return_value=True):
            with mock.patch('os.stat', return_value=file):
                self.assertEqual(exp_size,
                                 self._driver.manage_existing_get_size(
                                     volume, existing))

    def test_manage_existing_get_size_failure(self):
        volume = fake_volume.fake_volume_obj(self.context)
        existing = {'source-name': self._provider_location + '/' +
                    volume.name}
        with mock.patch('os.path.isfile', return_value=True):
            with mock.patch('os.stat', side_effect=OSError):
                self.assertRaises(exception.VolumeDriverException,
                                  self._driver.manage_existing_get_size,
                                  volume, existing)

    def test_unmanage(self):
        volume = fake_volume.fake_volume_obj(self.context)
        volume.provider_location = self._provider_location
        self._driver.unmanage(volume)

    def test_retype(self):
        volume = fake_volume.fake_volume_obj(self.context)
        retype, update = self._driver.retype(None, volume, None, None, None)
        self.assertTrue(retype)
        self.assertIsNone(update)
