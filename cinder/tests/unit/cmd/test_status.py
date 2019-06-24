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

"""Unit tests for the cinder-status CLI interfaces."""

import ddt
import mock

from oslo_config import cfg
from oslo_upgradecheck import upgradecheck as uc
import testtools

from cinder.cmd import status

import cinder.volume.manager as volume_manager

CONF = cfg.CONF


@ddt.ddt
class TestCinderStatus(testtools.TestCase):
    """Test cases for the cinder-status upgrade check command."""

    def setUp(self):
        super(TestCinderStatus, self).setUp()
        self.checks = status.Checks()

        # Make sure configuration is initialized
        try:
            CONF([], project='cinder')
        except cfg.RequiredOptError:
            # Doesn't matter in this situation
            pass

        # Make sure our expected path is returned
        patcher = mock.patch.object(CONF, 'find_file')
        self.addCleanup(patcher.stop)
        self.find_file = patcher.start()
        self.find_file.return_value = '/etc/cinder/'

    def _set_config(self, key, value, group=None):
        CONF.set_override(key, value, group=group)
        self.addCleanup(CONF.clear_override, key, group=group)

    def _set_backup_driver(self, driver_path):
        CONF.set_override('backup_driver', driver_path)
        self.addCleanup(CONF.clear_override, 'backup_driver')

    def _set_volume_driver(self, volume_driver, enabled_backend):
        CONF.register_opts(volume_manager.volume_backend_opts,
                           group=enabled_backend)
        CONF.set_override('enabled_backends', enabled_backend)
        CONF.set_override('volume_driver', volume_driver,
                          group=enabled_backend)
        self.addCleanup(CONF.clear_override, 'volume_driver',
                        group=enabled_backend)
        self.addCleanup(CONF.clear_override, 'enabled_backends')

    def test_check_backup_module(self):
        self._set_config(
            'backup_driver',
            'cinder.backup.drivers.swift.SwiftBackupDriver')
        result = self.checks._check_backup_module()
        self.assertEqual(uc.Code.SUCCESS, result.code)

    def test_check_backup_module_not_class(self):
        self._set_config('backup_driver', 'cinder.backup.drivers.swift')
        result = self.checks._check_backup_module()
        self.assertEqual(uc.Code.FAILURE, result.code)
        self.assertIn('requires the full path', result.details)

    def test_check_policy_file(self):
        with mock.patch.object(self.checks, '_file_exists') as fe:
            fe.return_value = False
            result = self.checks._check_policy_file()

        self.assertEqual(uc.Code.SUCCESS, result.code)

    def test_check_policy_file_exists(self):
        with mock.patch.object(self.checks, '_file_exists') as fe:
            fe.return_value = True
            result = self.checks._check_policy_file()

        self.assertEqual(uc.Code.WARNING, result.code)
        self.assertIn('policy.json file is present', result.details)

    def test_check_policy_file_custom_path(self):
        policy_path = '/my/awesome/configs/policy.yaml'
        self._set_config('policy_file', policy_path, group='oslo_policy')
        with mock.patch.object(self.checks, '_file_exists') as fe:
            fe.return_value = False
            result = self.checks._check_policy_file()
            fe.assert_called_with(policy_path)

        self.assertEqual(uc.Code.WARNING, result.code)
        self.assertIn(policy_path, result.details)

    def test_check_policy_file_custom_file(self):
        policy_path = 'mypolicy.yaml'
        self._set_config('policy_file', policy_path, group='oslo_policy')
        with mock.patch.object(self.checks, '_file_exists') as fe:
            fe.return_value = False
            result = self.checks._check_policy_file()
            fe.assert_called_with('/etc/cinder/%s' % policy_path)

        self.assertEqual(uc.Code.WARNING, result.code)
        self.assertIn(policy_path, result.details)

    def test_check_legacy_win_conf_fail(self):
        self._set_volume_driver(
            'cinder.volume.drivers.windows.windows.WindowsDriver',
            'winiscsi')
        result = self.checks._check_legacy_windows_config()
        self.assertEqual(uc.Code.FAILURE, result.code)
        self.assertIn('Please update to use', result.details)

    def test_check_legacy_win_conf_no_drivers(self):
        self._set_config('enabled_backends', None)
        result = self.checks._check_legacy_windows_config()
        self.assertEqual(uc.Code.SUCCESS, result.code)

    def test_check_removed_drivers(self):
        self._set_volume_driver(
            'cinder.volume.drivers.lvm.LVMVolumeDriver',
            'winiscsi')
        result = self.checks._check_removed_drivers()
        self.assertEqual(uc.Code.SUCCESS, result.code)

    @ddt.data('cinder.volume.drivers.coprhd.fc.EMCCoprHDFCDriver',
              'cinder.volume.drivers.coprhd.iscsi.EMCCoprHDISCSIDriver',
              'cinder.volume.drivers.coprhd.scaleio.EMCCoprHDScaleIODriver',
              'cinder.volume.drivers.disco.disco.DiscoDriver',
              'cinder.volume.drivers.hgst.HGSTDriver')
    def test_check_removed_drivers_fail(self, volume_driver):
        self._set_volume_driver(
            volume_driver,
            'testDriver')
        result = self.checks._check_removed_drivers()
        self.assertEqual(uc.Code.FAILURE, result.code)
        self.assertIn(volume_driver, result.details)
        # Check for singular version of result message
        self.assertIn('This driver has been removed', result.details)

    def test_check_multiple_removed_drivers_fail(self):
        d1 = 'cinder.volume.drivers.coprhd.fc.EMCCoprHDFCDriver'
        d3 = 'cinder.volume.drivers.coprhd.scaleio.EMCCoprHDScaleIODriver'
        d5 = 'cinder.volume.drivers.hgst.HGSTDriver'
        d2 = 'cinder.volume.drivers.foo.iscsi.FooDriver'
        d4 = 'cinder.volume.drivers.bar.fc.BarFCDriver'
        self._set_volume_driver(d1, 'b1')
        self._set_volume_driver(d2, 'b2')
        self._set_volume_driver(d3, 'b3')
        self._set_volume_driver(d4, 'b4')
        self._set_volume_driver(d5, 'b5')
        CONF.set_override('enabled_backends', 'b1,b2,b3,b4,b5')
        result = self.checks._check_removed_drivers()
        self.assertEqual(uc.Code.FAILURE, result.code)
        self.assertIn(d1, result.details)
        self.assertIn(d3, result.details)
        self.assertIn(d5, result.details)
        self.assertNotIn(d2, result.details)
        self.assertNotIn(d4, result.details)
        # check for plural version of result message
        self.assertIn('The following drivers', result.details)

    def test_check_removed_drivers_no_drivers(self):
        self._set_config('enabled_backends', None)
        result = self.checks._check_removed_drivers()
        self.assertEqual(uc.Code.SUCCESS, result.code)
