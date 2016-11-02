# Copyright (c) 2014 LINBIT HA Solutions GmbH
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

import sys
import time

import mock
from oslo_utils import importutils
from oslo_utils import timeutils

from cinder import context
from cinder import test
from cinder.volume import configuration as conf


try:
    import cinder.volume.drivers.drbdmanagedrv as drv
    from drbdmanage import clienthelper
    from drbdmanage.clienthelper import DrbdManageClientHelper as dm_c_h
    import drbdmanage.consts as mock_dm_consts
    import drbdmanage.exceptions as mock_dm_exc

except Exception as e:
    sys.stderr.write("DRBD Manage lib loading: exception %s\n" % e)
    drv = None

skip_unless_drbdmanage_installed = test.testtools.skipIf(
    drv is None,
    "No DRBD Manage installed")


class DrbdManageFakeDriver(object):

    def __init__(self):
        self.calls = []
        self.cur = -1

    def call_count(self):
        return len(self.calls)

    def next_call(self):
        self.cur += 1
        return self.calls[self.cur][0]

    def call_parm(self, arg_idx):
        return self.calls[self.cur][arg_idx]

    def run_external_plugin(self, name, props):
        self.calls.append(["run_external_plugin", name, props])

        call_okay = [[mock_dm_exc.DM_SUCCESS, "ACK", []]]
        not_done_yet = (call_okay,
                        dict(timeout=mock_dm_consts.BOOL_FALSE,
                             result=mock_dm_consts.BOOL_FALSE))
        success = (call_okay,
                   dict(timeout=mock_dm_consts.BOOL_FALSE,
                        result=mock_dm_consts.BOOL_TRUE))
        got_timeout = (call_okay,
                       dict(timeout=mock_dm_consts.BOOL_TRUE,
                            result=mock_dm_consts.BOOL_FALSE))

        if "retry" not in props:
            # Fake success, to not slow tests down
            return success

        if props["retry"] > 1:
            props["retry"] -= 1
            return not_done_yet

        if props.get("run-into-timeout"):
            return got_timeout

        return success

    def list_resources(self, res, serial, prop, req):
        self.calls.append(["list_resources", res, prop, req])
        if ('aux:cinder-id' in prop and
                prop['aux:cinder-id'].startswith("deadbeef")):
            return ([[mock_dm_exc.DM_ENOENT, "none", []]],
                    [])
        else:
            return ([[mock_dm_exc.DM_SUCCESS, "ACK", []]],
                    [("res", dict(prop))])

    def create_resource(self, res, props):
        self.calls.append(["create_resource", res, props])
        return [[mock_dm_exc.DM_SUCCESS, "ack", []]]

    def create_volume(self, res, size, props):
        self.calls.append(["create_volume", res, size, props])
        return [[mock_dm_exc.DM_SUCCESS, "ack", []],
                [mock_dm_exc.DM_INFO,
                 "create_volume",
                 [(mock_dm_consts.VOL_ID, '2')]]]

    def auto_deploy(self, res, red, delta, site_clients):
        self.calls.append(["auto_deploy", res, red, delta, site_clients])
        return [[mock_dm_exc.DM_SUCCESS, "ack", []] * red]

    def list_volumes(self, res, ser, prop, req):
        self.calls.append(["list_volumes", res, ser, prop, req])
        if ('aux:cinder-id' in prop and
                prop['aux:cinder-id'].startswith("deadbeef")):
            return ([[mock_dm_exc.DM_SUCCESS, "none", []]],
                    [])
        else:
            return ([[mock_dm_exc.DM_SUCCESS, "ACK", []]],
                    [("res", dict(), [(2, dict(prop))])
                     ])

    def remove_volume(self, res, nr, force):
        self.calls.append(["remove_volume", res, nr, force])
        return [[mock_dm_exc.DM_SUCCESS, "ack", []]]

    def text_query(self, cmd):
        self.calls.append(["text_query", cmd])
        if cmd[0] == mock_dm_consts.TQ_GET_PATH:
            return ([(mock_dm_exc.DM_SUCCESS, "ack", [])], ['/dev/drbd0'])
        return ([(mock_dm_exc.DM_ERROR, 'unknown command', [])], [])

    def list_assignments(self, nodes, res, ser, prop, req):
        self.calls.append(["list_assignments", nodes, res, ser, prop, req])
        if ('aux:cinder-id' in prop and
                prop['aux:cinder-id'].startswith("deadbeef")):
            return ([[mock_dm_exc.DM_SUCCESS, "none", []]],
                    [])
        else:
            return ([[mock_dm_exc.DM_SUCCESS, "ACK", []]],
                    [("node", "res", dict(), [(2, dict(prop))])
                     ])

    def create_snapshot(self, res, snap, nodes, props):
        self.calls.append(["create_snapshot", res, snap, nodes, props])
        return [[mock_dm_exc.DM_SUCCESS, "ack", []]]

    def list_snapshots(self, res, sn, serial, prop, req):
        self.calls.append(["list_snapshots", res, sn, serial, prop, req])
        if ('aux:cinder-id' in prop and
                prop['aux:cinder-id'].startswith("deadbeef")):
            return ([[mock_dm_exc.DM_SUCCESS, "none", []]],
                    [])
        else:
            return ([[mock_dm_exc.DM_SUCCESS, "ACK", []]],
                    [("res", [("snap", dict(prop))])
                     ])

    def remove_snapshot(self, res, snap, force):
        self.calls.append(["remove_snapshot", res, snap, force])
        return [[mock_dm_exc.DM_SUCCESS, "ack", []]]

    def resize_volume(self, res, vol, ser, size, delta):
        self.calls.append(["resize_volume", res, vol, ser, size, delta])
        return [[mock_dm_exc.DM_SUCCESS, "ack", []]]

    def restore_snapshot(self, res, snap, new, rprop, vprops):
        self.calls.append(["restore_snapshot", res, snap, new, rprop, vprops])
        return [[mock_dm_exc.DM_SUCCESS, "ack", []]]

    def assign(self, host, resource, props):
        self.calls.append(["assign", host, resource, props])
        return [[mock_dm_exc.DM_SUCCESS, "ack", []]]

    def create_node(self, name, prop):
        self.calls.append(["create_node", name, prop])
        if name.startswith('EXIST'):
            return [(mock_dm_exc.DM_EEXIST, "none", [])]
        else:
            return [(mock_dm_exc.DM_SUCCESS, "ack", [])]

    def set_drbdsetup_props(self, options):
        self.calls.append(["set_drbdsetup_props", options])
        return [[mock_dm_exc.DM_SUCCESS, "ack", []]]

    def modify_resource(self, res, ser, props):
        self.calls.append(["modify_resource", res, ser, props])
        return [[mock_dm_exc.DM_SUCCESS, "ack", []]]


class DrbdManageIscsiTestCase(test.TestCase):

    def _fake_safe_get(self, key):
        if key == 'iscsi_helper':
            return 'fake'

        if key.endswith('_policy'):
            return '{}'

        if key.endswith('_options'):
            return '{}'

        return None

    def _fake_safe_get_with_options(self, key):
        if key == 'drbdmanage_net_options':
            return('{"connect-int": "4", "allow-two-primaries": "yes", '
                   '"ko-count": "30"}')
        if key == 'drbdmanage_resource_options':
            return '{"auto-promote-timeout": "300"}'
        if key == 'drbdmanage_disk_options':
            return '{"c-min-rate": "4M"}'

        return self._fake_safe_get(key)

    def setUp(self):
        super(DrbdManageIscsiTestCase, self).setUp()

        if drv is None:
            return

        self.ctxt = context.get_admin_context()
        self._mock = mock.Mock()
        self.configuration = mock.Mock(conf.Configuration)
        self.configuration.san_is_local = True
        self.configuration.reserved_percentage = 1

        self.mock_object(importutils, 'import_object',
                         self.fake_import_object)
        self.mock_object(dm_c_h,
                         'call_or_reconnect',
                         self.fake_issue_dbus_call)
        self.mock_object(dm_c_h,
                         'dbus_connect',
                         self.fake_issue_dbus_connect)
        self.mock_object(drv.DrbdManageBaseDriver,
                         '_wait_for_node_assignment',
                         self.fake_wait_node_assignment)

        self.configuration.safe_get = self._fake_safe_get

        self.mock_object(clienthelper, 'delay_for', lambda s: None)

    # Infrastructure
    def fake_import_object(self, what, configuration, db, executor):
        return None

    def fake_issue_dbus_call(self, fn, *args):
        return fn(*args)

    def fake_wait_node_assignment(self, *args, **kwargs):
        return True

    def fake_issue_dbus_connect(self):
        self.odm = DrbdManageFakeDriver()

    def call_or_reconnect(self, method, *params):
        return method(*params)

    def fake_is_external_node(self, name):
        return False

    # Tests per se

    @skip_unless_drbdmanage_installed
    def test_create_volume(self):
        testvol = {'project_id': 'testprjid',
                   'name': 'testvol',
                   'size': 1,
                   'id': 'deadbeef-8068-11e4-98c0-5254008ea111',
                   'volume_type_id': 'drbdmanage',
                   'created_at': timeutils.utcnow()}

        dmd = drv.DrbdManageIscsiDriver(configuration=self.configuration)
        dmd.drbdmanage_devs_on_controller = False
        dmd.odm = DrbdManageFakeDriver()
        dmd.create_volume(testvol)
        self.assertEqual(8, dmd.odm.call_count())
        self.assertEqual("create_resource", dmd.odm.next_call())
        self.assertEqual("set_drbdsetup_props", dmd.odm.next_call())
        self.assertEqual("set_drbdsetup_props", dmd.odm.next_call())
        self.assertEqual("set_drbdsetup_props", dmd.odm.next_call())
        self.assertEqual("list_volumes", dmd.odm.next_call())
        self.assertEqual("create_volume", dmd.odm.next_call())
        self.assertEqual(1048576, dmd.odm.call_parm(2))
        self.assertEqual("auto_deploy", dmd.odm.next_call())

    @skip_unless_drbdmanage_installed
    def test_create_volume_with_options(self):
        testvol = {'project_id': 'testprjid',
                   'name': 'testvol',
                   'size': 1,
                   'id': 'deadbeef-8068-11e4-98c0-5254008ea111',
                   'volume_type_id': 'drbdmanage',
                   'created_at': timeutils.utcnow()}

        self.configuration.safe_get = self._fake_safe_get_with_options
        dmd = drv.DrbdManageIscsiDriver(configuration=self.configuration)
        dmd.drbdmanage_devs_on_controller = False
        dmd.odm = DrbdManageFakeDriver()
        dmd.create_volume(testvol)

        self.assertEqual(8, dmd.odm.call_count())

        self.assertEqual("create_resource", dmd.odm.next_call())

        self.assertEqual("set_drbdsetup_props", dmd.odm.next_call())
        self.assertEqual("reso", dmd.odm.call_parm(1)["type"])
        self.assertEqual("300", dmd.odm.call_parm(1)["auto-promote-timeout"])

        self.assertEqual("set_drbdsetup_props", dmd.odm.next_call())
        self.assertEqual("neto", dmd.odm.call_parm(1)["type"])
        self.assertEqual("30", dmd.odm.call_parm(1)["ko-count"])

        self.assertEqual("set_drbdsetup_props", dmd.odm.next_call())
        self.assertEqual("disko", dmd.odm.call_parm(1)["type"])
        self.assertEqual("4M", dmd.odm.call_parm(1)["c-min-rate"])

        self.assertEqual("list_volumes", dmd.odm.next_call())

        self.assertEqual("create_volume", dmd.odm.next_call())
        self.assertEqual(1048576, dmd.odm.call_parm(2))

        self.assertEqual("auto_deploy", dmd.odm.next_call())

    @skip_unless_drbdmanage_installed
    def test_create_volume_controller_all_vols(self):
        testvol = {'project_id': 'testprjid',
                   'name': 'testvol',
                   'size': 1,
                   'id': 'deadbeef-8068-11e4-98c0-5254008ea111',
                   'volume_type_id': 'drbdmanage',
                   'created_at': timeutils.utcnow()}

        dmd = drv.DrbdManageIscsiDriver(configuration=self.configuration)
        dmd.drbdmanage_devs_on_controller = True
        dmd.odm = DrbdManageFakeDriver()
        dmd.create_volume(testvol)
        self.assertEqual("create_resource", dmd.odm.next_call())
        self.assertEqual("set_drbdsetup_props", dmd.odm.next_call())
        self.assertEqual("set_drbdsetup_props", dmd.odm.next_call())
        self.assertEqual("set_drbdsetup_props", dmd.odm.next_call())
        self.assertEqual("list_volumes", dmd.odm.next_call())
        self.assertEqual("create_volume", dmd.odm.next_call())
        self.assertEqual(1048576, dmd.odm.call_parm(2))
        self.assertEqual("auto_deploy", dmd.odm.next_call())
        self.assertEqual("run_external_plugin", dmd.odm.next_call())
        self.assertEqual("assign", dmd.odm.next_call())
        self.assertEqual(9, dmd.odm.call_count())

    @skip_unless_drbdmanage_installed
    def test_delete_volume(self):
        testvol = {'project_id': 'testprjid',
                   'name': 'testvol',
                   'size': 1,
                   'id': 'ba253fd0-8068-11e4-98c0-5254008ea111',
                   'volume_type_id': 'drbdmanage',
                   'created_at': timeutils.utcnow()}

        dmd = drv.DrbdManageIscsiDriver(configuration=self.configuration)
        dmd.odm = DrbdManageFakeDriver()
        dmd.delete_volume(testvol)
        self.assertEqual("list_volumes", dmd.odm.next_call())
        self.assertEqual(testvol['id'], dmd.odm.call_parm(3)["aux:cinder-id"])
        self.assertEqual("remove_volume", dmd.odm.next_call())

    @skip_unless_drbdmanage_installed
    def test_local_path(self):
        testvol = {'project_id': 'testprjid',
                   'name': 'testvol',
                   'size': 1,
                   'id': 'ba253fd0-8068-11e4-98c0-5254008ea111',
                   'volume_type_id': 'drbdmanage',
                   'created_at': timeutils.utcnow()}

        dmd = drv.DrbdManageIscsiDriver(configuration=self.configuration)
        dmd.odm = DrbdManageFakeDriver()
        data = dmd.local_path(testvol)
        self.assertTrue(data.startswith("/dev/drbd"))

    @skip_unless_drbdmanage_installed
    def test_create_snapshot(self):
        testsnap = {'id': 'ca253fd0-8068-11e4-98c0-5254008ea111',
                    'volume_id': 'ba253fd0-8068-11e4-98c0-5254008ea111'}

        dmd = drv.DrbdManageIscsiDriver(configuration=self.configuration)
        dmd.odm = DrbdManageFakeDriver()
        dmd.create_snapshot(testsnap)
        self.assertEqual("list_volumes", dmd.odm.next_call())
        self.assertEqual("list_assignments", dmd.odm.next_call())
        self.assertEqual("create_snapshot", dmd.odm.next_call())
        self.assertIn('node', dmd.odm.call_parm(3))

    @skip_unless_drbdmanage_installed
    def test_delete_snapshot(self):
        testsnap = {'id': 'ca253fd0-8068-11e4-98c0-5254008ea111'}

        dmd = drv.DrbdManageIscsiDriver(configuration=self.configuration)
        dmd.odm = DrbdManageFakeDriver()
        dmd.delete_snapshot(testsnap)
        self.assertEqual("list_snapshots", dmd.odm.next_call())
        self.assertEqual("remove_snapshot", dmd.odm.next_call())

    @skip_unless_drbdmanage_installed
    def test_extend_volume(self):
        testvol = {'project_id': 'testprjid',
                   'name': 'testvol',
                   'size': 1,
                   'id': 'ba253fd0-8068-11e4-98c0-5254008ea111',
                   'volume_type_id': 'drbdmanage',
                   'created_at': timeutils.utcnow()}

        dmd = drv.DrbdManageIscsiDriver(configuration=self.configuration)
        dmd.odm = DrbdManageFakeDriver()
        dmd.extend_volume(testvol, 5)
        self.assertEqual("list_volumes", dmd.odm.next_call())
        self.assertEqual(testvol['id'], dmd.odm.call_parm(3)["aux:cinder-id"])
        self.assertEqual("resize_volume", dmd.odm.next_call())
        self.assertEqual("res", dmd.odm.call_parm(1))
        self.assertEqual(2, dmd.odm.call_parm(2))
        self.assertEqual(-1, dmd.odm.call_parm(3))
        self.assertEqual(5242880, dmd.odm.call_parm(4))

    @skip_unless_drbdmanage_installed
    def test_create_cloned_volume(self):
        srcvol = {'project_id': 'testprjid',
                  'name': 'testvol',
                  'size': 1,
                  'id': 'ba253fd0-8068-11e4-98c0-5254008ea111',
                  'volume_type_id': 'drbdmanage',
                  'created_at': timeutils.utcnow()}

        newvol = {'id': 'ca253fd0-8068-11e4-98c0-5254008ea111'}

        dmd = drv.DrbdManageIscsiDriver(configuration=self.configuration)
        dmd.odm = DrbdManageFakeDriver()
        dmd.create_cloned_volume(newvol, srcvol)
        self.assertEqual("list_volumes", dmd.odm.next_call())
        self.assertEqual("list_assignments", dmd.odm.next_call())
        self.assertEqual("create_snapshot", dmd.odm.next_call())
        self.assertEqual("run_external_plugin", dmd.odm.next_call())
        self.assertEqual("list_snapshots", dmd.odm.next_call())
        self.assertEqual("restore_snapshot", dmd.odm.next_call())
        self.assertEqual("set_drbdsetup_props", dmd.odm.next_call())
        self.assertEqual("set_drbdsetup_props", dmd.odm.next_call())
        self.assertEqual("set_drbdsetup_props", dmd.odm.next_call())
        self.assertEqual("run_external_plugin", dmd.odm.next_call())
        self.assertEqual("list_snapshots", dmd.odm.next_call())
        self.assertEqual("remove_snapshot", dmd.odm.next_call())

    @skip_unless_drbdmanage_installed
    def test_create_cloned_volume_larger_size(self):
        srcvol = {'project_id': 'testprjid',
                  'name': 'testvol',
                  'size': 1,
                  'id': 'ba253fd0-8068-11e4-98c0-5254008ea111',
                  'volume_type_id': 'drbdmanage',
                  'created_at': timeutils.utcnow()}

        newvol = {'size': 5,
                  'id': 'ca253fd0-8068-11e4-98c0-5254008ea111'}

        dmd = drv.DrbdManageIscsiDriver(configuration=self.configuration)
        dmd.odm = DrbdManageFakeDriver()
        dmd.create_cloned_volume(newvol, srcvol)
        self.assertEqual("list_volumes", dmd.odm.next_call())
        self.assertEqual("list_assignments", dmd.odm.next_call())
        self.assertEqual("create_snapshot", dmd.odm.next_call())
        self.assertEqual("run_external_plugin", dmd.odm.next_call())
        self.assertEqual("list_snapshots", dmd.odm.next_call())
        self.assertEqual("restore_snapshot", dmd.odm.next_call())
        self.assertEqual("set_drbdsetup_props", dmd.odm.next_call())
        self.assertEqual("set_drbdsetup_props", dmd.odm.next_call())
        self.assertEqual("set_drbdsetup_props", dmd.odm.next_call())
        self.assertEqual("run_external_plugin", dmd.odm.next_call())

        # resize image checks
        self.assertEqual("list_volumes", dmd.odm.next_call())
        self.assertEqual(newvol['id'], dmd.odm.call_parm(3)["aux:cinder-id"])
        self.assertEqual("resize_volume", dmd.odm.next_call())
        self.assertEqual("res", dmd.odm.call_parm(1))
        self.assertEqual(2, dmd.odm.call_parm(2))
        self.assertEqual(-1, dmd.odm.call_parm(3))
        self.assertEqual(5242880, dmd.odm.call_parm(4))

        self.assertEqual("run_external_plugin", dmd.odm.next_call())

        self.assertEqual("list_snapshots", dmd.odm.next_call())
        self.assertEqual("remove_snapshot", dmd.odm.next_call())

    @skip_unless_drbdmanage_installed
    def test_create_volume_from_snapshot(self):
        snap = {'project_id': 'testprjid',
                'name': 'testvol',
                'volume_size': 1,
                'id': 'ba253fd0-8068-11e4-98c0-5254008ea111',
                'volume_type_id': 'drbdmanage',
                'created_at': timeutils.utcnow()}

        newvol = {'id': 'ca253fd0-8068-11e4-98c0-5254008ea111'}

        dmd = drv.DrbdManageIscsiDriver(configuration=self.configuration)
        dmd.odm = DrbdManageFakeDriver()
        dmd.create_volume_from_snapshot(newvol, snap)
        self.assertEqual("list_snapshots", dmd.odm.next_call())
        self.assertEqual("restore_snapshot", dmd.odm.next_call())
        self.assertEqual("set_drbdsetup_props", dmd.odm.next_call())
        self.assertEqual("set_drbdsetup_props", dmd.odm.next_call())
        self.assertEqual("set_drbdsetup_props", dmd.odm.next_call())
        self.assertEqual("run_external_plugin", dmd.odm.next_call())

    @skip_unless_drbdmanage_installed
    def test_create_volume_from_snapshot_larger_size(self):
        snap = {'project_id': 'testprjid',
                'name': 'testvol',
                'volume_size': 1,
                'id': 'ba253fd0-8068-11e4-98c0-5254008ea111',
                'volume_type_id': 'drbdmanage',
                'created_at': timeutils.utcnow()}

        newvol = {'size': 5,
                  'id': 'ca253fd0-8068-11e4-98c0-5254008ea111'}

        dmd = drv.DrbdManageIscsiDriver(configuration=self.configuration)
        dmd.odm = DrbdManageFakeDriver()
        dmd.create_volume_from_snapshot(newvol, snap)
        self.assertEqual("list_snapshots", dmd.odm.next_call())
        self.assertEqual("restore_snapshot", dmd.odm.next_call())
        self.assertEqual("set_drbdsetup_props", dmd.odm.next_call())
        self.assertEqual("set_drbdsetup_props", dmd.odm.next_call())
        self.assertEqual("set_drbdsetup_props", dmd.odm.next_call())
        self.assertEqual("run_external_plugin", dmd.odm.next_call())


class DrbdManageDrbdTestCase(DrbdManageIscsiTestCase):

    def setUp(self):
        super(DrbdManageDrbdTestCase, self).setUp()

        self.mock_object(drv.DrbdManageDrbdDriver,
                         '_is_external_node',
                         self.fake_is_external_node)

    @skip_unless_drbdmanage_installed
    def test_drbd_create_export(self):
        volume = {'project_id': 'testprjid',
                  'name': 'testvol',
                  'size': 1,
                  'id': 'ba253fd0-8068-11e4-98c0-5254008ea111',
                  'volume_type_id': 'drbdmanage',
                  'created_at': timeutils.utcnow()}

        connector = {'host': 'node99',
                     'ip': '127.0.0.99'}

        dmd = drv.DrbdManageDrbdDriver(configuration=self.configuration)
        dmd.odm = DrbdManageFakeDriver()

        x = dmd.create_export({}, volume, connector)
        self.assertEqual("list_volumes", dmd.odm.next_call())
        self.assertEqual("create_node", dmd.odm.next_call())
        self.assertEqual("assign", dmd.odm.next_call())
        # local_path
        self.assertEqual("list_volumes", dmd.odm.next_call())
        self.assertEqual("text_query", dmd.odm.next_call())
        self.assertEqual("local", x["driver_volume_type"])


class DrbdManageCommonTestCase(DrbdManageIscsiTestCase):
    def setUp(self):
        super(DrbdManageCommonTestCase, self).setUp()

    @skip_unless_drbdmanage_installed

    def test_drbd_policy_loop_timeout(self):
        dmd = drv.DrbdManageDrbdDriver(configuration=self.configuration)
        dmd.odm = DrbdManageFakeDriver()

        res = dmd._call_policy_plugin('void', {},
                                      {'retry': 4,
                                       'run-into-timeout': True})
        self.assertFalse(res)
        self.assertEqual(4, dmd.odm.call_count())
        self.assertEqual("run_external_plugin", dmd.odm.next_call())
        self.assertEqual("run_external_plugin", dmd.odm.next_call())
        self.assertEqual("run_external_plugin", dmd.odm.next_call())
        self.assertEqual("run_external_plugin", dmd.odm.next_call())

    @skip_unless_drbdmanage_installed
    def test_drbd_policy_loop_success(self):
        dmd = drv.DrbdManageDrbdDriver(configuration=self.configuration)
        dmd.odm = DrbdManageFakeDriver()

        res = dmd._call_policy_plugin('void',
                                      {'base': 'data',
                                       'retry': 4},
                                      {'override': 'xyz'})
        self.assertTrue(res)
        self.assertEqual(4, dmd.odm.call_count())
        self.assertEqual("run_external_plugin", dmd.odm.next_call())
        self.assertEqual("run_external_plugin", dmd.odm.next_call())
        self.assertEqual("run_external_plugin", dmd.odm.next_call())
        self.assertEqual("run_external_plugin", dmd.odm.next_call())

    @skip_unless_drbdmanage_installed
    def test_drbd_policy_loop_simple(self):
        dmd = drv.DrbdManageDrbdDriver(configuration=self.configuration)
        dmd.odm = DrbdManageFakeDriver()

        res = dmd._call_policy_plugin('policy-name',
                                      {'base': "value",
                                       'over': "ignore"},
                                      {'over': "ride",
                                       'starttime': 0})
        self.assertTrue(res)

        self.assertEqual(1, dmd.odm.call_count())
        self.assertEqual("run_external_plugin", dmd.odm.next_call())
        self.assertEqual('policy-name', dmd.odm.call_parm(1))
        incoming = dmd.odm.call_parm(2)

        self.assertGreaterEqual(4, abs(float(incoming['starttime']) -
                                       time.time()))
        self.assertEqual('value', incoming['base'])
        self.assertEqual('ride', incoming['over'])
