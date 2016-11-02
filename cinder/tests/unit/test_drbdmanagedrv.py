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

import collections
import eventlet
import six
import sys
import time

import mock
from oslo_utils import importutils
from oslo_utils import timeutils

from cinder import context
from cinder import test
from cinder.volume import configuration as conf


class mock_dbus(object):
    def __init__(self):
        pass

    @staticmethod
    def Array(defaults, signature=None):
        return defaults


class mock_dm_consts(object):

    TQ_GET_PATH = "get_path"

    NODE_ADDR = "addr"

    CSTATE_PREFIX = "cstate:"
    TSTATE_PREFIX = "tstate:"

    FLAG_UPD_POOL = "upd_pool"
    FLAG_UPDATE = "update"
    FLAG_DRBDCTRL = "drbdctrl"
    FLAG_STORAGE = "storage"
    FLAG_EXTERNAL = "external"
    FLAG_DEPLOY = "deploy"

    FLAG_DISKLESS = "diskless"
    FLAG_CONNECT = "connect"
    FLAG_UPD_CON = "upd_con"
    FLAG_RECONNECT = "reconnect"
    FLAG_OVERWRITE = "overwrite"
    FLAG_DISCARD = "discard"
    FLAG_UPD_CONFIG = "upd_config"
    FLAG_STANDBY = "standby"
    FLAG_QIGNORE = "qignore"
    FLAG_REMOVE = "remove"

    AUX_PROP_PREFIX = "aux:"

    BOOL_TRUE = "true"
    BOOL_FALSE = "false"

    VOL_ID = "vol_id"


class mock_dm_exc(object):

    DM_SUCCESS = 0
    DM_INFO = 1
    DM_EEXIST = 101
    DM_ENOENT = 102
    DM_ERROR = 1000


class mock_dm_utils(object):

    @staticmethod
    def _aux_prop_name(key):
        if six.text_type(key).startswith(mock_dm_consts.AUX_PROP_PREFIX):
            return key[len(mock_dm_consts.AUX_PROP_PREFIX):]
        else:
            return None

    @staticmethod
    def aux_props_to_dict(props):
        aux_props = {}
        for (key, val) in props.items():
            aux_key = mock_dm_utils._aux_prop_name(key)
            if aux_key is not None:
                aux_props[aux_key] = val
        return aux_props

    @staticmethod
    def dict_to_aux_props(props):
        aux_props = {}
        for (key, val) in props.items():
            aux_key = mock_dm_consts.AUX_PROP_PREFIX + six.text_type(key)
            aux_props[aux_key] = six.text_type(val)
        return aux_props


def public_keys(c):
    return [n for n in c.__dict__.keys() if not n.startswith("_")]


sys.modules['dbus'] = mock_dbus
sys.modules['drbdmanage'] = collections.namedtuple(
    'module', ['consts', 'exceptions', 'utils'])
sys.modules['drbdmanage.utils'] = collections.namedtuple(
    'module', public_keys(mock_dm_utils))
sys.modules['drbdmanage.consts'] = collections.namedtuple(
    'module', public_keys(mock_dm_consts))
sys.modules['drbdmanage.exceptions'] = collections.namedtuple(
    'module', public_keys(mock_dm_exc))

import cinder.volume.drivers.drbdmanagedrv as drv

drv.dbus = mock_dbus
drv.dm_const = mock_dm_consts
drv.dm_utils = mock_dm_utils
drv.dm_exc = mock_dm_exc


def create_configuration(object):
    configuration = mock.MockObject(conf.Configuration)
    configuration.san_is_local = False
    configuration.append_config_values(mock.IgnoreArg())
    return configuration


class DrbdManageFakeDriver(object):

    resources = {}

    def __init__(self):
        self.calls = []

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


class DrbdManageIscsiTestCase(test.TestCase):

    def _fake_safe_get(self, key):
        if key == 'iscsi_helper':
            return 'fake'

        if key.endswith('_policy'):
            return '{}'

        return None

    @staticmethod
    def _fake_sleep(amount):
        pass

    def setUp(self):
        self.ctxt = context.get_admin_context()
        self._mock = mock.Mock()
        self.configuration = mock.Mock(conf.Configuration)
        self.configuration.san_is_local = True
        self.configuration.reserved_percentage = 1

        super(DrbdManageIscsiTestCase, self).setUp()

        self.stubs.Set(importutils, 'import_object',
                       self.fake_import_object)
        self.stubs.Set(drv.DrbdManageBaseDriver,
                       'call_or_reconnect',
                       self.fake_issue_dbus_call)
        self.stubs.Set(drv.DrbdManageBaseDriver,
                       'dbus_connect',
                       self.fake_issue_dbus_connect)
        self.stubs.Set(drv.DrbdManageBaseDriver,
                       '_wait_for_node_assignment',
                       self.fake_wait_node_assignment)

        self.configuration.safe_get = self._fake_safe_get

        self.stubs.Set(eventlet, 'sleep', self._fake_sleep)

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
        self.assertEqual("create_resource", dmd.odm.calls[0][0])
        self.assertEqual("list_volumes", dmd.odm.calls[1][0])
        self.assertEqual("create_volume", dmd.odm.calls[2][0])
        self.assertEqual(1048576, dmd.odm.calls[2][2])
        self.assertEqual("auto_deploy", dmd.odm.calls[3][0])
        self.assertEqual(5, len(dmd.odm.calls))

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
        self.assertEqual(6, len(dmd.odm.calls))
        self.assertEqual("create_resource", dmd.odm.calls[0][0])
        self.assertEqual("list_volumes", dmd.odm.calls[1][0])
        self.assertEqual("create_volume", dmd.odm.calls[2][0])
        self.assertEqual(1048576, dmd.odm.calls[2][2])
        self.assertEqual("auto_deploy", dmd.odm.calls[3][0])
        self.assertEqual("run_external_plugin", dmd.odm.calls[4][0])
        self.assertEqual("assign", dmd.odm.calls[5][0])

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
        self.assertEqual("list_volumes", dmd.odm.calls[0][0])
        self.assertEqual(testvol['id'], dmd.odm.calls[0][3]["aux:cinder-id"])
        self.assertEqual("remove_volume", dmd.odm.calls[1][0])

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

    def test_create_snapshot(self):
        testsnap = {'id': 'ca253fd0-8068-11e4-98c0-5254008ea111',
                    'volume_id': 'ba253fd0-8068-11e4-98c0-5254008ea111'}

        dmd = drv.DrbdManageIscsiDriver(configuration=self.configuration)
        dmd.odm = DrbdManageFakeDriver()
        dmd.create_snapshot(testsnap)
        self.assertEqual("list_volumes", dmd.odm.calls[0][0])
        self.assertEqual("list_assignments", dmd.odm.calls[1][0])
        self.assertEqual("create_snapshot", dmd.odm.calls[2][0])
        self.assertTrue('node' in dmd.odm.calls[2][3])

    def test_delete_snapshot(self):
        testsnap = {'id': 'ca253fd0-8068-11e4-98c0-5254008ea111'}

        dmd = drv.DrbdManageIscsiDriver(configuration=self.configuration)
        dmd.odm = DrbdManageFakeDriver()
        dmd.delete_snapshot(testsnap)
        self.assertEqual("list_snapshots", dmd.odm.calls[0][0])
        self.assertEqual("remove_snapshot", dmd.odm.calls[1][0])

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
        self.assertEqual("list_volumes", dmd.odm.calls[0][0])
        self.assertEqual(testvol['id'], dmd.odm.calls[0][3]["aux:cinder-id"])
        self.assertEqual("resize_volume", dmd.odm.calls[1][0])
        self.assertEqual("res", dmd.odm.calls[1][1])
        self.assertEqual(2, dmd.odm.calls[1][2])
        self.assertEqual(-1, dmd.odm.calls[1][3])
        self.assertEqual(5242880, dmd.odm.calls[1][4])

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
        self.assertEqual("list_volumes", dmd.odm.calls[0][0])
        self.assertEqual("list_assignments", dmd.odm.calls[1][0])
        self.assertEqual("create_snapshot", dmd.odm.calls[2][0])
        self.assertEqual("run_external_plugin", dmd.odm.calls[3][0])
        self.assertEqual("list_snapshots", dmd.odm.calls[4][0])
        self.assertEqual("restore_snapshot", dmd.odm.calls[5][0])
        self.assertEqual("run_external_plugin", dmd.odm.calls[6][0])
        self.assertEqual("list_snapshots", dmd.odm.calls[7][0])
        self.assertEqual("remove_snapshot", dmd.odm.calls[8][0])

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
        self.assertEqual("list_volumes", dmd.odm.calls[0][0])
        self.assertEqual("list_assignments", dmd.odm.calls[1][0])
        self.assertEqual("create_snapshot", dmd.odm.calls[2][0])
        self.assertEqual("run_external_plugin", dmd.odm.calls[3][0])
        self.assertEqual("list_snapshots", dmd.odm.calls[4][0])
        self.assertEqual("restore_snapshot", dmd.odm.calls[5][0])
        self.assertEqual("run_external_plugin", dmd.odm.calls[6][0])
        # resize image checks
        self.assertEqual("list_volumes", dmd.odm.calls[7][0])
        self.assertEqual(newvol['id'], dmd.odm.calls[7][3]["aux:cinder-id"])
        self.assertEqual("resize_volume", dmd.odm.calls[8][0])
        self.assertEqual("res", dmd.odm.calls[8][1])
        self.assertEqual(2, dmd.odm.calls[8][2])
        self.assertEqual(-1, dmd.odm.calls[8][3])
        self.assertEqual(5242880, dmd.odm.calls[8][4])

        self.assertEqual("run_external_plugin", dmd.odm.calls[9][0])
        self.assertEqual("list_snapshots", dmd.odm.calls[10][0])
        self.assertEqual("remove_snapshot", dmd.odm.calls[11][0])

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
        self.assertEqual("list_snapshots", dmd.odm.calls[0][0])
        self.assertEqual("restore_snapshot", dmd.odm.calls[1][0])
        self.assertEqual("run_external_plugin", dmd.odm.calls[2][0])

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
        self.assertEqual("list_snapshots", dmd.odm.calls[0][0])
        self.assertEqual("restore_snapshot", dmd.odm.calls[1][0])
        self.assertEqual("run_external_plugin", dmd.odm.calls[2][0])

        # resize image checks
        self.assertEqual("list_volumes", dmd.odm.calls[3][0])
        self.assertEqual(newvol['id'], dmd.odm.calls[3][3]["aux:cinder-id"])
        self.assertEqual("resize_volume", dmd.odm.calls[4][0])
        self.assertEqual("res", dmd.odm.calls[4][1])
        self.assertEqual(2, dmd.odm.calls[4][2])
        self.assertEqual(-1, dmd.odm.calls[4][3])
        self.assertEqual(5242880, dmd.odm.calls[4][4])


class DrbdManageDrbdTestCase(DrbdManageIscsiTestCase):

    def setUp(self):
        super(DrbdManageDrbdTestCase, self).setUp()

        self.stubs.Set(drv.DrbdManageDrbdDriver,
                       '_is_external_node',
                       self.fake_is_external_node)

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
        self.assertEqual("list_volumes", dmd.odm.calls[0][0])
        self.assertEqual("create_node", dmd.odm.calls[1][0])
        self.assertEqual("assign", dmd.odm.calls[2][0])
        # local_path
        self.assertEqual("list_volumes", dmd.odm.calls[3][0])
        self.assertEqual("text_query", dmd.odm.calls[4][0])
        self.assertEqual("local", x["driver_volume_type"])


class DrbdManageCommonTestCase(DrbdManageIscsiTestCase):
    def setUp(self):
        super(DrbdManageCommonTestCase, self).setUp()

    def test_drbd_policy_loop_timeout(self):
        dmd = drv.DrbdManageDrbdDriver(configuration=self.configuration)
        dmd.odm = DrbdManageFakeDriver()

        res = dmd._call_policy_plugin('void', {},
                                      {'retry': 4,
                                       'run-into-timeout': True})
        self.assertFalse(res)
        self.assertEqual(4, len(dmd.odm.calls))
        self.assertEqual("run_external_plugin", dmd.odm.calls[0][0])
        self.assertEqual("run_external_plugin", dmd.odm.calls[1][0])
        self.assertEqual("run_external_plugin", dmd.odm.calls[2][0])
        self.assertEqual("run_external_plugin", dmd.odm.calls[3][0])

    def test_drbd_policy_loop_success(self):
        dmd = drv.DrbdManageDrbdDriver(configuration=self.configuration)
        dmd.odm = DrbdManageFakeDriver()

        res = dmd._call_policy_plugin('void',
                                      {'base': 'data',
                                       'retry': 4},
                                      {'override': 'xyz'})
        self.assertTrue(res)
        self.assertEqual(4, len(dmd.odm.calls))
        self.assertEqual("run_external_plugin", dmd.odm.calls[0][0])
        self.assertEqual("run_external_plugin", dmd.odm.calls[1][0])
        self.assertEqual("run_external_plugin", dmd.odm.calls[2][0])
        self.assertEqual("run_external_plugin", dmd.odm.calls[3][0])

    def test_drbd_policy_loop_simple(self):
        dmd = drv.DrbdManageDrbdDriver(configuration=self.configuration)
        dmd.odm = DrbdManageFakeDriver()

        res = dmd._call_policy_plugin('policy-name',
                                      {'base': "value",
                                       'over': "ignore"},
                                      {'over': "ride",
                                       'starttime': 0})
        self.assertTrue(res)

        self.assertEqual(1, len(dmd.odm.calls))
        self.assertEqual("run_external_plugin", dmd.odm.calls[0][0])
        self.assertEqual('policy-name', dmd.odm.calls[0][1])

        incoming = dmd.odm.calls[0][2]
        self.assertGreaterEqual(4, abs(float(incoming['starttime']) -
                                       time.time()))
        self.assertEqual('value', incoming['base'])
        self.assertEqual('ride', incoming['over'])
