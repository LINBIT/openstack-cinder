# Copyright (C) 2013 Hewlett-Packard Development Company, L.P.
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

"""Tests for transfers table."""

from cinder import context
from cinder import db
from cinder.db.sqlalchemy import api as db_api
from cinder.db.sqlalchemy import models
from cinder import exception
from cinder import test
from cinder.tests.unit import fake_constants as fake
from cinder.tests.unit import utils


class TransfersTableTestCase(test.TestCase):
    """Test case for transfers model."""

    def setUp(self):
        super(TransfersTableTestCase, self).setUp()
        self.ctxt = context.RequestContext(user_id=fake.USER_ID,
                                           project_id=fake.PROJECT_ID)

    def _create_transfer(self, volume_id=None, source_project_id=None):
        """Create a transfer object."""
        transfer = {'display_name': 'display_name',
                    'salt': 'salt',
                    'crypt_hash': 'crypt_hash'}
        if volume_id is not None:
            transfer['volume_id'] = volume_id
        if source_project_id is not None:
            transfer['source_project_id'] = source_project_id
        return db.transfer_create(self.ctxt, transfer)['id']

    def test_transfer_create(self):
        # If the volume_id is Null a KeyError exception will be raised.
        self.assertRaises(KeyError,
                          self._create_transfer)

        volume_id = utils.create_volume(self.ctxt)['id']
        self._create_transfer(volume_id)

    def test_transfer_create_not_available(self):
        volume_id = utils.create_volume(self.ctxt, size=1,
                                        status='notavailable')['id']
        self.assertRaises(exception.InvalidVolume,
                          self._create_transfer,
                          volume_id)

    def test_transfer_get(self):
        volume_id1 = utils.create_volume(self.ctxt)['id']
        xfer_id1 = self._create_transfer(volume_id1)

        xfer = db.transfer_get(self.ctxt, xfer_id1)
        self.assertEqual(volume_id1, xfer.volume_id, "Unexpected volume_id")

        nctxt = context.RequestContext(user_id='new_user_id',
                                       project_id='new_project_id')
        self.assertRaises(exception.TransferNotFound,
                          db.transfer_get, nctxt, xfer_id1)

        xfer = db.transfer_get(nctxt.elevated(), xfer_id1)
        self.assertEqual(volume_id1, xfer.volume_id, "Unexpected volume_id")

    def test_transfer_get_all(self):
        volume_id1 = utils.create_volume(self.ctxt)['id']
        volume_id2 = utils.create_volume(self.ctxt)['id']
        self._create_transfer(volume_id1)
        self._create_transfer(volume_id2)

        self.assertRaises(exception.NotAuthorized,
                          db.transfer_get_all,
                          self.ctxt)
        xfer = db.transfer_get_all(context.get_admin_context())
        self.assertEqual(2, len(xfer), "Unexpected number of transfer records")

        xfer = db.transfer_get_all_by_project(self.ctxt, self.ctxt.project_id)
        self.assertEqual(2, len(xfer), "Unexpected number of transfer records")

        nctxt = context.RequestContext(user_id=fake.USER2_ID,
                                       project_id=fake.PROJECT2_ID)
        self.assertRaises(exception.NotAuthorized,
                          db.transfer_get_all_by_project,
                          nctxt, self.ctxt.project_id)
        xfer = db.transfer_get_all_by_project(nctxt.elevated(),
                                              self.ctxt.project_id)
        self.assertEqual(2, len(xfer), "Unexpected number of transfer records")

    def test_transfer_destroy(self):
        volume_id = utils.create_volume(self.ctxt)['id']
        volume_id2 = utils.create_volume(self.ctxt)['id']
        xfer_id1 = self._create_transfer(volume_id)
        xfer_id2 = self._create_transfer(volume_id2)

        xfer = db.transfer_get_all(context.get_admin_context())
        self.assertEqual(2, len(xfer), "Unexpected number of transfer records")
        self.assertFalse(xfer[0]['deleted'], "Deleted flag is set")

        db.transfer_destroy(self.ctxt, xfer_id1)
        xfer = db.transfer_get_all(context.get_admin_context())
        self.assertEqual(1, len(xfer), "Unexpected number of transfer records")
        self.assertEqual(xfer[0]['id'], xfer_id2,
                         "Unexpected value for Transfer id")

        nctxt = context.RequestContext(user_id=fake.USER2_ID,
                                       project_id=fake.PROJECT2_ID)
        self.assertRaises(exception.TransferNotFound,
                          db.transfer_destroy, nctxt, xfer_id2)

        db.transfer_destroy(nctxt.elevated(), xfer_id2)
        xfer = db.transfer_get_all(context.get_admin_context())
        self.assertEqual(0, len(xfer), "Unexpected number of transfer records")

    def test_transfer_accept(self):
        volume = utils.create_volume(self.ctxt)
        xfer_id = self._create_transfer(volume['id'], volume['project_id'])
        nctxt = context.RequestContext(user_id=fake.USER2_ID,
                                       project_id=fake.PROJECT2_ID)
        xfer = db.transfer_get(nctxt.elevated(), xfer_id)
        self.assertEqual(volume.project_id, xfer['source_project_id'])
        self.assertFalse(xfer['accepted'])
        self.assertIsNone(xfer['destination_project_id'])
        db.transfer_accept(nctxt.elevated(), xfer_id, fake.USER2_ID,
                           fake.PROJECT2_ID)

        xfer = db_api.model_query(
            nctxt.elevated(), models.Transfer, read_deleted='yes'
        ).filter_by(id=xfer_id).first()

        self.assertEqual(volume.project_id, xfer['source_project_id'])
        self.assertTrue(xfer['accepted'])
        self.assertEqual(fake.PROJECT2_ID, xfer['destination_project_id'])

    def test_transfer_accept_with_snapshots(self):
        volume_id = utils.create_volume(self.ctxt)['id']
        snapshot_id1 = utils.create_snapshot(self.ctxt, volume_id,
                                             status='available')['id']
        snapshot_id2 = utils.create_snapshot(self.ctxt, volume_id,
                                             status='available')['id']
        xfer_id = self._create_transfer(volume_id)
        nctxt = context.RequestContext(user_id=fake.USER2_ID,
                                       project_id=fake.PROJECT2_ID)
        db.transfer_accept(nctxt.elevated(), xfer_id, fake.USER2_ID,
                           fake.PROJECT2_ID)
        self.assertEqual(fake.PROJECT2_ID,
                         db.snapshot_get(nctxt, snapshot_id1)['project_id'])
        self.assertEqual(fake.PROJECT2_ID,
                         db.snapshot_get(nctxt, snapshot_id2)['project_id'])

    def test_transfer_accept_with_snapshots_invalid_status(self):
        volume_id = utils.create_volume(self.ctxt)['id']
        snapshot_id1 = utils.create_snapshot(self.ctxt, volume_id,
                                             status='available')['id']
        snapshot_id2 = utils.create_snapshot(self.ctxt, volume_id)['id']
        xfer_id = self._create_transfer(volume_id)
        nctxt = context.RequestContext(user_id=fake.USER2_ID,
                                       project_id=fake.PROJECT2_ID)
        self.assertRaises(exception.InvalidSnapshot, db.transfer_accept,
                          nctxt.elevated(), xfer_id, fake.USER2_ID,
                          fake.PROJECT2_ID)
        self.assertEqual(fake.PROJECT_ID,
                         db.snapshot_get(self.ctxt,
                                         snapshot_id1)['project_id'])
        self.assertEqual(fake.PROJECT_ID,
                         db.snapshot_get(self.ctxt,
                                         snapshot_id2)['project_id'])
        self.assertEqual('awaiting-transfer',
                         db.volume_get(self.ctxt, volume_id)['status'])
