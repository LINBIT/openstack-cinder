# Copyright (c) 2016 Red Hat, Inc.
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

from sqlalchemy import Boolean, Column, MetaData, String, Table
from sqlalchemy.sql import expression


def upgrade(migrate_engine):
    """Add replication info to clusters table."""
    meta = MetaData()
    meta.bind = migrate_engine

    clusters = Table('clusters', meta, autoload=True)
    replication_status = Column('replication_status', String(length=36),
                                default="not-capable")
    active_backend_id = Column('active_backend_id', String(length=255))
    frozen = Column('frozen', Boolean, nullable=False, default=False,
                    server_default=expression.false())
    if not hasattr(clusters.c, 'replication_status'):
        clusters.create_column(replication_status)
    if not hasattr(clusters.c, 'frozen'):
        clusters.create_column(frozen)
    if not hasattr(clusters.c, 'active_backend_id'):
        clusters.create_column(active_backend_id)
