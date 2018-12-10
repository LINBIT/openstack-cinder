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

from sqlalchemy import Column, MetaData, String, Table


def upgrade(migrate_engine):
    """Add encryption_key_id column to Backups."""
    meta = MetaData()
    meta.bind = migrate_engine
    backups = Table('backups', meta, autoload=True)

    encryption_key_id = Column('encryption_key_id', String(length=36))
    if not hasattr(backups.c, 'encryption_key_id'):
        backups.create_column(encryption_key_id)
