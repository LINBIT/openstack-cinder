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

from sqlalchemy import Column, String, MetaData, Table


def upgrade(migrate_engine):
    meta = MetaData(migrate_engine)

    messages = Table('messages', meta, autoload=True)
    detail_id = Column('detail_id', String(10), nullable=True)
    action_id = Column('action_id', String(10), nullable=True)
    if not hasattr(messages.c, 'detail_id'):
        messages.create_column(detail_id)
    if not hasattr(messages.c, 'action_id'):
        messages.create_column(action_id)
