# Copyright (c) 2017-2019 Dell Inc. or its subsidiaries.
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

import ast
import requests

from cinder import exception
from cinder.tests.unit.volume.drivers.dell_emc.powermax import (
    powermax_data as tpd)


class FakeLookupService(object):
    def get_device_mapping_from_network(self, initiator_wwns, target_wwns):
        return tpd.PowerMaxData.device_map


class FakeResponse(object):

    def __init__(self, status_code, return_object):
        self.status_code = status_code
        self.return_object = return_object

    def json(self):
        if self.return_object:
            return self.return_object
        else:
            raise ValueError

    def get_status_code(self):
        return self.status_code()

    def raise_for_status(self):
        if 200 <= self.status_code <= 204:
            return False
        else:
            return True


class FakeRequestsSession(object):

    def __init__(self, *args, **kwargs):
        self.data = tpd.PowerMaxData()

    def request(self, method, url, params=None, data=None):
        return_object = ''
        status_code = 200
        if method == 'GET':
            status_code, return_object = self._get_request(url, params)

        elif method == 'POST' or method == 'PUT':
            status_code, return_object = self._post_or_put(url, data)

        elif method == 'DELETE':
            status_code, return_object = self._delete(url)

        elif method == 'TIMEOUT':
            raise requests.Timeout

        elif method == 'EXCEPTION':
            raise Exception

        elif method == 'CONNECTION':
            raise requests.ConnectionError

        elif method == 'HTTP':
            raise requests.HTTPError

        elif method == 'SSL':
            raise requests.exceptions.SSLError

        elif method == 'EXCEPTION':
            raise exception.VolumeBackendAPIException

        return FakeResponse(status_code, return_object)

    def _get_request(self, url, params):
        status_code = 200
        return_object = None
        if self.data.failed_resource in url:
            status_code = 500
            return_object = self.data.job_list[2]
        elif 'sloprovisioning' in url:
            if 'volume' in url:
                return_object = self._sloprovisioning_volume(url, params)
            elif 'storagegroup' in url:
                return_object = self._sloprovisioning_sg(url)
            elif 'maskingview' in url:
                return_object = self._sloprovisioning_mv(url)
            elif 'portgroup' in url:
                return_object = self._sloprovisioning_pg(url)
            elif 'host' in url:
                return_object = self._sloprovisioning_ig(url)
            elif 'initiator' in url:
                return_object = self._sloprovisioning_initiator(url)
            elif 'service_level_demand_report' in url:
                return_object = self.data.srp_slo_details
            elif 'srp' in url:
                return_object = self.data.srp_details
            elif 'workloadtype' in url:
                return_object = self.data.workloadtype
            elif 'compressionCapable' in url:
                return_object = self.data.compression_info
            elif 'slo' in url:
                return_object = self.data.powermax_slo_details

        elif 'replication' in url:
            return_object = self._replication(url)

        elif 'system' in url:
            if 'director' in url:
                return_object = self._system_port(url)
            else:
                return_object = self._system(url)

        elif 'headroom' in url:
            return_object = self.data.headroom

        return status_code, return_object

    def _sloprovisioning_volume(self, url, params):
        return_object = self.data.volume_list[2]
        if '/private' in url:
            return_object = self.data.private_vol_details
        elif params:
            if '1' in params.values():
                return_object = self.data.volume_list[0]
            elif '2' in params.values():
                return_object = self.data.volume_list[1]
        else:
            for vol in self.data.volume_details:
                if vol['volumeId'] in url:
                    return_object = vol
                    break
        return return_object

    def _sloprovisioning_sg(self, url):
        return_object = self.data.sg_list
        for sg in self.data.sg_details:
            if sg['storageGroupId'] in url:
                return_object = sg
                break
        return return_object

    def _sloprovisioning_mv(self, url):
        if self.data.masking_view_name_i in url:
            return_object = self.data.maskingview[1]
        else:
            return_object = self.data.maskingview[0]
        return return_object

    def _sloprovisioning_pg(self, url):
        return_object = None
        for pg in self.data.portgroup:
            if pg['portGroupId'] in url:
                return_object = pg
                break
        return return_object

    def _system_port(self, url):
        return_object = None
        for port in self.data.port_list:
            if port['symmetrixPort']['symmetrixPortKey']['directorId'] in url:
                return_object = port
                break
        return return_object

    def _sloprovisioning_ig(self, url):
        return_object = None
        for ig in self.data.inititiatorgroup:
            if ig['hostId'] in url:
                return_object = ig
                break
        return return_object

    def _sloprovisioning_initiator(self, url):
        return_object = self.data.initiator_list[2]
        if self.data.wwpn1 in url:
            return_object = self.data.initiator_list[0]
        elif self.data.initiator in url:
            return_object = self.data.initiator_list[1]
        return return_object

    def _replication(self, url):
        return_object = None
        if 'storagegroup' in url:
            return_object = self._replication_sg(url)
        elif 'rdf_group' in url:
            if self.data.device_id in url:
                return_object = self.data.rdf_group_vol_details
            elif self.data.rdf_group_no in url:
                return_object = self.data.rdf_group_details
            else:
                return_object = self.data.rdf_group_list
        elif 'snapshot' in url:
            return_object = self.data.volume_snap_vx
        elif 'capabilities' in url:
            return_object = self.data.capabilities
        return return_object

    def _replication_sg(self, url):
        return_object = None
        if 'generation' in url:
            return_object = self.data.group_snap_vx
        elif 'rdf_group' in url:
            for sg in self.data.sg_rdf_details:
                if sg['storageGroupName'] in url:
                    return_object = sg
                    break
        elif 'storagegroup' in url:
            return_object = self.data.sg_details_rep[0]
        return return_object

    def _system(self, url):
        return_object = None
        if 'job' in url:
            for job in self.data.job_list:
                if job['jobId'] in url:
                    return_object = job
                    break
        elif 'version' in url:
            return_object = self.data.version_details
        else:
            for symm in self.data.symmetrix:
                if symm['symmetrixId'] in url:
                    return_object = symm
                    break
        return return_object

    def _post_or_put(self, url, payload):
        return_object = self.data.job_list[0]
        status_code = 201
        if self.data.failed_resource in url:
            status_code = 500
            return_object = self.data.job_list[2]
        elif payload:
            payload = ast.literal_eval(payload)
            if self.data.failed_resource in payload.values():
                status_code = 500
                return_object = self.data.job_list[2]
            if payload.get('executionOption'):
                status_code = 202
        return status_code, return_object

    def _delete(self, url):
        if self.data.failed_resource in url:
            status_code = 500
            return_object = self.data.job_list[2]
        else:
            status_code = 204
            return_object = None
        return status_code, return_object

    def session(self):
        return FakeRequestsSession()

    def close(self):
        pass


class FakeConfiguration(object):

    def __init__(self, emc_file=None, volume_backend_name=None,
                 interval=0, retries=0, replication_device=None, **kwargs):
        self.cinder_dell_emc_config_file = emc_file
        self.interval = interval
        self.retries = retries
        self.volume_backend_name = volume_backend_name
        self.config_group = volume_backend_name
        self.san_is_local = False
        if replication_device:
            self.replication_device = [replication_device]
        for key, value in kwargs.items():
            if key == 'san_login':
                self.san_login = value
            elif key == 'san_password':
                self.san_password = value
            elif key == 'san_ip':
                self.san_ip = value
            elif key == 'san_api_port':
                self.san_api_port = value
            elif key == 'san_rest_port':
                self.san_rest_port = value
            elif key == 'vmax_srp':
                self.vmax_srp = value
            elif key == 'vmax_service_level':
                self.vmax_service_level = value
            elif key == 'vmax_workload':
                self.vmax_workload = value
            elif key == 'vmax_port_groups':
                self.vmax_port_groups = value
            elif key == 'vmax_array':
                self.vmax_array = value
            elif key == 'use_chap_auth':
                self.use_chap_auth = value
            elif key == 'chap_username':
                self.chap_username = value
            elif key == 'chap_password':
                self.chap_password = value
            elif key == 'driver_ssl_cert_verify':
                self.driver_ssl_cert_verify = value
            elif key == 'driver_ssl_cert_path':
                self.driver_ssl_cert_path = value
            elif key == 'u4p_failover_target':
                self.u4p_failover_target = value
            elif key == 'u4p_failover_backoff_factor':
                self.u4p_failover_backoff_factor = value
            elif key == 'u4p_failover_retries':
                self.u4p_failover_retries = value
            elif key == 'u4p_failover_timeout':
                self.u4p_failover_timeout = value
            elif key == 'u4p_primary':
                self.u4p_primary = value

    def safe_get(self, key):
        try:
            return getattr(self, key)
        except Exception:
            return None

    def append_config_values(self, values):
        pass
