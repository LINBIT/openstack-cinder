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

import os
import pprint
import re

from oslo_serialization import jsonutils
import six

from cinder import test
from cinder.tests.functional import functional_helpers

PROJECT_ID = "6f70656e737461636b20342065766572"

# for pretty printing errors
pp = pprint.PrettyPrinter(indent=4)


class NoMatch(test.TestingException):
    pass


def pretty_data(data):
    data = jsonutils.dumps(jsonutils.loads(data), sort_keys=True,
                           indent=4)
    return '\n'.join(line.rstrip() for line in data.split('\n')).strip()


def objectify(data):
    if not data:
        return {}
    # templates will contain values like %(foo)s
    # throughout them. If these are inside of double quoted
    # strings, life is good, and we can treat it just like valid
    # json to load it to python.
    #
    # However we've got some fields which are ints, like
    # aggregate_id. This means we've got a snippet in the sample
    # that looks like:
    #
    #     "id": %(aggregate_id)s,
    #
    # which is not valid json, and will explode. We do a quick and
    # dirty transform of this to:
    #
    #     "id": "%(int:aggregate_id)s",
    #
    # That makes it valid data to convert to json, but keeps
    # around the information that we need to drop those strings
    # later. The regex anchors from the ': ', as all of these will
    # be top rooted keys.
    data = re.sub(r'(\: )%\((.+)\)s([^"])', r'\1"%(int:\2)s"\3', data)
    return jsonutils.loads(data)


class ApiSampleTestBase(functional_helpers._FunctionalTestBase):
    all_extensions = True
    sample_dir = None
    _project_id = True
    _use_common_volume_api_samples = False

    def __init__(self, *args, **kwargs):
        super(ApiSampleTestBase, self).__init__(*args, **kwargs)
        self.subs = {}

    def setUp(self):
        super(ApiSampleTestBase, self).setUp()
        self.api_major_version = 'v3'

        # this is used to generate sample docs
        self.generate_samples = os.getenv('GENERATE_SAMPLES') is not None

    @property
    def subs(self):
        return self._subs

    @subs.setter
    def subs(self, value):
        non_strings =  \
            {k: v for k, v in value.items() if
             (not k == 'volume_host') and
             (not isinstance(v, six.string_types))}
        if len(non_strings) > 0:
            raise TypeError("subs can't contain non-string values:"
                            "\n%(non_strings)s" %
                            {'non_strings': non_strings})
        else:
            self._subs = value

    @classmethod
    def _get_sample_path(cls, name, dirname, suffix='', api_version=None):
        parts = [dirname]
        parts.append('samples')
        # Note: if _use_common_volume_api_samples is set to True
        # then common volume sample files present in 'volumes' directory
        # will be used. As of now it is being used for volume POST request
        # to avoid duplicate copy of volume req and resp sample files.
        # Example - VolumesSampleBase's _create_volume method.
        if cls._use_common_volume_api_samples:
            parts.append('volumes')
        else:
            parts.append(cls.sample_dir)
            if api_version:
                parts.append('v' + api_version)
        parts.append(name + ".json" + suffix)
        return os.path.join(*parts)

    @classmethod
    def _get_sample(cls, name, api_version=None):
        dirname = os.path.dirname(os.path.abspath(__file__))
        dirname = os.path.normpath(os.path.join(dirname,
                                                "../../../api-ref/source/v3"))
        return cls._get_sample_path(name, dirname, api_version=api_version)

    @classmethod
    def _get_template(cls, name, api_version=None):
        dirname = os.path.dirname(os.path.abspath(__file__))
        dirname = os.path.normpath(os.path.join(dirname,
                                   "./api_sample_tests"))
        return cls._get_sample_path(name, dirname, suffix='.tpl',
                                    api_version=api_version)

    def _read_template(self, name):
        template = self._get_template(name)
        with open(template) as inf:
            return inf.read().strip()

    def _write_template(self, name, data):
        with open(self._get_template(name), 'w') as outf:
            outf.write(data)

    def _write_sample(self, name, data):
        with open(self._get_sample(name), 'w') as outf:
            outf.write(data)

    def _compare_dict(self, expected, result, result_str, matched_value):
        if not isinstance(result, dict):
            raise NoMatch('%(result_str)s: %(result)s is not a dict.'
                          % {'result_str': result_str, 'result': result})
        ex_keys = sorted(expected.keys())
        res_keys = sorted(result.keys())
        if ex_keys != res_keys:
            ex_delta = []
            res_delta = []
            for key in ex_keys:
                if key not in res_keys:
                    ex_delta.append(key)
            for key in res_keys:
                if key not in ex_keys:
                    res_delta.append(key)
            raise NoMatch(
                'Dictionary key mismatch:\n'
                'Extra key(s) in template:\n%(ex_delta)s\n'
                'Extra key(s) in %(result_str)s:\n%(res_delta)s\n' %
                {'ex_delta': ex_delta, 'result_str': result_str,
                 'res_delta': res_delta})
        for key in ex_keys:
            res = self._compare_result(expected[key], result[key],
                                       result_str)
            matched_value = res or matched_value
        return matched_value

    def _compare_list(self, expected, result, result_str, matched_value):
        if not isinstance(result, list):
            raise NoMatch(
                '%(result_str)s: %(result)s is not a list.' %
                {'result_str': result_str, 'result': result})

        expected = expected[:]
        extra = []

        # if it's a list of 1, do the simple compare which gives a
        # better error message.
        if len(result) == len(expected) == 1:
            return self._compare_result(expected[0], result[0], result_str)

        # This is clever enough to need some explanation. What we
        # are doing here is looping the result list, and trying to
        # compare it to every item in the expected list. If there
        # is more than one, we're going to get fails. We ignore
        # those. But every time we match an expected we drop it,
        # and break to the next iteration. Every time we hit the
        # end of the iteration, we add our results into a bucket
        # of non matched.
        #
        # This results in poor error messages because we don't
        # really know why the elements failed to match each
        # other. A more complicated diff might be nice.
        for res_obj in result:
            for i, ex_obj in enumerate(expected):
                try:
                    matched_value = self._compare_result(ex_obj,
                                                         res_obj,
                                                         result_str)
                    del expected[i]
                    break
                except NoMatch:
                    pass
            else:
                extra.append(res_obj)

        error = []
        if expected:
            error.append('Extra list items in template:')
            error.extend([repr(o) for o in expected])

        if extra:
            error.append('Extra list items in %(result_str)s:' %
                         {'result_str': result_str})
            error.extend([repr(o) for o in extra])

        if error:
            raise NoMatch('\n'.join(error))
        return matched_value

    def _compare_template(self, expected, result, result_str, matched_value):
        # escape stuff for regex
        for char in '[]<>?':
            expected = expected.replace(char, '\\%s' % char)
        # special handling of subs that are not quoted. We are
        # expecting an int but we had to pass in a string
        # so the json would parse properly.
        if expected.startswith("%(int:"):
            result = str(result)
            expected = expected.replace('int:', '')

        expected = expected % self.subs
        expected = '^%s$' % expected
        match = re.match(expected, result)
        if not match:
            raise NoMatch(
                'Values do not match:\n'
                'Template: %(expected)s\n%(result_str)s: %(result)s' %
                {'expected': expected, 'result_str': result_str,
                 'result': result})
        try:
            matched_value = match.group()
        except IndexError:
            if match.groups():
                matched_value = match.groups()[0]
        return matched_value

    def _compare_result(self, expected, result, result_str):

        matched_value = None
        # None
        if expected is None:
            if result is None:
                pass
            else:
                raise NoMatch('%(result_str)s: Expected None, got %(result)s.'
                              % {'result_str': result_str, 'result': result})
        # dictionary
        elif isinstance(expected, dict):
            matched_value = self._compare_dict(
                expected, result, result_str, matched_value)
        # list
        elif isinstance(expected, list):
            matched_value = self._compare_list(
                expected, result, result_str, matched_value)
        # template string
        elif isinstance(expected, six.string_types) and '%' in expected:
            if expected[-1] == '%':
                if result != expected:
                    raise NoMatch(
                        'Values do not match:\n'
                        'Template: %(expected)s\n%(result_str)s: '
                        '%(result)s' % {'expected': expected,
                                        'result_str': result_str,
                                        'result': result})
            else:
                matched_value = self._compare_template(
                    expected, result, result_str, matched_value)
        # string
        elif isinstance(expected, six.string_types):

            # Ignore whitespace in this comparison
            expected = expected.strip()
            if isinstance(result, six.string_types):
                result = result.strip()

            if expected != result:
                raise NoMatch(
                    'Values do not match:\n'
                    'Template: %(expected)s\n%(result_str)s: '
                    '%(result)s' % {'expected': expected,
                                    'result_str': result_str,
                                    'result': result})
        # int
        elif isinstance(expected, (six.integer_types, float)):
            if expected != result:
                raise NoMatch(
                    'Values do not match:\n'
                    'Template: %(expected)s\n%(result_str)s: '
                    '%(result)s' % {'expected': expected,
                                    'result_str': result_str,
                                    'result': result})

        else:
            raise ValueError(
                'Unexpected type %(expected_type)s'
                % {'expected_type': type(expected)})

        return matched_value

    def generalize_subs(self, subs, vanilla_regexes):
        """Give the test a chance to modify subs after the server response

        was verified, and before the on-disk doc/samples file is checked.
        This may be needed by some tests to convert exact matches expected
        from the server into pattern matches to verify what is in the
        sample file.

        If there are no changes to be made, subs is returned unharmed.
        """
        return subs

    def _update_links(self, sample_data):
        """Process sample data and update version specific links."""
        # replace version urls
        url_re = self._get_host() + "/v3/" + PROJECT_ID
        new_url = self._get_host() + "/" + self.api_major_version
        if self._project_id:
            new_url += "/" + PROJECT_ID
        updated_data = re.sub(url_re, new_url, sample_data)

        # replace unversioned urls
        url_re = self._get_host() + "/" + PROJECT_ID
        new_url = self._get_host()
        if self._project_id:
            new_url += "/" + PROJECT_ID
        updated_data = re.sub(url_re, new_url, updated_data)
        return updated_data

    def _verify_response(self, name, subs, response, exp_code,
                         update_links=True):

        # Always also include the laundry list of base regular
        # expressions for possible key values in our templates. Test
        # specific patterns (the value of ``subs``) can override
        # these.
        regexes = self._get_regexes()
        regexes.update(subs)
        subs = regexes
        self.subs = subs
        message = response.text if response.status_code >= 400 else None
        self.assertEqual(exp_code, response.status_code, message)
        response_data = response.content
        response_data = pretty_data(response_data)
        if not os.path.exists(self._get_template(name)):
            self._write_template(name, response_data)
            template_data = response_data
        else:
            template_data = self._read_template(name)
        if (self.generate_samples and
                not os.path.exists(self._get_sample(name))):
            self._write_sample(name, response_data)
            sample_data = response_data
        else:
            with open(self._get_sample(name)) as sample:
                sample_data = sample.read()
                if update_links:
                    sample_data = self._update_links(sample_data)

        try:
            template_data = objectify(template_data)
            response_data = objectify(response_data)
            response_result = self._compare_result(template_data,
                                                   response_data, "Response")
        except NoMatch as e:
            raise NoMatch("\nFailed to match Template to Response: \n%s\n"
                          "Template: %s\n\n"
                          "Response: %s\n\n" %
                          (e,
                           pp.pformat(template_data),
                           pp.pformat(response_data)))

        try:
            # replace some of the subs with patterns for the
            # doc/samples check. Also let the test do some of its
            # own generalization, if necessary
            vanilla_regexes = self._get_regexes()
            subs['os-vol-host-attr:host'] = vanilla_regexes['host_name']
            subs['id'] = vanilla_regexes['id']
            subs['uuid'] = vanilla_regexes['uuid']
            subs['image_id'] = vanilla_regexes['uuid']
            subs = self.generalize_subs(subs, vanilla_regexes)
            self.subs = subs
            sample_data = objectify(sample_data)
            self._compare_result(template_data, sample_data, "Sample")
            return response_result
        except NoMatch as e:
            raise NoMatch("\nFailed to match Template to Sample: \n%s\n"
                          "Template: %s\n\n"
                          "Sample: %s\n\n"
                          "Hint: does your test need to override "
                          "ApiSampleTestBase.generalize_subs()?" %
                          (e,
                           pp.pformat(template_data),
                           pp.pformat(sample_data)))

    def _get_host(self):
        return 'http://localhost:8776'

    def _get_glance_host(self):
        return 'http://glance.openstack.example.com'

    def _get_regexes(self):
        text = r'(\\"|[^"])*'
        isotime_re = '\d{4}-[0,1]\d-[0-3]\dT\d{2}:\d{2}:\d{2}Z'
        strtime_re = '\d{4}-[0,1]\d-[0-3]\dT\d{2}:\d{2}:\d{2}\.\d{6}'
        extension_update = (
            '\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[+-]\d{2}:\d{2}')
        strtime_url_re = ('\d{4}-[0,1]\d-[0-3]\d'
                          '\+\d{2}\%3A\d{2}\%3A\d{2}\.\d{6}')

        return {
            'isotime': isotime_re,
            'strtime': strtime_re,
            'strtime_url': strtime_url_re,
            'strtime_or_none': r'None|%s' % strtime_re,
            'password': '[0-9a-zA-Z]{1,12}',
            'id': '(?P<id>[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}'
                  '-[0-9a-f]{4}-[0-9a-f]{12})',
            'uuid': '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}'
                    '-[0-9a-f]{4}-[0-9a-f]{12}',
            'request_id': 'req-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}'
                          '-[0-9a-f]{4}-[0-9a-f]{12}',
            'host': 'https?://[0-9]+(?:\.[0-9]+){3}:[0-9]+',
            'host_name': r'\w+',
            'glance_host': self._get_glance_host(),
            'os-vol-host-attr:host': self.volume.host,
            'text': text,
            'int': '[0-9]+',
            'user_id': text,
            'api_vers': self.api_major_version,
            'volume_endpoint': self._get_volume_endpoint(),
            'versioned_volume_endpoint': self._get_versioned_volume_endpoint(),
            'name': text,
            'description': text,
            'extension_update': extension_update
        }

    def _get_volume_endpoint(self):
        if self._project_id:
            return '%s/%s' % (self._get_host(), PROJECT_ID)
        else:
            return self._get_host()

    def _get_versioned_volume_endpoint(self):
        if self._project_id:
            return '%s/v3/%s' % (self._get_host(), PROJECT_ID)
        else:
            return self._get_host() + '/v3/'

    def _get_response(self, url, method, body=None,
                      headers=None):
        headers = headers or {}
        headers['Content-Type'] = 'application/json'
        headers['Accept'] = 'application/json'
        return self.api.api_request(url, body=body, method=method,
                                    headers=headers)

    def _do_options(self, url, headers=None):
        return self._get_response(url, 'OPTIONS', headers=headers)

    def _do_get(self, url, headers=None,
                return_json_body=False):
        response = self._get_response(url, 'GET', headers=headers)
        if return_json_body and hasattr(response, 'content'):
            return jsonutils.loads(response.content)
        return response

    def _do_post(self, url, name=None, subs=None, method='POST', headers=None):
        self.subs = {} if subs is None else subs
        body = None
        if name:
            body = self._read_template(name) % self.subs
            sample = self._get_sample(name)
            if self.generate_samples and not os.path.exists(sample):
                    self._write_sample(name, body)
        return self._get_response(url, method, body, headers=headers)

    def _do_put(self, url, name=None, subs=None, headers=None):
        # name indicates that we have a body document. While the HTTP
        # spec implies that PUT is supposed to have one, we have some
        # APIs which don't.
        if name:
            return self._do_post(
                url, name, subs, method='PUT', headers=headers)
        else:
            return self._get_response(url, 'PUT', headers=headers)

    def _do_delete(self, url, headers=None):
        return self._get_response(url, 'DELETE', headers=headers)
