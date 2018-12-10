# Copyright (c) 2014, 2015, Oracle and/or its affiliates. All rights reserved.
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
ZFS Storage Appliance REST API Client Programmatic Interface
"""

import json
import ssl
import time

from oslo_log import log
from oslo_utils import strutils
import six
from six.moves import http_client
from six.moves import urllib

LOG = log.getLogger(__name__)


class Status(object):
    """Result HTTP Status"""

    def __init__(self):
        pass

    #: Request return OK
    OK = http_client.OK

    #: New resource created successfully
    CREATED = http_client.CREATED

    #: Command accepted
    ACCEPTED = http_client.ACCEPTED

    #: Command returned OK but no data will be returned
    NO_CONTENT = http_client.NO_CONTENT

    #: Bad Request
    BAD_REQUEST = http_client.BAD_REQUEST

    #: User is not authorized
    UNAUTHORIZED = http_client.UNAUTHORIZED

    #: The request is not allowed
    FORBIDDEN = http_client.FORBIDDEN

    #: The requested resource was not found
    NOT_FOUND = http_client.NOT_FOUND

    #: The request is not allowed
    NOT_ALLOWED = http_client.METHOD_NOT_ALLOWED

    #: Request timed out
    TIMEOUT = http_client.REQUEST_TIMEOUT

    #: Invalid request
    CONFLICT = http_client.CONFLICT

    #: Service Unavailable
    BUSY = http_client.SERVICE_UNAVAILABLE


class RestResult(object):
    """Result from a REST API operation"""
    def __init__(self, response=None, err=None):
        """Initialize a RestResult containing the results from a REST call.

        :param response: HTTP response
        """
        self.response = response
        self.error = err
        self.data = ""
        self.status = 0
        if self.response:
            self.status = self.response.getcode()
            result = self.response.read()
            while result:
                self.data += result.decode("utf-8")
                result = self.response.read()

        if self.error:
            self.status = self.error.code
            self.data = http_client.responses[self.status]

        LOG.debug('Response code: %s', self.status)
        LOG.debug('Response data: %s', self.data)

    def get_header(self, name):
        """Get an HTTP header with the given name from the results

        :param name: HTTP header name
        :return: The header value or None if no value is found
        """
        if self.response is None:
            return None
        info = self.response.info()
        return info.get(name)


class RestClientError(Exception):
    """Exception for ZFS REST API client errors"""
    def __init__(self, status, name="ERR_INTERNAL", message=None):

        """Create a REST Response exception

        :param status: HTTP response status
        :param name: The name of the REST API error type
        :param message: Descriptive error message returned from REST call
        """
        super(RestClientError, self).__init__(message)
        self.code = status
        self.name = name
        self.msg = message
        if status in http_client.responses:
            self.msg = http_client.responses[status]

    def __str__(self):
        return "%d %s %s" % (self.code, self.name, self.msg)


class RestClientURL(object):
    """ZFSSA urllib client"""
    def __init__(self, url, **kwargs):
        """Initialize a REST client.

        :param url: The ZFSSA REST API URL
        :key session: HTTP Cookie value of x-auth-session obtained from a
                      normal BUI login.
        :key timeout: Time in seconds to wait for command to complete.
                      (Default is 60 seconds)
        """
        self.url = url
        self.local = kwargs.get("local", False)
        self.base_path = kwargs.get("base_path", "/api")
        self.timeout = kwargs.get("timeout", 60)
        self.headers = None
        if kwargs.get('session'):
            self.headers['x-auth-session'] = kwargs.get('session')

        self.headers = {"content-type": "application/json"}
        self.do_logout = False
        self.auth_str = None

    def _path(self, path, base_path=None):
        """build rest url path"""
        if path.startswith("http://") or path.startswith("https://"):
            return path
        if base_path is None:
            base_path = self.base_path
        if not path.startswith(base_path) and not (
                self.local and ("/api" + path).startswith(base_path)):
            path = "%s%s" % (base_path, path)
        if self.local and path.startswith("/api"):
            path = path[4:]
        return self.url + path

    def _authorize(self):
        """Performs authorization setting x-auth-session"""
        self.headers['authorization'] = 'Basic %s' % self.auth_str
        if 'x-auth-session' in self.headers:
            del self.headers['x-auth-session']

        try:
            result = self.post("/access/v1")
            del self.headers['authorization']
            if result.status == http_client.CREATED:
                self.headers['x-auth-session'] = \
                    result.get_header('x-auth-session')
                self.do_logout = True
                LOG.info('ZFSSA version: %s',
                         result.get_header('x-zfssa-version'))

            elif result.status == http_client.NOT_FOUND:
                raise RestClientError(result.status, name="ERR_RESTError",
                                      message="REST Not Available: \
                                      Please Upgrade")

        except RestClientError:
            del self.headers['authorization']
            raise

    def login(self, auth_str):
        """Login to an appliance using a user name and password.

        Start a session like what is done logging into the BUI.  This is not a
        requirement to run REST commands, since the protocol is stateless.
        What is does is set up a cookie session so that some server side
        caching can be done.  If login is used remember to call logout when
        finished.

        :param auth_str: Authorization string (base64)
        """
        self.auth_str = auth_str
        self._authorize()

    def logout(self):
        """Logout of an appliance"""
        result = None
        try:
            result = self.delete("/access/v1", base_path="/api")
        except RestClientError:
            pass

        self.headers.clear()
        self.do_logout = False
        return result

    def islogin(self):
        """return if client is login"""
        return self.do_logout

    @staticmethod
    def mkpath(*args, **kwargs):
        """Make a path?query string for making a REST request

        :cmd_params args: The path part
        :cmd_params kwargs: The query part
        """
        buf = six.StringIO()
        query = "?"
        for arg in args:
            buf.write("/")
            buf.write(arg)
        for k in kwargs:
            buf.write(query)
            if query == "?":
                query = "&"
            buf.write(k)
            buf.write("=")
            buf.write(kwargs[k])
        return buf.getvalue()

    def request(self, path, request, body=None, **kwargs):
        """Make an HTTP request and return the results

        :param path: Path used with the initialized URL to make a request
        :param request: HTTP request type (GET, POST, PUT, DELETE)
        :param body: HTTP body of request
        :key accept: Set HTTP 'Accept' header with this value
        :key base_path: Override the base_path for this request
        :key content: Set HTTP 'Content-Type' header with this value
        """
        out_hdrs = dict.copy(self.headers)
        if kwargs.get("accept"):
            out_hdrs['accept'] = kwargs.get("accept")

        if body:
            if isinstance(body, dict):
                body = json.dumps(body)
            body = body.encode("utf-8")
            out_hdrs['content-length'] = len(body)
        else:
            body = None

        zfssaurl = self._path(path, kwargs.get("base_path"))
        req = urllib.request.Request(zfssaurl, body, out_hdrs)
        req.get_method = lambda: request
        maxreqretries = kwargs.get("maxreqretries", 10)
        retry = 0
        response = None

        LOG.debug('Request: %(request)s %(url)s',
                  {'request': request, 'url': zfssaurl})
        LOG.debug('Out headers: %s', out_hdrs)
        if body and body != '':
            # body may contain chap secret so must be masked
            LOG.debug('Body: %s', strutils.mask_password(body))

        context = None
        if hasattr(ssl, '_create_unverified_context'):
            context = ssl._create_unverified_context()
        else:
            context = None

        while retry < maxreqretries:
            try:
                if context:
                    # only schemes that can be used will be http or https if it
                    # is given in the path variable, or the path will begin
                    # with the REST API location meaning invalid or unwanted
                    # schemes cannot be used
                    response = urllib.request.urlopen(req,  # nosec
                                                      timeout=self.timeout,
                                                      context=context)
                else:
                    response = urllib.request.urlopen(req,  # nosec : see above
                                                      timeout=self.timeout)
            except urllib.error.HTTPError as err:
                if err.code == http_client.NOT_FOUND:
                    LOG.debug('REST Not Found: %s', err.code)
                else:
                    LOG.error('REST Not Available: %s', err.code)

                if err.code == http_client.SERVICE_UNAVAILABLE and \
                   retry < maxreqretries:
                    retry += 1
                    time.sleep(1)
                    LOG.error('Server Busy retry request: %s', retry)
                    continue
                if (err.code == http_client.UNAUTHORIZED or
                    err.code == http_client.INTERNAL_SERVER_ERROR) and \
                   '/access/v1' not in zfssaurl:
                    try:
                        LOG.error('Authorizing request: %(zfssaurl)s '
                                  'retry: %(retry)d.',
                                  {'zfssaurl': zfssaurl, 'retry': retry})
                        self._authorize()
                        req.add_header('x-auth-session',
                                       self.headers['x-auth-session'])
                    except RestClientError:
                        pass
                    retry += 1
                    time.sleep(1)
                    continue

                return RestResult(err=err)

            except urllib.error.URLError as err:
                LOG.error('URLError: %s', err.reason)
                raise RestClientError(-1, name="ERR_URLError",
                                      message=err.reason)

            break

        if (response and
            (response.getcode() == http_client.SERVICE_UNAVAILABLE and
                retry >= maxreqretries)):
            raise RestClientError(response.getcode(), name="ERR_HTTPError",
                                  message="REST Not Available: Disabled")

        return RestResult(response=response)

    def get(self, path, **kwargs):
        """Make an HTTP GET request

        :param path: Path to resource.
        """
        return self.request(path, "GET", **kwargs)

    def post(self, path, body="", **kwargs):
        """Make an HTTP POST request

        :param path: Path to resource.
        :param body: Post data content
        """
        return self.request(path, "POST", body, **kwargs)

    def put(self, path, body="", **kwargs):
        """Make an HTTP PUT request

        :param path: Path to resource.
        :param body: Put data content
        """
        return self.request(path, "PUT", body, **kwargs)

    def delete(self, path, **kwargs):
        """Make an HTTP DELETE request

        :param path: Path to resource that will be deleted.
        """
        return self.request(path, "DELETE", **kwargs)

    def head(self, path, **kwargs):
        """Make an HTTP HEAD request

        :param path: Path to resource.
        """
        return self.request(path, "HEAD", **kwargs)
