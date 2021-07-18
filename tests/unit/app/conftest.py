# -*- coding: utf-8 -*-
# MinIO Python Library for Amazon S3 Compatible Cloud Storage,
# (C) 2015-2020 MinIO, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from http import client as httplib

from nose.tools import eq_


class MockResponse(object):
    def __init__(self, method, url, headers, status_code,
                 response_headers=None, content=None):
        self.method = method
        self.url = url
        self.request_headers = {
            key.lower(): value for key, value in headers.items()
        }
        self.status = status_code
        self.headers = {
            key.lower(): value for key, value in (
                response_headers or {}).items()
        }
        self.data = content
        if content is None:
            self.reason = httplib.responses[status_code]

    # noinspection PyUnusedLocal
    def read(self, *args, **kwargs):
        return self.data

    def mock_verify(self, method, url, headers):
        pytest.eq_(self.method, method)
        pytest.eq_(self.url, url)
        headers = {
            key.lower(): value for key, value in headers.items()
        }
        for header in self.request_headers:
            eq_(self.request_headers[header], headers[header])

    # noinspection PyUnusedLocal
    def stream(self, chunk_size=1, decode_unicode=False):
        if self.data is not None:
            return iter(bytearray(self.data, 'utf-8'))
        return iter([])

    # dummy release connection call.
    def release_conn(self):
        return

    def getheader(self, key, value=None):
        return self.headers.get(key, value) if self.headers else value

    def __getitem__(self, key):
        if key == "status":
            return self.status


class MockConnection(object):
    def __init__(self):
        self.requests = []

    def mock_add_request(self, request):
        self.requests.append(request)

    # noinspection PyUnusedLocal
    def request(self, method, url, headers, redirect=False):
        # only pop off matching requests
        return_request = self.requests[0]
        return_request.mock_verify(method, url, headers)
        return self.requests.pop(0)

    # noinspection PyRedeclaration,PyUnusedLocal,PyUnusedLocal

    def urlopen(self, method, url, headers={}, preload_content=False,
                body=None, redirect=False):
        return self.request(method, url, headers)