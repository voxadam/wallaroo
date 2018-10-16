#
# Copyright 2018 The Wallaroo Authors.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
#  implied. See the License for the specific language governing
#  permissions and limitations under the License.
#

from resilience import _test_resilience

class Creator(object):
    def __init__(self, module):
        self.module = module

    def create(self, test_name_fmt, api, cmd, ops, cycles=1, initial=None,
                    validate_output=True, sources=1):
        test_name = test_name_fmt.format(api=api,
                                         ops='_'.join((o.name().replace(':','')
                                                       for o in ops*cycles)))
        def f():
            _test_resilience(cmd, ops=ops, cycles=cycles, initial=initial,
                             validate_output=validate_output, sources=sources,
                             api=api)
        f.func_name = test_name
        setattr(self.module, test_name, f)
