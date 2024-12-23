# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
import re

from superset.db_engine_specs.tdengine import TDengineEngineSpec
from tests.integration_tests.db_engine_specs.base_tests import TestDbEngineSpec


class TestTDengineEngineSpecs(TestDbEngineSpec):
    def test_epoch_to_dttm(self):
        """
        DB Eng Specs (tdengine): Test epoch to dttm
        """
        assert TDengineEngineSpec.epoch_to_dttm() == "TO_ISO8601({col})"

    def test_execute(self):
        """
        DB Eng Specs (tdengine): Test quotation mark replacement
        """
        origin = "SELECT COUNT(*) AS 'total' FROM test.tb"
        expected = "SELECT COUNT(*) AS `total` FROM test.tb"
        result = re.sub(r'\s*(?i:AS)\s+[\'"]([^\'"]+)[\'"]', r" AS `\1`", origin)
        assert expected == result
