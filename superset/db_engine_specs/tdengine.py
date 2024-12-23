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
from typing import Any

from superset.constants import TimeGrain
from superset.db_engine_specs.base import BaseEngineSpec
from superset.models.core import Database


class TDengineEngineSpec(BaseEngineSpec):
    """Engine spec for TDengine."""

    engine = "taosrest"
    engine_name = "TDengine"

    _time_grain_expressions = {
        None: "{col}",
        TimeGrain.SECOND: "TIMETRUNCATE({col}, 1s, 0)",
        TimeGrain.MINUTE: "TIMETRUNCATE({col}, 1m, 0)",
        TimeGrain.HOUR: "TIMETRUNCATE({col}, 1h, 0)",
        TimeGrain.DAY: "TIMETRUNCATE({col}, 1d, 0)",
        TimeGrain.WEEK: "TIMETRUNCATE({col}, 1w, 0)",
    }

    @classmethod
    def epoch_to_dttm(cls) -> str:
        # the timezone of taos client will be used for the conversion.
        return "TO_ISO8601({col})"

    @classmethod
    def execute(
        cls,
        cursor: Any,
        query: str,
        database: Database,
        **kwargs: Any,
    ) -> None:
        # replace all ' or " after keyword AS with ` since TDengine is only support it
        # now, more details on: https://github.com/taosdata/TDengine/issues/28766.
        query = re.sub(r'\s*(?i:AS)\s+[\'"]([^\'"]+)[\'"]', r" AS `\1`", query)
        super().execute(cursor, query, database, **kwargs)
