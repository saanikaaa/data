# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
'''
This module generates TMCF file in OUTPUT directory.
There are two tmcf files are generated
1. population_estimate_by_srh.tmcf - for importing as-is datq from US Census
2. population_estimate_by_srh_agg.tmcf - for importing aggregated data
'''

import os
from constants import OUTPUT_DIR

_CODEDIR = os.path.dirname(os.path.realpath(__file__))
os.system("mkdir -p " + _CODEDIR + OUTPUT_DIR)

population_estimate_by_srh = '''Node: E:population_estimate_by_srh->E0
typeOf: dcs:StatVarObservation
variableMeasured: C:population_estimate_by_srh->SV
observationAbout: C:population_estimate_by_srh->LOCATION
observationDate: C:population_estimate_by_srh->YEAR
observationPeriod: "P1Y"
measurementMethod: C:population_estimate_by_srh->MEASUREMENT_METHOD
value: C:population_estimate_by_srh->OBSERVATION
'''

with open(_CODEDIR + OUTPUT_DIR + 'population_estimate_by_srh.tmcf',
          'w',
          encoding='utf-8') as file:
    file.writelines(population_estimate_by_srh)

population_estimate_by_srh_agg = '''Node: E:population_estimate_by_srh_agg->E0
typeOf: dcs:StatVarObservation
variableMeasured: C:population_estimate_by_srh_agg->SV
observationAbout: C:population_estimate_by_srh_agg->LOCATION
observationDate: C:population_estimate_by_srh_agg->YEAR
observationPeriod: "P1Y"
measurementMethod: C:population_estimate_by_srh_agg->MEASUREMENT_METHOD
value: C:population_estimate_by_srh_agg->OBSERVATION
'''

with open(_CODEDIR + OUTPUT_DIR + 'population_estimate_by_srh_agg.tmcf',
          'w',
          encoding='utf-8') as file:
    file.writelines(population_estimate_by_srh_agg)
