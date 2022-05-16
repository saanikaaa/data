'''
This module generates TMCF file in OUTPUT directory.
There are two tmcf files are generated
1. population_estimate_by_srh.tmcf - for importing as-is datq from US Census
2. population_estimate_by_srh_agg.tmcf - for importing aggregated data
'''

import os
from constants import OUTPUT_DIR

_CODEDIR = os.path.dirname(os.path.realpath(__file__))

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
