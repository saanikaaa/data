'''
This module generates MCF file in OUTPUT directory.
There are two mcf files are generated
1. population_estimate_by_srh.mcf - for importing as-is datq from US Census
2. population_estimate_by_srh_agg.mcf - for importing aggregated data
'''

import os
from constants import OUTPUT_DIR

_CODEDIR = os.path.dirname(os.path.realpath(__file__))

genderType = ['Male', 'Female']
hispanic = ['HispanicOrLatino', 'NotHispanicOrLatino']
race = [
    'WhiteAlone', 'BlackOrAfricanAmericanAlone',
    'AmericanIndianOrAlaskaNativeAlone', 'AsianOrPacificIslander', 'AsianAlone',
    'NativeHawaiianOrOtherPacificIslanderAlone', 'TwoOrMoreRaces',
    'WhiteAloneOrInCombinationWithOneOrMoreOtherRaces',
    'BlackOrAfricanAmericanAloneOrInCombinationWithOneOrMoreOtherRaces',
    'AmericanIndianAndAlaskaNativeAloneOrInCombinationWithOneOrMoreOtherRaces',
    'AsianAloneOrInCombinationWithOneOrMoreOtherRaces',
    'NativeHawaiianAndOtherPacificIslanderAloneOrInCombination\
WithOneOrMoreOtherRaces'
]

mcf_srh = '''Node: dcid:{}
typeOf: dcs:StatisticalVariable
statType: dcs:measuredValue
measuredProperty: dcs:count
populationType: dcs:Person
gender: dcs:{}
race: dcs:{}

'''

mcf_sh = '''Node: dcid:{}
typeOf: dcs:StatisticalVariable
statType: dcs:measuredValue
measuredProperty: dcs:count
populationType: dcs:Person
gender: dcs:{}

'''

# Count_Person_Male_NotHispanicOrLatino_WhiteAlone
# Count_Person_Female_NotHispanicOrLatino_WhiteAlone
# SV's are already present, hence not generating them
mcf = ""

for gt in genderType:
    for h in hispanic:
        for r in race:
            if 'Count_Person_' + gt + '_' + h + '_' + r not in [
                    'Count_Person_Male_NotHispanicOrLatino_WhiteAlone',
                    'Count_Person_Female_NotHispanicOrLatino_WhiteAlone'
            ]:
                mcf += mcf_srh.format('Count_Person_' + gt + '_' + h + '_' + r,
                                      gt, h + '__' + r)

for gt in genderType:
    for h in hispanic:
        mcf += mcf_srh.format('Count_Person_' + gt + '_' + h, gt, h)

with open(_CODEDIR + OUTPUT_DIR + 'population_estimate_by_srh.mcf',
          'w',
          encoding='utf-8') as mcf_file:
    mcf_file.write(mcf)

# Following section created MCF nodes for Aggregated data
# E.g., of gggregate: Hispanic = Hispanic Male + Hispanic Female

mcf_rh = '''Node: dcid:{}
typeOf: dcs:StatisticalVariable
statType: dcs:measuredValue
measuredProperty: dcs:count
populationType: dcs:Person
race: dcs:{}

'''

mcf = ""

# Count_Person_NotHispanicOrLatino_WhiteAlone
# SV is already present, hence not generating it.
for h in hispanic:
    for r in race:
        if 'Count_Person_' + h + '_' + r != \
            'Count_Person_NotHispanicOrLatino_WhiteAlone':
            mcf += mcf_rh.format('Count_Person_' + h + '_' + r, h + '__' + r)

with open(_CODEDIR + OUTPUT_DIR + 'population_estimate_by_srh_agg.mcf',
          'w',
          encoding='utf-8') as mcf_file:
    mcf_file.write(mcf)
