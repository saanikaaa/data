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

# pylint: disable=missing-module-docstring
# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring
# pylint: disable=unused-import

import os
import unittest
import mcf_generator
import tmcf_generator

from process import main
from constants import OUTPUT_DIR

_CODEDIR = os.path.dirname(os.path.realpath(__file__))


class ProcessTest(unittest.TestCase):

    def testAggMcf(self):
        expected_agg_mcf_file = open(
            _CODEDIR +
            '/testdata/expected_results/population_estimate_by_srh_agg.mcf',
            'r',
            encoding='utf-8')
        expected_agg_mcf = expected_agg_mcf_file.read()
        actual_agg_mcf_file = open(_CODEDIR + OUTPUT_DIR +
                                   'population_estimate_by_srh_agg.mcf',
                                   'r',
                                   encoding='utf-8')
        actual_agg_mcf = actual_agg_mcf_file.read()

        expected_agg_mcf_file.close()
        actual_agg_mcf_file.close()

        self.assertEqual(expected_agg_mcf, actual_agg_mcf)

    def testMcf(self):
        expected_mcf_file = open(
            _CODEDIR +
            '/testdata/expected_results/population_estimate_by_srh.mcf',
            'r',
            encoding='utf-8')
        expected_mcf = expected_mcf_file.read()
        actual_mcf_file = open(_CODEDIR + OUTPUT_DIR +
                               'population_estimate_by_srh.mcf',
                               'r',
                               encoding='utf-8')
        actual_mcf = actual_mcf_file.read()

        expected_mcf_file.close()
        actual_mcf_file.close()

        self.assertEqual(expected_mcf, actual_mcf)

    def testAggTmcf(self):
        expected_agg_tmcf_file = open(
            _CODEDIR +
            '/testdata/expected_results/population_estimate_by_srh_agg.tmcf',
            'r',
            encoding='utf-8')
        expected_agg_tmcf = expected_agg_tmcf_file.read()
        actual_agg_tmcf_file = open(_CODEDIR + OUTPUT_DIR +
                                    'population_estimate_by_srh_agg.tmcf',
                                    'r',
                                    encoding='utf-8')
        actual_agg_tmcf = actual_agg_tmcf_file.read()

        expected_agg_tmcf_file.close()
        actual_agg_tmcf_file.close()

        self.assertEqual(expected_agg_tmcf, actual_agg_tmcf)

    def testTmcf(self):
        expected_tmcf_file = open(
            _CODEDIR +
            '/testdata/expected_results/population_estimate_by_srh.tmcf',
            'r',
            encoding='utf-8')
        expected_tmcf = expected_tmcf_file.read()
        actual_tmcf_file = open(_CODEDIR + OUTPUT_DIR +
                                'population_estimate_by_srh.tmcf',
                                'r',
                                encoding='utf-8')
        actual_tmcf = actual_tmcf_file.read()

        expected_tmcf_file.close()
        actual_tmcf_file.close()

        self.assertEqual(expected_tmcf, actual_tmcf)

    def testAggCsv(self):
        main('/testdata/input_files/')
        expected_agg_csv_file = open(
            _CODEDIR +
            '/testdata/expected_results/population_estimate_by_srh_agg.csv',
            'r',
            encoding='utf-8')
        expected_agg_csv = expected_agg_csv_file.read()
        actual_agg_csv_file = open(_CODEDIR + OUTPUT_DIR +
                                   'population_estimate_by_srh_agg.csv',
                                   'r',
                                   encoding='utf-8')
        actual_agg_csv = actual_agg_csv_file.read()

        expected_agg_csv_file.close()
        actual_agg_csv_file.close()

        self.assertEqual(expected_agg_csv, actual_agg_csv)

    def testCsv(self):
        main('/testdata/input_files/')
        expected_csv_file = open(
            _CODEDIR +
            '/testdata/expected_results/population_estimate_by_srh.csv',
            'r',
            encoding='utf-8')
        expected_csv = expected_csv_file.read()
        actual_csv_file = open(_CODEDIR + OUTPUT_DIR +
                               'population_estimate_by_srh.csv',
                               'r',
                               encoding='utf-8')
        actual_csv = actual_csv_file.read()

        expected_csv_file.close()
        actual_csv_file.close()

        self.assertEqual(expected_csv, actual_csv)
