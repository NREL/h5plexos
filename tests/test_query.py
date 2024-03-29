import os
import unittest
import numpy as np
import pandas as pd
from h5plexos.process import process_solution
from h5plexos.query import PLEXOSSolution

class TestPlexosQuerySolution(unittest.TestCase):

    def test_query_object_values_interval(self):
        """Verify object values are available
        """

        h5filename = "tests/RTS_DA.h5"
        process_solution("tests/Model DAY_AHEAD Solution.zip", h5filename)

        expected_timestamps = list(pd.DatetimeIndex(start="1/1/2020 00:00:00",
                                               end="1/1/2020 23:00:00",
                                               freq="H"))

        expected_flow = [-330.197431834115, -332.389888001791, -334.860066230212,
                         -334.794115561143, -339.795973724878, -368.928288297157,
                         -308.155615860858, -282.568408516569, -265.123568756352,
                         -224.586887604017, -162.915840602681, -131.667018544265,
                         -127.29943543624, -108.499125476666, -37.5693788753338,
                         -42.3496225987148, 54.084407447253, -62.1597519929606,
                         -72.7661568218603, -79.917583594192, -146.889373743392,
                         -197.581977296408, -289.342389741395, -229.896443712577]

        expected_offtake = [398.1, 398.1, 398.1, 398.1, 398.1, 398.1,
                            755.2398, 398.1, 398.1, 398.1, 398.1, 398.1,
                            398.1, 637.658136350009, 755.2398, 755.2398, 755.2398, 755.2398,
                            755.2398, 755.2398, 755.2398, 755.2398, 755.2398, 755.2398]

        with PLEXOSSolution(h5filename) as db:

            # Test line flows

            result = db.query_object_property("line", "Flow", names=["A27"]).iloc[:24]
            self.assertTrue(np.isclose(expected_flow, list(result)).all())
            self.assertEqual(expected_timestamps, [x[3] for x in result.index])

            result = db.line("Flow", names=["A27"]).iloc[:24]
            self.assertTrue(np.isclose(expected_flow, list(result)).all())
            self.assertEqual(expected_timestamps, [x[3] for x in result.index])

            # Test generator fuel offtake

            result = db.query_relation_property(
                "generator_fuels", "Offtake", parents=["101_STEAM_3"], children=["Coal"]).iloc[:24]
            self.assertTrue(np.isclose(expected_offtake, list(result)).all())
            self.assertEqual(expected_timestamps, [x[3] for x in result.index])

            result = db.generator_fuels(
                "Offtake", parents=["101_STEAM_3"], children=["Coal"]).iloc[:24]
            self.assertTrue(np.isclose(expected_offtake, list(result)).all())
            self.assertEqual(expected_timestamps, [x[3] for x in result.index])

        os.remove(h5filename)

        h5filename = "tests/RTS_DA_h2.h5"
        process_solution("tests/Model DAY_AHEAD_h2 Solution.zip", h5filename)

        expected_generation = [
            0, 0, 0, 0, 0, 8.8,
            13.6, 15.6, 17.1, 17.1, 18.2, 18.5,
            18.1, 16.8, 16.3, 14.2, 9.8, 4.4,
            0, 0, 0, 0, 0, 0
        ]

        with PLEXOSSolution(h5filename) as db:

            result = db.generator("Generation", names=["102_PV_1"]).xs(
                slice("7/3/2020", "7/3/2020"), level="timestamp")
            self.assertTrue(np.isclose(expected_generation, list(result)).all())

            result = db.generator("Generation", categories=["Solar PV"]).xs(
                ("102_PV_1", slice("7/3/2020", "7/3/2020")),
                level=("name", "timestamp"))
            self.assertTrue(np.isclose(expected_generation, list(result)).all())

            result = db.generator("Generation").xs(
                ("102_PV_1", slice("7/3/2020", "7/3/2020")),
                level=("name", "timestamp"))
            self.assertTrue(np.isclose(expected_generation, list(result)).all())

        os.remove(h5filename)

    def test_query_object_values_month(self):

        h5filename = "tests/RTS_DA_h2.h5"
        process_solution("tests/Model DAY_AHEAD_h2 Solution.zip", h5filename)

        expected_generation = [
            0, 0, 0, 0, 0, 0,
            88.98564573773,
            73.72927432927,
            116.81835971605,
            127.5698581023,
            326.90425895285,
            249.19300099602]

        with PLEXOSSolution(h5filename) as db:

            result = db.generator(
                "Generation", names=["122_WIND_1"], timescale="month")
            self.assertTrue(np.isclose(expected_generation, list(result)).all())

        os.remove(h5filename)

    # TODO: Decide how to communicate query units
    # def test_object_unit(self):
    #     """Verify object property units are available
    #     """
    #     h5filename = "tests/mda_output_times.hdf5"
    #     h5file = process_solution("tests/mda_output.zip", h5filename)
    #     self.assertEqual("kV", h5file['data/Nodes/Voltage/period_0/phase_4'].attrs["unit"])
    #     h5file.close()
    #     os.remove(h5filename)
