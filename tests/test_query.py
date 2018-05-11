import os
import unittest
import numpy as np
import pandas as pd
from h5plexos.process import process_solution
from h5plexos.query import PLEXOSSolution

class TestPlexosQuerySolution(unittest.TestCase):

    def test_query_object_values(self):
        """Verify object values are available
        """

        h5filename = "tests/mda_output_values.h5"
        process_solution("tests/mda_output.zip", h5filename)

        expected = [-0.935319116500001, -0.6970154267499986, -0.5217735017499989,
                    -0.41615258650000153, -0.3980630747500005, -0.46516376499999984,
                    -0.7597340485000006, -1.2800584555000007, -1.812169899250002,
                    -2.0393797997500016, -2.1432084820000004, -2.20546277575,
                    -2.2587450190000005, -2.15386336825, -2.0509797174999984,
                    -1.98446034625, -1.9687104047500001, -2.1013393862500007,
                    -2.4032077540000008, -2.3716624119999983, -2.0844381467499993,
                    -1.7796791724999996, -1.4374390120000011, -1.1613561009999995]
        expected_timestamps = pd.DatetimeIndex(start="16/04/2020 00:00:00",
                                               end="16/04/2020 23:00:00",
                                               freq="H")

        with PLEXOSSolution(h5filename) as db:

            result = db.query_objects("Lines", "Flow", names=["B1_B2"]).iloc[:24, 0]
            self.assertEqual(expected, list(result))
            self.assertEqual(list(expected_timestamps), list(result.index))

            result = db.Lines("Flow", names=["B1_B2"]).iloc[:24, 0]
            self.assertEqual(expected, list(result))
            self.assertEqual(list(expected_timestamps), list(result.index))

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
