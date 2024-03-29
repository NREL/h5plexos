import os
import unittest
import numpy as np
from h5plexos.process import process_solution

class TestPlexosProcessSolution(unittest.TestCase):
    # New h5file for each test, as a failure will break tests run afterwards

    def test_process(self):
        """Verify the zip file is processed properly
        """
        h5filename = "tests/RTS_DA.hdf5"

        h5file = process_solution("tests/Model DAY_AHEAD_h1 Solution.zip", h5filename)

        times = h5file['/metadata/times/interval']
        self.assertEqual(b"2020-01-01T00:00:00", times[0])
        self.assertEqual(b"2020-01-02T23:00:00", times[47])

        phase_times = h5file['/metadata/times/ST']
        self.assertEqual(b"2020-01-01T00:00:00", phase_times[0])
        self.assertEqual(b"2020-01-02T23:00:00", phase_times[47])
        h5file.close()

        h5file = process_solution("tests/Model DAY_AHEAD_h2 Solution.zip", h5filename)

        times = h5file['/metadata/times/interval']
        self.assertEqual(b"2020-07-01T00:00:00", times[0])
        self.assertEqual(b"2020-07-02T23:00:00", times[47])

        phase_times = h5file['/metadata/times/ST']
        self.assertEqual(b"2020-07-01T00:00:00", phase_times[0])
        self.assertEqual(b"2020-07-02T23:00:00", phase_times[47])
        h5file.close()

        os.remove(h5filename)

    def test_properties(self):
        """Verify property values are available
        """
        h5filename = "tests/RTS_DA_properties.hdf5"

        h5file = process_solution("tests/Model DAY_AHEAD_h1 Solution.zip", h5filename)
        expected = np.array(
            [28.8742192841606, 29.2507825525972, 27.5952142765667,
             21.9715405175445, 7.98968935072566, -3.84725642745148,
             1.85503631499546, 5.07103646862386, 14.9091253711748,
             16.7898373291084, 12.3500587495798, 14.0178266329467,
             15.3848278766994, 22.8098348077484, 20.2177700376444,
             8.27787285160804, -13.7586901465986, 6.0633617651001,
             6.35689451038861, 8.8109582959243, 24.1764745175945,
             22.6143140432123, 36.1641235074314, 36.3852069384884])
        idx = np.where(h5file["/metadata/objects/line"]["name"] ==
                       bytes("AB1", "UTF8"))[0][0]
        np.testing.assert_allclose(h5file["/data/ST/interval/line/Flow"][idx,:24].ravel(), expected)
        h5file.close()

        h5file = process_solution("tests/Model DAY_AHEAD_h2 Solution.zip", h5filename)
        expected = np.array(
            [170, 170, 170, 170,
            170, 170, 170, 170,
            170, 170, 170, 170,
            231.7, 293.3, 293.3, 293.3,
            335.423428999999, 355, 355, 355,
            294.542642999999, 293.3, 231.7, 231.7])
        idx = np.where(h5file["/metadata/objects/generator"]["name"] ==
                       bytes("107_CC_1", "UTF8"))[0][0]
        np.testing.assert_allclose(h5file["/data/ST/interval/generator/Generation"][idx,24:48].ravel(), expected)
        h5file.close()

        os.remove(h5filename)

    def test_object_times(self):
        """Verify object times are available
        """
        h5filename = "tests/RTS_DA_times.hdf5"

        h5file = process_solution("tests/Model DAY_AHEAD_h1 Solution.zip", h5filename)
        expected = [b"2020-01-01T%02d:00:00" % x for x in range(24)]
        self.assertEqual(expected, list(h5file['/metadata/times/ST'][0:24]))
        h5file.close()

        h5file = process_solution("tests/Model DAY_AHEAD_h2 Solution.zip", h5filename)
        expected = [b"2020-07-02T%02d:00:00" % x for x in range(24)]
        self.assertEqual(expected, list(h5file['/metadata/times/ST'][24:48]))
        h5file.close()

        os.remove(h5filename)

    def test_object_units(self):
        """Verify object property units are available
        """
        h5filename = "tests/RTS_DA_units.hdf5"
        h5file = process_solution("tests/Model DAY_AHEAD_h1 Solution.zip", h5filename)
        self.assertEqual("$/MWh", h5file["data/ST/interval/node/Price"].attrs["unit"])
        self.assertEqual("MW", h5file["data/ST/interval/generator/Generation"].attrs["unit"])
        self.assertEqual("GWh", h5file["data/ST/month/generator/Generation"].attrs["unit"])
        h5file.close()
        os.remove(h5filename)

