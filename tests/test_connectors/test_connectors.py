import unittest

import ayeaye


class Alpha(ayeaye.Model):
    x = ayeaye.Connect(engine_url="file://mine/a.csv")
    y = ayeaye.Connect(engine_url=["csv://mine/a.csv", "s3+csv://mine/a.csv"])


class Beta(ayeaye.Model):
    x = ayeaye.Connect(engine_url="csv://mine/a.csv")
    # 'mine' is the bucket name
    y = ayeaye.Connect(engine_url="s3+csv://mine/a.csv", access=ayeaye.AccessMode.WRITE)


class TestConnectors(unittest.TestCase):
    def test_data_flow(self):
        """
        DataConnectors declare own inputs and outputs.

        The actual values of the data flow keys are expected to change. These tests just compare
        keys. Identical keys means they are referencing the same data.
        """
        a = Alpha()
        b = Beta()

        # readability
        ax = a.x.data_flow()
        ay = a.y.data_flow()
        bx = b.x.data_flow()
        by = b.y.data_flow()

        msg = "Alpha.x and Beta.x refer to the same file"
        self.assertEqual(ax.inputs, bx.inputs, msg)

        msg = "Alpha.y contains exactly Beta.x and Beta.y"
        self.assertEqual(len(bx.inputs), 1, msg)
        self.assertEqual(len(by.outputs), 1, msg)
        self.assertEqual(len(ay.inputs), 2, msg)

        bx_by = {bx.inputs[0], by.outputs[0]}
        self.assertEqual(len(bx_by), 2, msg)

        ay_set = set(ay.inputs)
        self.assertEqual(len(ay_set), 2, msg)

        self.assertEqual(bx_by, ay_set, msg)
