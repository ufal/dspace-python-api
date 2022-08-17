import unittest
from logs import log


class TestStringMethods(unittest.TestCase):

    def test_upper(self):
        log("starting upper test")
        self.assertEqual('foo'.upper(), 'FOO')

    def test_isupper(self):
        log("starting isupper test")
        self.assertTrue('FOO'.isupper())
        self.assertFalse('Foo'.isupper())

    def x_test_that_fails(self):
        self.assertTrue(False)

    def test_split(self):
        log("starting split test")
        s = 'hello world'
        self.assertEqual(s.split(), ['hello', 'world'])
        # check that s.split fails when the separator is not a string
        with self.assertRaises(TypeError):
            s.split(2)

    def setUp(self):
        log("setting up test")

    def tearDown(self):
        log("tearing down test")


if __name__ == '__main__':
    unittest.main()
