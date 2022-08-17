import unittest
from logs import log
import urllib.request


class TestStringMethods(unittest.TestCase):

    def test_url(self):
        page = urllib.request.urlopen('http://services.runescape.com/m=hiscore/ranking?table=0&category_type=0&time_filter=0&date=1519066080774&user=zezima')
        print(page.read())


    def test_upper(self):
        log("starting upper test")
        self.assertEqual('foo'.upper(), 'FOO')

    def test_isupper(self):
        log("starting isupper test")
        self.assertTrue('FOO'.isupper())
        self.assertFalse('Foo'.isupper())

    def x_test_that_fails(self):
        localVar = 3
        self.assertTrue(localVar == 2)

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
