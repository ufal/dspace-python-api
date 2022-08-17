import unittest
from support.logs import log
import urllib.request


class DummyTests(unittest.TestCase):

    def test_seeFE(self):
        page = urllib.request.urlopen('http://dev-5.pc/')
        # print(page.read())


    def test_seeBE(self):
        page = urllib.request.urlopen('http://dev-5.pc/server')
        # print(page.read())


    def test_seeOAI(self):
        # page = urllib.request.urlopen('http://dev-5.pc/server/oai/request?verb=ListRecords&metadataPrefix=oai_dc')
        page = urllib.request.urlopen('http://localhost:8080/server/oai/request?verb=ListRecords&metadataPrefix=cmdi')
        log(page.read())



    def test_upper(self):
        log("starting upper test")
        self.assertEqual('foo'.upper(), 'FOO')

    def test_isupper(self):
        log("starting isupper test")
        self.assertTrue('FOO'.isupper())
        self.assertFalse('Foo'.isupper())

    def xtest_that_fails(self):
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
