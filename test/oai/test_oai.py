import unittest
import urllib.request
from support.logs import log


class TestOAI(unittest.TestCase):
    def test_oai_exists(self):
        page = urllib.request.urlopen('http://localhost:8080/server/oai/request?verb=ListRecords&metadataPrefix=cmdi')
        log("oai page: " + str(page.read()))
