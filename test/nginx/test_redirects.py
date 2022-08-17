import unittest
import urllib.request
from support.logs import log

class NginxRedirectTests(unittest.TestCase):
    def test_basic(self):
        page = urllib.request.urlopen('http://localhost:8080/server/')
        log("oai page: " + str(str(page.read())))