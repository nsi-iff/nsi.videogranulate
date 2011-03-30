import unittest
from nsivideogranulate.interfaces.auth import IAuth
from nsivideogranulate.interfaces.http import IHttp
from nsivideogranulate.auth import Authentication
from nsivideogranulate.http import HttppcHandler

class TestInterface(unittest.TestCase):

    def test_auth(self):
        self.assertEquals(IAuth.implementedBy(Authentication), True)
        self.assertEquals(sorted(IAuth.names()), ['add_user',
                                                'authenticate',
                                                'del_user'])

    def test_handler(self):
        self.assertEquals(IXmlrpc.implementedBy(XmlrpcHandler), True)
        self.assertEquals(sorted(IXmlrpc.names()), ['get',
                                                'get_current_user',
                                                'post'])

if __name__ == "__main__":
    unittest.main()

