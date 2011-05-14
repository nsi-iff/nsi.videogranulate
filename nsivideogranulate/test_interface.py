from base64 import b64encode, decodestring
#import codecs
import binascii
import cyclone.web
from twisted.internet.threads import deferToThread 
from restfulie import Restfulie

def imprime(texto):
    print texto

class TestInterfaceHandler(cyclone.web.RequestHandler):

    def get(self):
        self.render("index.html")

    @cyclone.web.asynchronous
    def post(self):
        video = self.request.files["file"][0]["body"]
        video = binascii.b2a_base64(video)
        open('/tmp/test.flv', 'w+').write(decodestring(video))
        #f = codecs.open('/tmp/test.flv', 'wb')
        #f.write(video)
        #f.close()
        video_granulate_service = Restfulie.at("http://localhost:8885/").auth('test', 'test').as_('application/json')
        response = video_granulate_service.post({'video':video, 'format':'ogm', 'callback':'http://google.com'})
        self.render("granulate.html", body=response.body, code=response.code)

