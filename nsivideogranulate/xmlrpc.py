#!/usr/bin/env python
#-*- coding:utf-8 -*-

from base64 import encodestring, decodestring, b64encode
from StringIO import StringIO
from xmlrpclib import Server
from time import sleep
import cyclone.web
from twisted.internet import defer
from zope.interface import implements
from nsivideogranulate.interfaces.xmlrpc import IXmlrpc
from nsi.granulate import Granulate

class XmlrpcHandler(cyclone.web.XmlrpcRequestHandler):

    implements(IXmlrpc)

#    allowNone = True

    def get_current_user(self):
        auth = self.request.headers.get("Authorization")
        if auth:
          return decodestring(auth.split(" ")[-1]).split(":")

    @defer.inlineCallbacks
    @cyclone.web.asynchronous
    def xmlrpc_granulate(self, video):
        auth = self.request.headers.get("Authorization")
        if auth:
            converted_video_uid = yield self._convert_video(video)
#            Método de conversão funcionando com os novos padrões
#            Pré-salvar grãos no SAM (com alguma tag dizendo que não estão prontos) e colocar o UID deles e do vídeo a ser granularizado na fila
#            Criar script para gerar os grãos e salvá-los
#            Criar um template para este script

#            converted_video = yield self._get_video(converted_video_uid)
#            images = yield self._granulate("teste.ogv", converted_video)
#            uid = yield self._store_in_sam(images)
#            defer.returnValue(uid)
            defer.returnValue(converted_video_uid)

    def _granulate(self, filename, data):
        auth = self.request.headers.get("Authorization")
        if auth:
            granulate = Granulate()
            grains = granulate.granulate(filename, decodestring(data))
            return [b64encode(image.getContent().read()) for image in grains['image_list']]

    def _convert_video(self, video):
        auth = self.request.headers.get("Authorization")
        if auth:
            converter = Server('http://video:convert@localhost:8080/xmlrpc')
            uid = converter.convert(video)
            while not converter.done(uid):
                sleep(10)
            return uid

    def _get_video(self, uid):
        auth = self.request.headers.get("Authorization")
        if auth:
            SAM = Server('http://video:convert@localhost:8888/xmlrpc')
            video_dict = eval(SAM.get(uid))
            return video_dict['data']

    def _store_in_sam(self, data):
        auth = self.request.headers.get("Authorization")
        if auth:
            sam = Server('http://video:convert@localhost:8888/xmlrpc')
            return sam.set(data)

