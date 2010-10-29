#!/usr/bin/env python
#-*- coding:utf-8 -*-

from urllib import urlencode
from urllib2 import urlopen, Request
from simplejson import dumps, loads
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

    allowNone = True

    def get_current_user(self):
        auth = self.request.headers.get("Authorization")
        if auth:
          return decodestring(auth.split(" ")[-1]).split(":")

    @defer.inlineCallbacks
    @cyclone.web.asynchronous
    def xmlrpc_done(self, grains_uid):
        auth = self.request.headers.get("Authorization")
        if auth:
            sam = Server("http://video:convert@localhost:8888/xmlrpc")
            grains_dict = yield sam.get(grains_uid)
            if isinstance(grains_dict, str):
                grains_dict = eval(grains_dict)
                if isinstance(grains_dict, dict) and not len(grains_dict['data']['grains']) == 0:
                    defer.returnValue(True)
            defer.returnValue(False)

    @defer.inlineCallbacks
    @cyclone.web.asynchronous
    def xmlrpc_granulate(self, video):
        auth = self.request.headers.get("Authorization")
        if auth:
            video_uid = yield self._convert_video(video)
            video_grains = {'grains':[]}
            grains_uid = yield self._pre_store_in_sam(video_grains)
            response = yield self._enqueue_uid_to_granulate(grains_uid, video_uid)
            defer.returnValue(grains_uid)

    def _convert_video(self, video):
        converter = Server('http://video:convert@localhost:8080/xmlrpc')
        uid = converter.convert(video)
        return uid

    def _pre_store_in_sam(self, data):
        sam = Server('http://video:convert@localhost:8888/xmlrpc')
        return sam.set(data)

    def _enqueue_uid_to_granulate(self, grains_uid, video_uid):
        message = {"grains_uid":grains_uid, "video_uid":video_uid}
        data = urlencode({"queue":"to_granulate", "value":dumps(message)})
        request = Request("http://localhost:8886/", data)
        response = urlopen(request)
        response_data = response.read()
        return response_data

#    def _granulate(self, filename, data):
#        auth = self.request.headers.get("Authorization")
#        if auth:
#            granulate = Granulate()
#            grains = granulate.granulate(filename, decodestring(data))
#            return [b64encode(image.getContent().read()) for image in grains['image_list']]

