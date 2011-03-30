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
from nsivideogranulate.interfaces.http import IHttp
from nsi.granulate import Granulate
from restfulie import Restfulie
from celery.execute import send_task

class HttpHandler(cyclone.web.RequestHandler):

    implements(IHttp)

    allowNone = True

    def _get_current_user(self):
        auth = self.request.headers.get("Authorization")
        if auth:
          return decodestring(auth.split(" ")[-1]).split(":")

    def _check_auth(self):
        user, password = self._get_current_user()
        if not self.settings.auth.authenticate(user, password):
            raise cyclone.web.HTTPError(401, 'Unauthorized')

    def _load_request_as_json(self):
        return loads(self.request.body)

    @defer.inlineCallbacks
    @cyclone.web.asynchronous
    def get(self):
        self._check_auth()
        self.set_header('Content-Type', 'application/json')
        uid = self._load_request_as_json().get('key')
        sam = Restfulie.at('http://localhost:8888/').auth('test', 'test').as_('application/json')
        response = yield sam.get({'key':uid})
        if response.code == 404:
            raise cyclone.web.HTTPError(404, "Key not found.")
        grains = response.resource()
        if hasattr(grains.data, 'granulated') and not grains.data.granulated:
            self.finish(cyclone.web.escape.json_encode({'done':False}))
        self.finish(cyclone.web.escape.json_encode({'done':True}))

    @defer.inlineCallbacks
    @cyclone.web.asynchronous
    def post(self):
        self._check_auth()
        self.set_header('Content-Type', 'application/json')
        video = self._load_request_as_json().get('video')
        video_format = self._load_request_as_json().get('format')
        if not video_format == 'ogm':
            video_uid = yield self._convert_video(video)
        else:
            video_uid = yield self._pre_store_in_sam(video)
        video_grains = {'grains':[], 'done':False}
        grains_uid = yield self._pre_store_in_sam(video_grains)
        response = yield self._enqueue_uid_to_granulate(grains_uid, video_uid)
        self.finish(cyclone.web.escape.json_encode({'grains_key':grains_uid, 'video_key':video_uid}))

    def _convert_video(self, video):
        converter = Restfulie.at('http://localhost:8080/').auth('test', 'test').as_('application/json')
        response = converter.post({'video':video}).resource()
        uid = response.key
        return uid

    def _pre_store_in_sam(self, data):
        sam = Restfulie.at('http://localhost:8888/').auth('test', 'test').as_('application/json')
        response = sam.put({'value':data}).resource()
        uid = response.key
        return uid

    def _enqueue_uid_to_granulate(self, grains_uid, video_uid):
        send_task('nsivideogranulate.tasks.granulate_video', args=(grains_uid,video_uid,5))

#    def _granulate(self, filename, data):
#        auth = self.request.headers.get("Authorization")
#        if auth:
#            granulate = Granulate()
#            grains = granulate.granulate(filename, decodestring(data))
#            return [b64encode(image.getContent().seek(0).read()) for image in grains['image_list']]

