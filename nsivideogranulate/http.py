#!/usr/bin/env python
#-*- coding:utf-8 -*-

from json import loads
from base64 import decodestring
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

    def _load_sam_config(self):
        self.sam_settings = {'url': self.settings.sam_url, 'auth': [self.settings.sam_user, self.settings.sam_pass]}

    def _load_videoconvert_config(self):
        self.videoconvert_settings = {'url': self.settings.videoconvert_url, 'auth': (self.settings.videoconvert_user, self.settings.videoconvert_pass)}

    def __init__(self, *args, **kwargs):
        cyclone.web.RequestHandler.__init__(self, *args, **kwargs)
        self._load_sam_config()
        self._load_videoconvert_config()
        self.sam = Restfulie.at(self.sam_settings['url']).auth(*self.sam_settings['auth']).as_('application/json')

    @defer.inlineCallbacks
    @cyclone.web.asynchronous
    def get(self):
        self._check_auth()
        self.set_header('Content-Type', 'application/json')
        uid = self._load_request_as_json().get('key')
        response = yield self.sam.get(key=uid)
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
        callback_url = self._load_request_as_json().get('callback') or None
        video_format = self._load_request_as_json().get('format')
        if not video_format == 'ogm':
            video_uid = yield self._convert_video(video)
        else:
            video_uid = yield self._pre_store_in_sam(video)
        video_grains = {'grains':[], 'done':False}
        grains_uid = yield self._pre_store_in_sam(video_grains)
        response = yield self._enqueue_uid_to_granulate(grains_uid, video_uid, callback_url)
        self.finish(cyclone.web.escape.json_encode({'grains_key':grains_uid, 'video_key':video_uid}))

    def _convert_video(self, video):
        converter = Restfulie.at(self.videoconvert_settings['url']).auth(*self.videoconvert_settings['auth']).as_('application/json')
        response = converter.post(video=video).resource()
        uid = response.key
        return uid

    def _pre_store_in_sam(self, data):
        response = self.sam.put(value=data).resource()
        uid = response.key
        return uid

    def _enqueue_uid_to_granulate(self, grains_uid, video_uid, callback_url):
        send_task('nsivideogranulate.tasks.granulate_video', args=(grains_uid, video_uid, callback_url, self.sam_settings))

