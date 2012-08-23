#!/usr/bin/env python
#-*- coding:utf-8 -*-

from json import loads
from base64 import decodestring
import functools
import cyclone.web
from twisted.internet import defer
from twisted.python import log
from zope.interface import implements
from urlparse import urlsplit

from nsivideogranulate.interfaces.http import IHttp
from nsi.granulate import Granulate
from restfulie import Restfulie
from celery.execute import send_task


def auth(method):
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        auth_type, auth_data = self.request.headers.get("Authorization").split()
        if not auth_type == "Basic":
            raise cyclone.web.HTTPAuthenticationRequired("Basic", realm="Restricted Access")
        user, password = decodestring(auth_data).split(":")
        # authentication itself
        if not self.settings.auth.authenticate(user, password):
            raise cyclone.web.HTTPError(401, "Unauthorized")
        return method(self, *args, **kwargs)
    return wrapper


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
        self._task_queue = self.settings.task_queue
        self.sam = Restfulie.at(self.sam_settings['url']).auth(*self.sam_settings['auth']).as_('application/json')

    @auth
    @defer.inlineCallbacks
    @cyclone.web.asynchronous
    def get(self):
        request_as_json = self._load_request_as_json()
        video_key = self._load_request_as_json().get('video_key')
        uid = request_as_json.get('key') or video_key
        if not video_key and not uid:
            log.msg('GET failed!')
            log.msg('The video key was not provided.')
            raise cyclone.web.HTTPError(400, 'Malformed request.')
        response = yield self.sam.get(key=uid)
        if response.code == "404":
            log.msg('GET failed!')
            log.msg('The video with key %s was not found at SAM.' % uid)
            raise cyclone.web.HTTPError(404, "Key not found.")
        elif response.code == "401":
            log.msg('GET failed!')
            log.msg('Authentication with SAM failed.')
            raise cyclone.web.HTTPError(401, "Couldn't authenticate with SAM.")
        elif response.code == "500":
            log.msg('GET failed!')
            log.msg('Error while trying to connect to SAM.')
            raise cyclone.web.HTTPError(500, "Error while trying to connect to SAM.")
        response = response.resource()
        self.set_header('Content-Type', 'application/json')
        if video_key:
            grains = self._get_grains_keys(video_key)
            log.msg('Found the grains for the video with key %s.' % video_key)
            self.finish(cyclone.web.escape.json_encode(grains))
        elif hasattr(response.data, 'granulated') and  response.data.granulated:
            log.msg('Video with key %s is granulated.' % uid)
            self.finish(cyclone.web.escape.json_encode({'done':True}))
        else:
            log.msg('Video with key %s is not granulated.' % uid)
            self.finish(cyclone.web.escape.json_encode({'done':False}))

    def _get_grains_keys(self, video_key):
        video_uid = video_key
        response = self.sam.get(key=video_uid)
        if response.code == '404':
            log.msg("GET failed!")
            log.msg("Couldn't find any value for the key: %s" % key)
            raise cyclone.web.HTTPError(404, 'Key not found in SAM.')
        elif response.code == "401":
            log.msg('GET failed!')
            log.msg('Authentication with SAM failed.')
            raise cyclone.web.HTTPError(401, "Couldn't authenticate with SAM.")
        elif response.code == "500":
            log.msg('GET failed!')
            log.msg('Error while trying to connect to SAM.')
            raise cyclone.web.HTTPError(500, "Error while trying to connect to SAM.")
        sam_entry = loads(response.body)
        grains = sam_entry['data']['grains_keys']
        return grains

    @auth
    @defer.inlineCallbacks
    @cyclone.web.asynchronous
    def post(self):
        request = self._load_request_as_json()
        callback_url = request.get('callback') or None
        filename = request.get('filename') or urlsplit(request_as_json.get('video_link')).path.split('/')[-1] or None
        if not filename:
            log.msg('POST failed.')
            log.msg("Couldn't calculate the filename or it wasn't provided.")
            raise cyclone.web.HTTPError(400, 'Malformed request.')
        video_uid = request.get('video_uid') or None
        video_link = None
        callback_verb = request.get('verb') or 'POST'

        # se nao tiver link....
        if request.get('video'):
            video = request.get('video')
            video_uid = yield self._pre_store_in_sam({'video':video, 'granulated':False})
            log.msg('Got an entire video to be granulated and stored it in SAM key %s.' % video_uid)
            del video
        elif request.get('video_uid'):
            log.msg('Got a video to granualte from in key %s.' % request.get('video_uid'))
        # se tiver um link
        elif request.get('video_link'):
            video_uid = yield self._pre_store_in_sam({'video':'', 'granulated':False})
            video_link = request.get('video_link')
            log.msg('Got a video to download from %s and store in key %s.' % (video_link, video_uid))
        else:
            log.msg('POST failed.')
            log.msg('Neither a video, a link, or an uid were provided.')
            raise cyclone.web.HTTPError(400, 'Malformed request.')

        response = yield self._enqueue_uid_to_granulate(video_uid, filename, callback_url, callback_verb, video_link)

        self.set_header('Content-Type', 'application/json')
        self.finish(cyclone.web.escape.json_encode({'video_key':video_uid}))

    def _pre_store_in_sam(self, data):
        response = self.sam.put(value=data)
        if response.code == '404':
            log.msg("GET failed!")
            log.msg("Couldn't find any value for the key: %s" % key)
            raise cyclone.web.HTTPError(404, 'Key not found in SAM.')
        elif response.code == "401":
            log.msg('GET failed!')
            log.msg('Authentication with SAM failed.')
            raise cyclone.web.HTTPError(401, "Couldn't authenticate with SAM.")
        elif response.code == "500":
            log.msg('GET failed!')
            log.msg('Error while trying to connect to SAM.')
        uid = response.resource().key
        return uid

    def _enqueue_uid_to_granulate(self, video_uid, filename, callback_url, callback_verb, video_link):
        try:
            send_task(
                        'nsivideogranulate.tasks.VideoGranulation',
                        args=(self._task_queue, video_uid, filename, callback_url, self.sam_settings, video_link, callback_verb),
                        queue=self._task_queue, routing_key=self._task_queue
                     )
        except:
            log.msg('POST failed.')
            log.msg('Could not enqueue the video to granulate.')
            raise cyclobe.web.HTTPError(500, 'Can not enqueue the video to granulate.')

