#!/usr/bin/env python
# encoding: utf-8
from base64 import decodestring, b64encode
from nsi.granulate import Granulate
from restfulie import Restfulie
from celery.task import task, Task
from celery.execute import send_task
from json import dumps


class VideoException(Exception):
    pass

class VideoDownloadException(Exception):
    pass

class VideoGranulation(Task):

    def run(self, video_uid, filename, callback_url, sam_settings, video_link, verb='POST'):
        self.filename = filename
        self.video_uid = video_uid
        self.callback_url = callback_url
        self.callback_verb = verb.lower()
        self.video_link = video_link
        print video_link

        self.sam = Restfulie.at(sam_settings['url']).as_('application/json').auth(*sam_settings['auth'])
        self._granulate_video()

    def _granulate_video(self):
        print "Starting new job."
        if self.video_link:
            self._video = self._download_video(self.video_link)
        else:
            response = self._get_from_sam(self.video_uid).resource().data
            self._video = response.video
        print "Video size: %d" % len(self._video)
        if hasattr(response, 'granulated') and not response.granulated:
            print "Starting the granularization..."
            self._process_video()
            self._update_video_grains_keys()
            del self._video
            print "Done the granularization."
        if not self.callback_url == None:
            print "Callback task sent."
            # send_task('nsivideogranulate.tasks.Callback', args=(self.callback_url, self.callback_verb, self.grains_uid),
                       # queue='granulate', routing_key='granulate')
        else:
            print "No callback."
        #else:
            #raise VideoException("Video already granulated.")

    def _download_video(self, video_link):
        try:
            print "Downloading video from %s" % video_link
            video = Restfulie.at(video_link).get().body
        except Exception:
            raise VideoDownloadException("Could not download the video from %s" % video_link)
        else:
            print "Video downloaded."
        return b64encode(video)

    def _process_video(self):
        granulate = Granulate()
        grains = granulate.granulate(str(self.filename), decodestring(self._video))
        grains_keys = {'images':[], 'videos':[]}
        if grains.has_key('image_list'):
            for image in grains['image_list']:
                encoded_image = {
                                       'filename':image.id,
                                       'content':b64encode(image.getContent().getvalue()),
                                       'description':image.description
                                  }
                image_key = self.sam.put(value=encoded_image).resource().key
                grains_keys['images'].append(image_key)

        if grains.has_key('file_list'):
            for video in grains['file_list']:
                encoded_video = {
                                      'filename':video.id,
                                      'content':b64encode(video.getContent().getvalue())
                                 }
                video_key = self.sam.put(value=encoded_video).resource().key
                grains_keys['videos'].append(video_key)
        self.grains_keys = grains_keys
        del grains

    def _update_video_grains_keys(self):
        new_video = {'video':self._video, 'granulated':True, 'grains_keys':self.grains_keys}
        self.sam.post(key=self.video_uid, value=new_video)

    def _get_from_sam(self, uid):
        return self.sam.get(key=uid)


class Callback(Task):

    max_retries = 3

    def run(self, url, verb, grains_uid):
        try:
            print "Sending callback to %s" % url
            restfulie = Restfulie.at(url).as_('application/json')
            response = getattr(restfulie, verb)(uid=grains_uid, done=True)
            print response.body
        except Exception, e:
            Callback.retry(exc=e, countdown=10)
        else:
            print "Callback executed."
            print "Response code: %s" % response.code

