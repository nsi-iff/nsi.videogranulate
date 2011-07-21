#!/usr/bin/env python
# encoding: utf-8
from base64 import decodestring, b64encode
from nsi.granulate import Granulate
from restfulie import Restfulie
from celery.task import task, Task
from celery.execute import send_task


class VideoException(Exception):
    pass

class VideoGranulation(Task):

    def run(self, grains_uid, video_uid, callback_url, sam_settings):
        self.grains_uid = grains_uid
        self.video_uid = video_uid
        self.callback_url = callback_url

        self.sam = Restfulie.at(sam_settings['url']).as_('application/json').auth(*sam_settings['auth'])

        self._granulate_video()

    def _granulate_video(self):
        print "Starting new job."
        grains = self._get_from_sam(self.grains_uid).resource()
        video = self._get_from_sam(self.video_uid).resource()
        print "Tamanho do vídeo %d" % len(video.data)
        if hasattr(grains.data, 'done') and not grains.data.done:
            print "Starting the granularization..."
            self._process_video()
            print "Done the granularization."
            if not self.callback_url == None:
                print "Callback task sent."
                send_task('nsivideogranulate.tasks.Callback', args=(self.callback_url, self.grains_uid))
            else:
                print "No callback."
        else:
            raise VideoException("Video already granulated.")

    def _process_video(self):
        video = self._get_from_sam(self.video_uid).resource().data
        granulate = Granulate()
        grains = granulate.granulate('nothing.ogv', decodestring(video))
        encoded_grains = [b64encode(image.getContent().getvalue()) for image in grains['image_list']]
        self._store_in_sam(self.grains_uid, {'grains':encoded_grains})
        del granulate

    def _store_in_sam(self, uid, data):
        return self.sam.post(key=uid, value=data)

    def _get_from_sam(self, uid):
        return self.sam.get(key=uid)


class Callback(Task):

    max_retries = 3

    def run(self, url, grains_uid):
        try:
            print "Sending callback to %s" % url
            response = Restfulie.at(url).as_('application/json').post(key=grains_uid, status='Done')
        except Exception, e:
            Callback.retry(exc=e, countdown=10)
        else:
            print "Callback executed."
            print "Response code: %s" % response.code

