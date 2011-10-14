#!/usr/bin/env python
# encoding: utf-8
from base64 import decodestring, b64encode
from nsi.granulate import Granulate
from restfulie import Restfulie
from celery.task import task, Task
from celery.execute import send_task
from pickle import dumps


class VideoException(Exception):
    pass

class VideoDownloadException(Exception):
    pass

class VideoGranulation(Task):

    def run(self, grains_uid, video_uid, filename, callback_url, sam_settings, video_link):
        self.filename = filename
        self.grains_uid = grains_uid
        self.video_uid = video_uid
        self.callback_url = callback_url
        self.video_link = video_link
        print video_link

        self.sam = Restfulie.at(sam_settings['url']).as_('application/json').auth(*sam_settings['auth'])
        self._granulate_video()

    def _granulate_video(self):
        print "Starting new job."
        grains = self._get_from_sam(self.grains_uid).resource()
        if self.video_link:
            self._video = self._download_video(self.video_link)
        else:
            self._video = self._get_from_sam(self.video_uid).resource().data
        print "Video size: %d" % len(self._video)
        if hasattr(grains.data, 'done') and not grains.data.done:
            print "Starting the granularization..."
            self._process_video()
            print "Done the granularization."
            if not self.callback_url == None:
                print "Callback task sent."
                send_task('nsivideogranulate.tasks.Callback', args=(self.callback_url, self.grains_uid),
                           queue='granulate', routing_key='granulate')
            else:
                print "No callback."
        else:
            raise VideoException("Video already granulated.")

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
        #encoded_grains = [b64encode(image.getContent().getvalue()) for image in grains['image_list']]
        #encoded_videos = [b64encode(video.getContent().getvalue()) for video in grains['file_list']]
        encoded_grains = [dumps(image) for image in grains['image_list']]
        encoded_videos = [dumps(video) for video in grains['file_list']]
        self._store_in_sam(self.grains_uid, {'images':encoded_grains, 'videos':encoded_videos})

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

