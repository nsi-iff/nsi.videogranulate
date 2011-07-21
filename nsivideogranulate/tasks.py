#!/usr/bin/env python
# encoding: utf-8
from base64 import decodestring, b64encode
from nsi.granulate import Granulate
from restfulie import Restfulie
from celery.task import task

class VideoException(Exception):
    pass

@task
def granulate_video(grains_uid, video_uid, callback_url, sam_settings):
    print "Starting new job."
    grains = get_from_sam(grains_uid, sam_settings)
    grains = grains.resource()
    video = get_from_sam(video_uid, sam_settings).resource()
    if hasattr(grains.data, 'done') and not grains.data.done:
        print "Starting the granularization..."
        granulate(video_uid, grains_uid, sam_settings)
        print "Done the granularization."
        if not callback_url == None:
            print callback_url
            response = Restfulie.at(callback_url).as_('application/json').post(key=grains_uid, status='Done')
            print "Callback executed."
            print "Response code: %s" % response.code
        else:
            print "No callback."
    else:
        raise VideoException("Video already granulated.")

def granulate(video_uid, grains_uid, sam_settings):
    video = get_from_sam(video_uid, sam_settings).resource().data
    granulate = Granulate()
    grains = granulate.granulate('nothing.ogv', decodestring(video))
    encoded_grains = [b64encode(image.getContent().getvalue()) for image in grains['image_list']]
    store_in_sam(grains_uid, {'grains':encoded_grains}, sam_settings)

def store_in_sam(uid, video, sam_settings):
    sam = Restfulie.at(sam_settings['url']).as_('application/json').auth(*sam_settings['auth'])
    return sam.post(key=uid, value=video)

def get_from_sam(uid, sam_settings):
    sam = Restfulie.at(sam_settings['url']).as_('application/json').auth(*sam_settings['auth'])
    return sam.get(key=uid)

