#!/usr/bin/env python
# encoding: utf-8
from base64 import decodestring, b64encode
from nsi.granulate import Granulate
from restfulie import Restfulie
from celery.task import task

class VideoException(Exception):
    pass

@task
def granulate_video(grains_uid, video_uid, callback_url):
    print "Starting new job."
    grains = get_from_sam(grains_uid)
    grains = grains.resource()
    video = get_from_sam(video_uid).resource()
    if hasattr(grains.data, 'done') and not grains.data.done:
        print "Starting the granularization..."
        granulate(video_uid, grains_uid)
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

def granulate(video_uid, grains_uid):
    video = get_from_sam(video_uid).resource().data
    granulate = Granulate()
    grains = granulate.granulate('nothing.ogv', decodestring(video))
    encoded_grains = [b64encode(image.getContent().getvalue()) for image in grains['image_list']]
    store_in_sam(grains_uid, {'grains':encoded_grains})

def store_in_sam(uid, video):
    sam = Restfulie.at("http://0.0.0.0:8888/").as_("application/json").auth('test', 'test')
    return sam.post(key=uid, value=video)

def get_from_sam(uid):
    sam = Restfulie.at("http://0.0.0.0:8888/").as_("application/json").auth('test', 'test')
    return sam.get(key=uid)
