import requests
import urllib2
import json
from datetime import datetime
import threading
import math

API_URL = 'http://127.0.0.1:5000/api/'
sessionId = 0

intervalBufferLevelThread = None
countme = 1

reporting_interval = 1000

PLAYER = None

class VideoState():
  ended = 0
  init = 1
  play = 2
  playing = 3
  pause = 4
  seek = 5
  rebuffering = 6


class CurrentState():
  videoState = VideoState.ended
  bufferStart = 0
  streamPos = 0
  bitRate = 0
  url = ''
  last_bitrate = 0
  last_width = 0
  last_height = 0
  last_representation_change = 0
  bitrate = 0
  width = 0
  height = 0
  last_representation = None

class Endpoints():
  STALLING = 'stalling'
  ADAPTATION = 'adaptation'
  SESSION = 'experiments'
  BUFFER_LEVEL = 'bufferlevel'
  BW_ESTIMATE = 'bw_estimation'
  BW_SAMPLE = 'bw_sample'

results = {
    'stalling':[],
    'adaptation':[],
    'experiments':[],
    'bufferlevel':[],
    'bw_estimation':[],
    'bw_sample':[]
}

def getResults():
    return results

def sendRequest(endpoint, data):
  global sessionId
  #headers = {'content-type': 'application/json'}
  #response = requests.post(API_URL + endpoint, data=data, headers=headers)
  results.get(endpoint).append(data)
  print {endpoint: data}
  #if endpoint == Endpoints.SESSION:
    #sessionId = response.json()['id']
    #print('session_id is : ' , sessionId)

def getUTCTimestamp():
  return datetime.now().strftime("%a, %d %b %Y %H:%M:%S")

def init(experimentId, dashPlayer, videoName, videoUrl, playerType, adaptionAlgorithm):
  global PLAYER

  data = {
        'timestamp': getUTCTimestamp(),
        'client_id': 'no_client_id',
        'experiment_id': experimentId,
        'test_type': videoName,
        'video_url': videoUrl,
        'browser': 'python_2.7',
        'player_type': playerType,
        'adaption_algorithm': adaptionAlgorithm
  }

  PLAYER = dashPlayer

  sendRequest(Endpoints.SESSION, data)

def onBufferLevelUpdate(event):
  data = {
    'timestamp': getUTCTimestamp(),
    'playback_position': event.pos,
    'buffer_level': event.buffer_level,
    'experiment': sessionId
    }

def onStalling(duration, streamPos):
  data = {
    'timestamp': getUTCTimestamp(),
    'eventtype': 'stalling',
    'playback_position': streamPos,
    'experiment': sessionId,
    'duration': duration * 1000
    }

  sendRequest(Endpoints.STALLING, data)

def onInitStalling(data):
  #data.timestamp = datetime.now()
  sendRequest(Endpoints.STALLING, data)

def reportBandwidthSample(playbackPosition, delayMs, bytes_):
  print('report bandwidth sampke', playbackPosition, delayMs, bytes_)
  data = {
    'timestamp': getUTCTimestamp(),
    'playback_position': playbackPosition,
    'delay_ms': delayMs,
    'bytes': bytes_,
    'experiment': sessionId
    }
  print('report bandwidth sampke', playbackPosition, delayMs, bytes)
  sendRequest(Endpoints.BW_SAMPLE, data)

def bufferingStart(time, streamPos):
  #CurrentState.videoState = videoState.rebuffering
  CurrentState.bufferStart = time
  CurrentState.streamPos = streamPos


def bufferingEnd(stallingTime):
  stallingDuration = stallingTime - alreadyLoggedStalling
  alreadyLoggedstalling = stallingTime

  onStalling(stallingDuration)

  #CurrentState.videoState = VideoState.playing

def onAdaptation(event):

  current_playback_time = event['playback_position']
  time_in_representation = current_playback_time - CurrentState.last_representation_change

  if len(results.get('adaptation')) == 1:
      results.get('adaptation')[0]['init_representation'] = results.get('adaptation')[0]['bitrate']

  if CurrentState.last_bitrate == event['bitrate']:
      results.get('adaptation')[-1]['time_in_representation'] = time_in_representation
      return

  print "Playback time {}".format(event['playback_position'])
  CurrentState.last_representation_change = current_playback_time

  amplitude = 0

  if CurrentState.last_representation != None:
      amplitude = math.fabs(CurrentState.last_representation - event.get('segment_layer', 0))
      CurrentState.last_representation =  event.get('segment_layer', 0)


  data = {
    'timestamp': getUTCTimestamp(),
    'last_bitrate': CurrentState.bitrate,
    'last_height': CurrentState.height,
    'last_width': CurrentState.width,
    'experiment': sessionId,
    'height':event['height'],
    'width': event['width'],
    'bitrate':event['bitrate'],
    'playback_position': event['playback_position'],
    'time_in_representation': time_in_representation,
    'amplitude': amplitude,
    'representation' : event.get('segment_layer', 0),
    }

  CurrentState.last_width =  CurrentState.width
  CurrentState.last_height =  CurrentState.height
  CurrentState.last_bitrate = CurrentState.bitrate
  CurrentState.width = event['width']
  CurrentState.height = event['height']
  CurrentState.bitrate = event['bitrate']
  CurrentState.last_representation = event.get('segment_layer', 0)

  sendRequest(Endpoints.ADAPTATION, data)


def onPlay():
  CurrentState.videoState = VideoState.play

def onPause():
  CurrentState.videoState = VideoState.pause

def onEnded():
  CurrentState.videoState = VideoState.ended

def onPlaying():
  CurrentState.videoState = VideoSTate.playing

def startupDelay(delayMs):

  data = {
    'timestamp': getUTCTimestamp(),
    'eventtype': 'initialStalling',
    'playback_position': 0,
    'experiment': sessionId,
    'duration': delayMs
    }

  sendRequest(Endpoints.STALLING, data)


def setBufferLevelProvider():
  global PLAYER
  global intervalBufferLevelThread
  print(PLAYER)

  def reportBufferLevel():
    global PLAYER
    print(PLAYER)
    data = {
      'timestamp': getUTCTimestamp(),
      'playback_position': PLAYER.playback_timer.time(),
      'buffer_level':  PLAYER.buffer_length,
      'experiment': sessionId
      }

    sendRequest(Endpoints.BUFFER_LEVEL, data)

  intervalBufferLevelThread = setInterval(reportBufferLevel, reporting_interval / 1000)


def setInterval(func, sec):

  def inner_function():
    setInterval(func, sec)
    func()

  thread = threading.Timer(sec, inner_function)
  thread.setDaemon(True)
  thread.start()

  return thread

"""
Update: had to remove the "starting" of the old thread because the recursive nature of the interval does not work with python
This function manually starts every interval from within the interval. So as long the stoping condition is not met,
the interval is running.
def runInterval():
  intervalBufferLevelThread.start()
  """

def clearInterval(interval=intervalBufferLevelThread):
  interval.cancel()
  return True

"""
  dash_el.reportBandwidthEstimate = function(abrManager, player) {
      var data = {
        'bw': abrManager.getBandwidthEstimate(),
        'playback_position': player.getStats().playTime,
        'timestamp': new Date().toUTCString(),
        'experiment': session_id
      }

      report(data, endpoints.BW_ESTIMATE);
  }

  dash_el.registerBandwidthEstimateLogging = function(abrManager, player) {
    var self = this
    intervalBandwidthEstimateLogging = setInterval(function() {
      if(currentState.state != videoState.ended && currentState.state != videoState.pause) {
        self.reportBandwidthEstimate(abrManager, player)
      } else {
        //console.log("no buffering report because is paused or ended");
      }
    }, dash_el.settings.reporting_interval);
  }


  return dash_el;
"""
