import requests
import urllib2
import json
from datetime import datetime
import threading

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

class Endpoints():
  STALLING = 'stalling'
  ADAPTATION = 'adaptation'
  SESSION = 'experiments'
  BUFFER_LEVEL = 'bufferlevel'
  BW_ESTIMATE = 'bw_estimation'
  BW_SAMPLE = 'bw_sample'


def sendRequest(endpoint, data):
  global sessionId
  headers = {'content-type': 'application/json'}
  response = requests.post(API_URL + endpoint, data=data, headers=headers)

  if endpoint == Endpoints.SESSION:
    sessionId = response.json()['id']
    print('session_id is : ' , sessionId)

def getUTCTimestamp():
  return str(datetime.now())

def init(experimentId, dashPlayer, videoName, videoUrl, playerType, adaptionAlgorithm):
  global PLAYER

  data = json.dumps({
        'timestamp': getUTCTimestamp(),
        'client_id': 'no_client_id',
        'experiment_id': experimentId,
        'test_type': videoName,
        'video_url': videoUrl,
        'browser': 'python_2.7',
        'player_type': playerType,
        'adaption_algorithm': adaptionAlgorithm
  })

  PLAYER = dashPlayer

  sendRequest(Endpoints.SESSION, data)

def onBufferLevelUpdate(event):
  data = json.dumps({
    'timestamp': getUTCTimestamp(),
    'playback_position': event.pos,
    'buffer_level': event.buffer_level,
    'experiment': sessionId
    })

def onStalling(duration):
  data = json.dumps({
    'timestamp': getUTCTimestamp(),
    'eventtype': 'stalling',
    'playback_position': CurrentState.streamPos,
    'experiment': sessionId,
    'duration': duration * 1000
    })

  sendRequest(Endpoints.STALLING, data)

def reportBandwidthSample(playbackPosition, delayMs, bytes):
  print('report bandwidth sampke', playbackPosition, delayMs, bytes)
  data = json.dumps({
    'timestamp': getUTCTimestamp(),
    'playback_position': playbackPosition,
    'delay_ms': delayMs,
    'bytes': bytes,
    'experiment': sessionId
    })
  print('report bandwidth sampke', playbackPosition, delayMs, bytes)
  sendRequest(Endpoints.BW_SAMPLE, data)

def bufferingStart(time, streamPos):
  CurrentState.videoState = videoState.rebuffering
  CurrentState.bufferStart = time
  CurrentState.streamPos = streamPos


def bufferingEnd(stallingTime):
  stallingDuration = stallingTime - alreadyLoggedStalling
  alreadyLoggedstalling = stallingTime

  onStalling(stallingDuration)

  CurrentState.videoState = VideoState.playing

def onAdaptation(event):

  current_playback_time = event['playback_position']
  time_in_representation = current_playback_time - CurrentState.last_representation_change
  CurrentState.last_representation_change = current_playback_time

  data = json.dumps({
    'timestamp': getUTCTimestamp(),
    'last_bitrate': CurrentState.bitrate,
    'last_height': CurrentState.height,
    'last_width': CurrentState.width,
    'experiment': sessionId,
    'height':event['height'],
    'width': event['width'],
    'bitrate':event['bitrate'],
    'playback_position': event['playback_position'],
    'time_in_representation': time_in_representation
    })

  CurrentState.last_width =  CurrentState.width
  CurrentState.last_height =  CurrentState.height
  CurrentState.last_bitrate = CurrentState.bitrate
  CurrentState.width = event['width']
  CurrentState.height = event['height']
  CurrentState.bitrate = event['bitrate']

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

  data = json.dumps({
    'timestamp': getUTCTimestamp(),
    'eventtype': 'initialStalling',
    'playback_position': 0,
    'experiment': sessionId,
    'duration': delayMs
    })

  sendRequest(Endpoints.STALLING, data)


def setBufferLevelProvider():
  global PLAYER
  global intervalBufferLevelThread
  print(PLAYER)

  def reportBufferLevel():
    global PLAYER
    print(PLAYER)
    data = json.dumps({
      'timestamp': getUTCTimestamp(),
      'playback_position': PLAYER.playback_timer.time(),
      'buffer_level':  PLAYER.buffer_length,
      'experiment': sessionId
      })

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

def clearInterval(interval):
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
