import subprocess
import argparse
import time
import datetime
import calendar
import json
import shlex
import os
import sys
import logging, logging.handlers

LEVELS = {'debug': logging.DEBUG,
          'info': logging.INFO,
          'warning': logging.WARNING,
          'error': logging.ERROR,
          'critical': logging.CRITICAL}
WAIT_TIME = 0.5
LOGGER_MODE = 'info'
FILE_LOGGER = 'log.file'
STREAM_LOGGER = 'log.stream'

def listener_configurer(filename, days):
  root_file = logging.getLogger(FILE_LOGGER)
  root_stdout = logging.getLogger(STREAM_LOGGER)
  # Define type of logger
  h = logging.handlers.TimedRotatingFileHandler(filename, when='D', interval=1, backupCount=days)
  s = logging.StreamHandler()
  # Define formatting
  f = logging.Formatter('%(asctime)s %(levelname)s %(processName)s %(message)s')
  h.setFormatter(f)
  s.setFormatter(f)
  # Add handler
  root_file.addHandler(h)
  root_stdout.addHandler(s)

def applog(level):
  def queue_handler_decorate(fn):
    def fn_wrapper(self, msg):
      root_file = logging.getLogger(FILE_LOGGER)
      root_file.addHandler(self.handler)
      root_file.setLevel(LEVELS.get(LOGGER_MODE)) # send all messages, for demo; no other level or filter logic applied.

      root_stdout = logging.getLogger(STREAM_LOGGER)
      root_stdout.addHandler(self.handler)
      root_stdout.setLevel(LEVELS.get(LOGGER_MODE))

      root_file.log(LEVELS.get(level), msg)
      root_stdout.log(LEVELS.get(level), msg)
    return fn_wrapper
  return queue_handler_decorate


########################################################################
class SWFRerunTimedOut:
  """"""

  #----------------------------------------------------------------------
  def __init__(self, region, domain):
    """Constructor"""
    self.__region = region
    self.__domain = domain
    pass
  
  #----------------------------------------------------------------------
  def toEpoch(self, year, month, dd, hour, min):
    """"""
    return calendar.timegm(datetime.datetime(year, month, dd, hour, min).timetuple())
    
  
  #----------------------------------------------------------------------
  def fetchTimedoutTasks(self, ts):
    """"""
    if not (id(ts) and type(ts) in (datetime.datetime, datetime.date)):
      raise Exception("incorrect object type: expected datetime")
    
    now = datetime.datetime.today()
    epochNow = self.toEpoch(now.year, now.month, now.day, now.hour, now.minute)
    epochOld = self.toEpoch(ts.year, ts.month, ts.day, ts.hour, ts.minute)
    cmds = "aws swf list-closed-workflow-executions --domain \"{0}\" --start-time-filter oldestDate={1},latestDate={2} --close-status-filter status=TIMED_OUT --region {3}"
    cmds = cmds.format(self.__domain, epochOld, epochNow, self.__region)
    outputs = subprocess.check_output(shlex.split(cmds))
    j = json.loads(outputs)
    return [ (info.get('execution').get('runId'), info.get('execution').get('workflowId')) for info in j.get('executionInfos')]
      
  #----------------------------------------------------------------------
  def fetchExecHistory(self, workflowId, runId):
    """"""
    cmds = "aws swf get-workflow-execution-history --domain \"{0}\" --execution workflowId={1},runId={2} --region {3}"
    cmds = cmds.format(self.__domain, workflowId, runId, self.__region)
    outputs = subprocess.check_output(shlex.split(cmds))
    j = json.loads(outputs)
    return j.get('events')[0].get('workflowExecutionStartedEventAttributes')
  
  #----------------------------------------------------------------------
  def composeDocument(self, workflowId, history):
    """"""
    result = {}
    result.setdefault('domain', self.__domain)
    result.setdefault('workflowId', workflowId)
    result.setdefault('workflowType', history.get('workflowType'))
    result.setdefault('taskList', history.get('taskList'))
    result.setdefault('input', history.get('input'))
    result.setdefault('executionStartToCloseTimeout', history.get('executionStartToCloseTimeout'))
    result.setdefault('taskStartToCloseTimeout', history.get('taskStartToCloseTimeout'))
    result.setdefault('childPolicy', history.get('childPolicy'))
    return result
  
  #----------------------------------------------------------------------
  def rerunTasks(self, ts):
    """"""
    cmds = "aws swf start-workflow-execution --region {0} --cli-input-json file://{1}"
    tempFile = "workflow.json"

    tasks = self.fetchTimedoutTasks(ts)
    for idx, task in enumerate(tasks):
      runId = task[0]
      workflowId = task[-1]

      history = self.fetchExecHistory(workflowId, runId)
      time.sleep(WAIT_TIME)

      doc = self.composeDocument(workflowId, history)
      time.sleep(WAIT_TIME)

      fp = open(tempFile, 'w')
      fp.write(json.dumps(doc))
      fp.close()

      outputs = subprocess.check_output(shlex.split(cmds.format(self.__region, tempFile)))
      #print("re-run workflowId: {0}, old runId: {1}, new runId: {2}".format(workflowId, runId, json.loads(outputs).get('runId')))
      time.sleep(WAIT_TIME)
      self.logInfo("{1} > re-run workflowId: {0}".format(workflowId, idx))

    if os.path.isfile(tempFile):
      os.remove(tempFile)
      
  #----------------------------------------------------------------------
  @applog("info")
  def logInfo(self, msg):
    """"""
    return msg
    
  
if __name__ == "__main__":
  parser = argparse.ArgumentParser(description="Re-run SWF timed out tasks in batch", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
  parser.add_argument('--region', type=str, default='ap-southeast-2', help='SWF region')
  parser.add_argument('--domain', type=str, help='SWF domain', required=True)
  parser.add_argument('date', type=str, metavar='YYYY-MM-DD', help='date from which SWF tasks timed out')
  parser.add_argument('-t', '--time', type=str, metavar='hh:mm', default='00:00', help='time from which SWF tasks timed out')
  parser.add_argument('--log', type=str, default='swf-rerun.log', help='log output')
  args = parser.parse_args()
  listener_configurer(args.log, 7)
  try:
    swf = SWFRerunTimedOut(args.region, args.domain)
    d = args.date.split('-')
    t = args.time.split(':')
    year, month, day = int(d[0]), int(d[1]), int(d[-1])
    hour, minute = int(t[0]), int(t[-1])
    ts = datetime.datetime(year, month, day, hour, minute)
  
    swf.rerunTasks(ts)
  except Exception, e:
    print(e.message)
    sys.exit(1)
    