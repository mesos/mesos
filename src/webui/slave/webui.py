import bottle
import commands
import datetime
import json
import os
import sys
import urllib

from bottle import abort, route, send_file, template


@route('/')
def index():
  bottle.TEMPLATES.clear() # For rapid development
  return template("index", slave_port = slave_port)


@route('/framework/:id#.*#')
def framework(id):
  bottle.TEMPLATES.clear() # For rapid development
  return template("framework", slave_port = slave_port, framework_id = id)


@route('/static/:filename#.*#')
def static(filename):
  send_file(filename, root = './webui/static')


@route('/log/:level#[A-Z]*#')
def log_full(level):
  send_file('mesos-slave.' + level, root = log_dir,
            guessmime = False, mimetype = 'text/plain')


@route('/log/:level#[A-Z]*#/:lines#[0-9]*#')
def log_tail(level, lines):
  bottle.response.content_type = 'text/plain'
  command = 'tail -%s %s/mesos-slave.%s' % (lines, log_dir, level)
  return commands.getoutput(command)


@route('/framework-logs/:fid#.*#/:log_type#[a-z]*#')
def framework_log_full(fid, log_type):
  url = "http://localhost:" + slave_port + "/slave/state.json"
  data = urllib.urlopen(url).read()
  state = json.loads(data)
  sid = state['id']
  if sid != -1:
    dir = '%s/slave-%s/fw-%s' % (work_dir, sid, fid)
    i = max(os.listdir(dir))
    exec_dir = '%s/slave-%s/fw-%s/%s' % (work_dir, sid, fid, i)
    send_file(log_type, root = exec_dir,
              guessmime = False, mimetype = 'text/plain')
  else:
    abort(403, 'Slave not yet registered with master')


@route('/framework-logs/:fid#.*#/:log_type#[a-z]*#/:lines#[0-9]*#')
def framework_log_tail(fid, log_type, lines):
  bottle.response.content_type = 'text/plain'
  url = "http://localhost:" + slave_port + "/slave/state.json"
  data = urllib.urlopen(url).read()
  state = json.loads(data)
  sid = state['id']
  if sid != -1:
    dir = '%s/slave-%s/fw-%s' % (work_dir, sid, fid)
    i = max(os.listdir(dir))
    filename = '%s/slave-%s/fw-%s/%s/%s' % (work_dir, sid, fid, i, log_type)
    command = 'tail -%s %s' % (lines, filename)
    return commands.getoutput(command)
  else:
    abort(403, 'Slave not yet registered with master')


bottle.TEMPLATE_PATH.append('./webui/slave/')

# TODO(*): Add an assert to confirm that all the arguments we are
# expecting have been passed to us, which will give us a better error
# message when they aren't!

slave_port = sys.argv[1]
webui_port = sys.argv[2]
log_dir = sys.argv[3]
work_dir = sys.argv[4]

bottle.debug(True)
bottle.run(host = '0.0.0.0', port = webui_port)
