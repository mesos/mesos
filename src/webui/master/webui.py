import sys
import bottle
import commands

from bottle import route, send_file, template


@route('/')
def index():
  bottle.TEMPLATES.clear() # For rapid development
  return template("index", master_port = master_port)


@route('/framework/:id#[0-9-]*#')
def framework(id):
  bottle.TEMPLATES.clear() # For rapid development
  return template("framework", master_port = master_port, framework_id = id)


@route('/static/:filename#.*#')
def static(filename):
  send_file(filename, root = './webui/static')


@route('/log/:level#[A-Z]*#')
def log_full(level):
  send_file('mesos-master.' + level, root = log_dir,
            guessmime = False, mimetype = 'text/plain')


@route('/log/:level#[A-Z]*#/:lines#[0-9]*#')
def log_tail(level, lines):
  bottle.response.content_type = 'text/plain'
  command = 'tail -%s %s/mesos-master.%s' % (lines, log_dir, level)
  return commands.getoutput(command)


bottle.TEMPLATE_PATH.append('./webui/master/')

# TODO(*): Add an assert to confirm that all the arguments we are
# expecting have been passed to us, which will give us a better error
# message when they aren't!

master_port = sys.argv[1]
webui_port = sys.argv[2]
log_dir = sys.argv[3]

bottle.debug(True)
bottle.run(host = '0.0.0.0', port = webui_port)
