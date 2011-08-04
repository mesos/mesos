% import json
% import urllib

% from webui_lib import *

% url = "http://localhost:" + slave_port + "/slave/state.json"
% data = urllib.urlopen(url).read()
% state = json.loads(data)

<html>
<head>
  <title>Framework {{framework_id}}</title>
  <link rel="stylesheet" type="text/css" href="/static/stylesheet.css" />
</head>
<body>

<h1>Framework {{framework_id}}</h1>

% # Find the framework with the given ID.
% framework = None
% for i in range(len(state['frameworks'])):
%   if state['frameworks'][i]['id'] == framework_id:
%     framework = state['frameworks'][i]
%   end
% end

% if framework != None:
%   # Count the number of tasks and sum the resources.
%   tasks = 0
%   cpus = 0
%   mem = 0
%   for executor in framework['executors']:
%     for task in executor['tasks']:
%       cpus += task['resources']['cpus']
%       mem += task['resources']['mem']
%       tasks += 1
%     end
%   end

<p>
  Name: {{framework['name']}}<br />
  Running Tasks: {{tasks}}<br />
  CPUs: {{cpus}}<br />
  MEM: {{format_mem(mem)}}<br />
</p>

<p>Logs:
  <a href="/framework-logs/{{framework['id']}}/stdout">[stdout]</a>
  <a href="/framework-logs/{{framework['id']}}/stderr">[stderr]</a>
</p>

<h2>Tasks</h2>

% # TODO: sort these by task ID.
% if tasks > 0:
<table class="lists">
  <tr>
    <th class="lists">ID</th>
    <th class="lists">Name</th>
    <th class="lists">State</th>
  </tr>
  % for executor in framework['executors']:
  %   for task in executor['tasks']:
  <tr>
    <td class="lists">{{task['id']}}</td>
    <td class="lists">{{task['name']}}</td>
    <td class="lists">{{task['state']}}</td>
  </tr>
  %   end
  % end
</table>
% else:
<p>No tasks are running.</p>
% end
% else:
<p>No framework with ID {{framework_id}} is active on this slave.</p>
% end

<p><a href="/">Back to Slave</a></p>

</body>
</html>
