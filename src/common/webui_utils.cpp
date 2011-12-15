#include "common/webui_utils.hpp"

#include <pthread.h>
#include <sys/stat.h>
#include "config/config.hpp"
#include "common/utils.hpp"

#ifdef MESOS_WEBUI
#include <Python.h>

namespace mesos {
namespace internal {
namespace utils {
namespace webui {

namespace {

struct WebuiArgs
{
  std::string webuiDir;
  std::string webuiScript;
  std::string rpcPort;
  std::string webuiPort;
  std::string logDir;
  std::string workDir;
};

void* run(void* rawArgs)
{
  WebuiArgs* args = static_cast<WebuiArgs*>(rawArgs);
  Py_Initialize();
  char* argv[5];
  argv[0] = const_cast<char*>(args->webuiScript.c_str());
  argv[1] = const_cast<char*>(args->rpcPort.c_str());
  argv[2] = const_cast<char*>(args->webuiPort.c_str());
  argv[3] = const_cast<char*>(args->logDir.c_str());
  argv[4] = const_cast<char*>(args->workDir.c_str());
  PySys_SetArgv(4, argv);
  std::string webuiPathSetup =
      "import sys\n"
      "sys.path.append('" + args->webuiDir + "/webui/common')\n"
      "sys.path.append('" + args->webuiDir + "/bottle-0.8.3')\n";
  PyRun_SimpleString(webuiPathSetup.c_str());
  LOG(INFO) << "Loading " << args->webuiScript.c_str();
  FILE* file = fopen(args->webuiScript.c_str(), "r");
  PyRun_SimpleFile(file, args->webuiScript.c_str());
  fclose(file);
  Py_Finalize();
  delete args;
}

} // namespace {

void start(const Configuration& conf, const std::string& rawScript,
           int rpcPort, int defaultWebuiPort)
{
  WebuiArgs* args = new WebuiArgs;
  args->webuiDir = conf.get("webui_dir", MESOS_WEBUIDIR);
  std::string script = rawScript;
  if (script[0] != '/') {
    script = args->webuiDir + "/" + script;
  }

  struct stat st;
  if (-1 == stat(script.c_str(), &st)) {
    LOG(WARNING) << "Couldn't find webui script in " << script;
    LOG(WARNING) << "Assuming uninstalled; using webui_dir=.";
    // Try '.' because that's where the webui files should be if we are running
    // out of the build directory.
    args->webuiDir = ".";
    script = args->webuiDir + "/" + rawScript;
    if (-1 == stat(script.c_str(), &st)) {
      LOG(FATAL) << "Couldn't find webui script " << rawScript;
    }
  }
  // TODO(*): It would be nice if we didn't have to be specifying
  // default values for configuration options in the code like
  // this. For example, we specify /tmp for log_dir because that is
  // what glog does, but it would be nice if at this point in the game
  // all of the configuration options have been set (from defaults or
  // from the command line, environment, or configuration file) and we
  // can just query what their values are.
  args->webuiScript = script;
  args->rpcPort = utils::stringify(rpcPort);
  args->webuiPort = conf.get("webui_port", utils::stringify(defaultWebuiPort));
  args->logDir = conf.get("log_dir", FLAGS_log_dir);
  if (conf.contains("work_dir")) {
    args->workDir = conf.get("work_dir", "");
  } else if (conf.contains("home")) {
    args->workDir = conf.get("home", "") + "/work";
  } else {
    args->workDir = "work";
  }
  LOG(INFO) << "Starting web server on port " << args->webuiPort
            << " (running " << script << ")";

  pthread_t thread;
  if (pthread_create(&thread, 0, run, static_cast<void*>(args)) != 0) {
    LOG(FATAL) << "Failed to create web server thread";
  }
  pthread_detach(thread);
}

} // namespace process {
} // namespace utils {
} // namespace internal {
} // namespace mesos {

#endif // def MESOS_WEBUI
