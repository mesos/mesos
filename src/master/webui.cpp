#include <pthread.h>

#include <sstream>
#include <string>

#include <process/dispatch.hpp>

#include "master/webui.hpp"

#include "configurator/configuration.hpp"

#ifdef MESOS_WEBUI

#include <Python.h>


namespace mesos {
namespace internal {
namespace master {
namespace webui {

static std::string masterPort;
static std::string webuiPort;
static std::string logDir;


void* run(void*)
{
  LOG(INFO) << "Master web server thread started";
  Py_Initialize();
  char* argv[4];
  argv[0] = const_cast<char*>("webui/master/webui.py");
  argv[1] = const_cast<char*>(masterPort.c_str());
  argv[2] = const_cast<char*>(webuiPort.c_str());
  argv[3] = const_cast<char*>(logDir.c_str());
  PySys_SetArgv(4, argv);
  PyRun_SimpleString(
      "import sys\n"
      "sys.path.append('webui/common')\n"
      "sys.path.append('webui/bottle-0.8.3')\n");
  LOG(INFO) << "Loading webui/master/webui.py";
  FILE* file = fopen("webui/master/webui.py", "r");
  PyRun_SimpleFile(file, "webui/master/webui.py");
  fclose(file);
  Py_Finalize();
}


void start(const process::PID<Master>& master, const Configuration& conf)
{
  masterPort = utils::stringify(master.port);

  // TODO(*): It would be nice if we didn't have to be specifying
  // default values for configuration options in the code like
  // this. For example, we specify /tmp for log_dir because that is
  // what glog does, but it would be nice if at this point in the game
  // all of the configuration options have been set (from defaults or
  // from the command line, environment, or configuration file) and we
  // can just query what their values are.
  webuiPort = conf.get("webui_port", "8080");
  logDir = conf.get("log_dir", FLAGS_log_dir);

  LOG(INFO) << "Starting master web server on port " << webuiPort;

  pthread_t thread;
  if (pthread_create(&thread, 0, run, NULL) != 0) {
    LOG(FATAL) << "Failed to create master web server thread";
  }
}

} // namespace webui {
} // namespace master {
} // namespace internal {
} // namespace mesos {

#endif // MESOS_WEBUI
