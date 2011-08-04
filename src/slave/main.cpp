#include <libgen.h>

#include "common/build.hpp"
#include "common/logging.hpp"

#include "configurator/configurator.hpp"

#include "detector/detector.hpp"

#include "isolation_module_factory.hpp"
#include "slave.hpp"
#include "webui.hpp"

using namespace mesos::internal;
using namespace mesos::internal::slave;

using std::cerr;
using std::endl;
using std::string;


void usage(const char *programName, const Configurator& configurator)
{
  cerr << "Usage: " << programName
       << " --master=URL [...]" << endl
       << endl
       << "URL may be one of:" << endl
       << "  mesos://id@host:port" << endl
       << "  zoo://host1:port1,host2:port2,..." << endl
       << "  zoofile://file where file contains a host:port pair per line"
       << endl
       << endl
       << "Supported options:" << endl
       << configurator.getUsage();
}


int main(int argc, char** argv)
{
  Configurator configurator;
  Logging::registerOptions(&configurator);
  Slave::registerOptions(&configurator);
  configurator.addOption<int>("port", 'p', "Port to listen on", 0);
  configurator.addOption<string>("ip", "IP address to listen on");
  configurator.addOption<string>("master", 'm', "Master URL");
  configurator.addOption<string>("isolation", 'i', "Isolation module name", "process");
#ifdef MESOS_WEBUI
  configurator.addOption<int>("webui_port", 'w', "Web UI port", 8081);
#endif

  if (argc == 2 && string("--help") == argv[1]) {
    usage(argv[0], configurator);
    exit(1);
  }

  Configuration conf;
  try {
    conf = configurator.load(argc, argv, true);
  } catch (ConfigurationException& e) {
    cerr << "Configuration error: " << e.what() << endl;
    exit(1);
  }

  Logging::init(argv[0], conf);

  if (conf.contains("port")) {
    setenv("LIBPROCESS_PORT", conf["port"].c_str(), 1);
  }

  if (conf.contains("ip")) {
    setenv("LIBPROCESS_IP", conf["ip"].c_str(), 1);
  }

  // Initialize libprocess library (but not glog, done above).
  process::initialize(false);

  if (!conf.contains("master")) {
    cerr << "Master URL argument (--master) required." << endl;
    exit(1);
  }
  string master = conf["master"];

  string isolation = conf["isolation"];
  LOG(INFO) << "Creating \"" << isolation << "\" isolation module";
  IsolationModule* isolationModule = IsolationModule::create(isolation);

  if (isolationModule == NULL) {
    cerr << "Unrecognized isolation type: " << isolation << endl;
    exit(1);
  }

  LOG(INFO) << "Build: " << build::DATE << " by " << build::USER;
  LOG(INFO) << "Starting Mesos slave";

  if (chdir(dirname(argv[0])) != 0) {
    fatalerror("Could not chdir into %s", dirname(argv[0]));
  }

  Slave* slave = new Slave(conf, false, isolationModule);
  process::spawn(slave);

  MasterDetector* detector = MasterDetector::create(
      master,
      slave->self(),
      false,
      Logging::isQuiet(conf));

#ifdef MESOS_WEBUI
  webui::start(slave->self(), conf);
#endif

  process::wait(slave->self());
  delete slave;

  MasterDetector::destroy(detector);
  IsolationModule::destroy(isolationModule);

  return 0;
}
