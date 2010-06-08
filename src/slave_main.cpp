#include <getopt.h>

#include "slave.hpp"
#include "slave_webui.hpp"

using namespace std;

using namespace nexus::internal::slave;


void usage(const char *programName)
{
  cerr << "Usage: " << programName
       << " [--cpus NUM]"
       << " [--mem NUM]"
       << " [--isolation TYPE]"
       << " [--zookeeper ZOO_SERVERS]"
       << " [--quiet]"
       << " <master_pid>"
       << endl
       << "ZOO_SERVERS is a url of the form:"
       << "  zoo://host1:port1,host2:port2,..., or"
       << "  zoofile://file where file contains a host:port pair per line"
       << endl;
}


int main(int argc, char **argv)
{
  if (argc == 2 && string("--help") == argv[1]) {
    usage(argv[0]);
    exit(1);
  }

  option options[] = {
    {"cpus", required_argument, 0, 'c'},
    {"mem", required_argument, 0, 'm'},
    {"isolation", required_argument, 0, 'i'},
    {"zookeeper", required_argument, 0, 'z'},
    {"quiet", no_argument, 0, 'q'},
  };

  Resources resources(1, 1 * Gigabyte);
  string isolation = "process";
  string zookeeper = "";
  bool quiet = false;

  int opt;
  int index;
  while ((opt = getopt_long(argc, argv, "c:m:i:z:q", options, &index)) != -1) {
    switch (opt) {
      case 'c': 
	resources.cpus = atoi(optarg);
        break;
      case 'm':
	resources.mem = atoll(optarg);
        break;
      case 'i':
	isolation = optarg;
        break;
      case 'z':
	zookeeper = optarg;
        break;
      case 'q':
        quiet = true;
        break;
      case '?':
        // Error parsing options; getopt prints an error message, so just exit
        exit(1);
        break;
      default:
        break;
    }
  }

  if (!quiet)
    google::SetStderrLogging(google::INFO);
  else
    MasterDetector::setQuiet(true);

  FLAGS_log_dir = "/tmp";
  FLAGS_logbufsecs = 1;
  google::InitGoogleLogging(argv[0]);

  // Check that we either have zookeeper as an argument or exactly one
  // non-option argument (i.e., the master PID).
  if (zookeeper.empty() && optind != argc - 1) {
    usage(argv[0]);
    exit(1);
  }

  // Read the optional argument if necessary.
  if (zookeeper.empty()) {
    zookeeper = argv[optind];
  }

  LOG(INFO) << "Build: " << BUILD_DATE << " by " << BUILD_USER;
  LOG(INFO) << "Starting Nexus slave";

  Slave* slave = new Slave(zookeeper, resources, false, isolation);
  PID pid = Process::spawn(slave);

#ifdef NEXUS_WEBUI
  if (chdir(dirname(argv[0])) != 0)
    fatalerror("could not change into %s for running webui", dirname(argv[0]));
  startSlaveWebUI(pid);
#endif

  Process::wait(pid);
  return 0;
}
