#include <process/process.hpp>
#include <process/protobuf.hpp>

#include "configurator/configurator.hpp"

#include "messages/messages.hpp"

using namespace mesos::internal;

using namespace process;

using std::cerr;
using std::cout;
using std::endl;


void usage(const char* programName, const Configurator& configurator)
{
  cerr << "Usage: " << programName
       << " --master=URL --name=NAME --num-replicas=NUM [...]" << endl
       << endl
       << "'master' may be one of:" << endl
       << "  mesos://id@host:port" << endl
       << "  zoo://host1:port1,host2:port2,..." << endl
       << "  zoofile://file where file contains a host:port pair per line"
       << endl
       << endl
       << "Supported options:" << endl
       << configurator.getUsage();
}


class SubmitSchedulerProcess : public ProtobufProcess<SubmitSchedulerProcess>
{
public:
  SubmitSchedulerProcess(const UPID& _master,
                         const std::string& _name,
                         const Promise<bool>& _promise)
    : master(_master), name(_name), promise(_promise) {}

protected:
  virtual void operator () ()
  {
    cout << "Sending request to " << master << endl;
    SubmitSchedulerRequest request;
    request.set_name(name);
    send(master, request);
    receive();
    SubmitSchedulerResponse response;
    response.ParseFromString(body());
    promise.set(response.okay());
  }

private:
  const UPID master;
  const std::string name;
  Promise<bool> promise;
};


int main(int argc, char** argv)
{
  // TODO(vinod): Add options!
  Configurator configurator;

  if (argc == 2 && std::string("--help") == argv[1]) {
    usage(argv[0], configurator);
    exit(1);
  }

  Configuration conf;
  try {
    conf = configurator.load(argc, argv, true);
  } catch (const ConfigurationException& e) {
    std::cerr << "Configuration error: " << e.what() << std::endl;
    exit(1);
  }

  // Initialize libprocess library
  process::initialize();

  if (!conf.contains("master")) {
    usage(argv[0], configurator);
    exit(1);
  }

  // TODO(vinod): Parse 'master' when we add ZooKeeper support.
  UPID master(conf["master"]);

  if (!master) {
    cerr << "Could not parse --master=" << conf["master"] << endl;
    usage(argv[0], configurator);
    exit(1);
  }

  if (!conf.contains("name")) {
    usage(argv[0], configurator);
    exit(1);
  }

 /*
  if (!conf.contains("num-replicas")) {
    usage(argv[0], configurator);
    exit(1);
  }
*/

  LOG(INFO) << "Submitting scheduler ...";

  Promise<bool> promise;

  SubmitSchedulerProcess process(master, conf["name"], promise);
  process::spawn(process);

  Future<bool> future = promise.future();
  future.await(5.0);

  if (future.ready()) {
    if (future.get()) {
      cout << "Scheduler submitted successfully" << endl;
    } else {
      cout << "Failed to submit scheduler" << endl;
    }
  } else {
    cout << "Timed out waiting for scheduler" << endl;
  }

  // TODO(vinod): This (or similar) code would be used when Ben
  // implements the new Protocol mechanism for communication.
  //   Protocol<SubmitSchedulerRequest, SubmitSchedulerResponse> submit;

  //   SubmitSchedulerRequest request;
  //   request.set_name(name);

  //   Future<SubmitSchedulerResponse> future = submit(master, request);

  //   future.await();

  return 0;
}
