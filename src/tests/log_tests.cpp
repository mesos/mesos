#include <gmock/gmock.h>

#include <string>

#include <boost/utility.hpp>

#include <process/future.hpp>
#include <process/promise.hpp>
#include <process/protobuf.hpp>

#include "common/option.hpp"
#include "common/type_utils.hpp"
#include "common/utils.hpp"

#include "log/replica.hpp"

#include "messages/messages.hpp"

using namespace mesos;
using namespace mesos::internal;
using namespace mesos::internal::log;

using namespace process;


TEST(LogTest, Cache)
{
  cache<uint64_t, std::string> cache(10);

  for (int i = 0; i < 10; i++) {
    cache.put(i, utils::stringify(i));
  }

  for (int i = 0; i < 10; i++) {
    Option<std::string> option = cache.get(i);
    ASSERT_TRUE(option.isSome());
    EXPECT_EQ(utils::stringify(i), option.get());
  }

  Option<std::string> option = Option<std::string>::none();

  option = cache.get(1);
  ASSERT_TRUE(option.isSome());
  EXPECT_EQ("1", option.get());

  cache.put(10, "10");

  option = cache.get(0);
  EXPECT_TRUE(option.isNone());

  option = cache.get(10);
  ASSERT_TRUE(option.isSome());
  EXPECT_EQ("10", option.get());

  option = cache.get(1);
  ASSERT_TRUE(option.isSome());
  EXPECT_EQ("1", option.get());

  cache.put(11, "11");

  option = cache.get(1);
  EXPECT_TRUE(option.isSome());

  option = cache.get(2);
  EXPECT_TRUE(option.isNone());
}


// Provides some testing infrastructure.
namespace {

using process::Promise;

// Implements a process of sending protobuf "requests" to a process
// and waiting for a protobuf "response", but uses futures so that
// this can be done without needing to implement a process.
template <typename Request, typename Response>
class RequestResponseProcess
  : public ProtobufProcess<RequestResponseProcess<Request, Response> >
{
public:
  RequestResponseProcess(
      const UPID& _pid,
      const Request& _request,
      const Promise<Response>& _promise)
    : pid(_pid), request(_request), promise(_promise) {}

protected:
  virtual void operator () ()
  {
    ProtobufProcess<RequestResponseProcess<Request, Response> >::send(
        pid, request);
    ProcessBase::receive();
    Response response;
    CHECK(ProcessBase::name() == response.GetTypeName());
    response.ParseFromString(ProcessBase::body());
    promise.set(response);
  }

private:
  const UPID pid;
  const Request request;
  Promise<Response> promise;
};


// Allows you to describe request/response protocols and then use
// those for sending requests and getting back responses.
template <typename Request, typename Response>
struct Protocol
{
  Future<Response> operator () (const UPID& pid, const Request& request)
  {
    Promise<Response> promise;
    RequestResponseProcess<Request, Response>* process =
      new RequestResponseProcess<Request, Response>(pid, request, promise);
    spawn(process, true);
    return promise.future();
  }
};


// Some replica protocols.
Protocol<PromiseRequest, PromiseResponse> promise;
Protocol<NopRequest, NopResponse> nop;
Protocol<AppendRequest, AppendResponse> append;
Protocol<TruncateRequest, TruncateResponse> truncate;
Protocol<LearnRequest, LearnResponse> learn;

} // namespace {


TEST(LogTest, Promise)
{
  const std::string file = ".log_test_promise";

  ReplicaProcess replica(file);
  spawn(replica);

  PromiseRequest request;
  PromiseResponse response;
  Future<PromiseResponse> future;

  request.set_id(2);

  future = promise(replica, request);

  future.await(2.0);
  ASSERT_TRUE(future.ready());

  response = future.get();
  EXPECT_TRUE(response.okay());
  EXPECT_EQ(2, response.id());
  EXPECT_TRUE(response.has_position());
  EXPECT_EQ(0, response.position());
  EXPECT_FALSE(response.has_action());

  request.set_id(1);

  future = promise(replica, request);

  future.await(2.0);
  ASSERT_TRUE(future.ready());

  response = future.get();
  EXPECT_FALSE(response.okay());
  EXPECT_EQ(1, response.id());
  EXPECT_FALSE(response.has_position());
  EXPECT_FALSE(response.has_action());

  request.set_id(3);

  future = promise(replica, request);

  future.await(2.0);
  ASSERT_TRUE(future.ready());

  response = future.get();
  EXPECT_TRUE(response.okay());
  EXPECT_EQ(3, response.id());
  EXPECT_TRUE(response.has_position());
  EXPECT_EQ(0, response.position());
  EXPECT_FALSE(response.has_action());

  terminate(replica);
  wait(replica);

  utils::os::rm(file);
}


TEST(LogTest, Append)
{
  const std::string file = ".log_test_append";

  ReplicaProcess replica(file);
  spawn(replica);

  const int id = 1;

  PromiseRequest request1;
  request1.set_id(id);

  Future<PromiseResponse> future1 = promise(replica, request1);

  future1.await(2.0);
  ASSERT_TRUE(future1.ready());

  PromiseResponse response1 = future1.get();
  EXPECT_TRUE(response1.okay());
  EXPECT_EQ(id, response1.id());
  EXPECT_TRUE(response1.has_position());
  EXPECT_EQ(0, response1.position());
  EXPECT_FALSE(response1.has_action());

  AppendRequest request2;
  request2.set_id(id);
  request2.set_position(1);
  request2.set_bytes("hello world");

  Future<AppendResponse> future2 = append(replica, request2);

  future2.await(2.0);
  ASSERT_TRUE(future2.ready());

  AppendResponse response2 = future2.get();
  EXPECT_TRUE(response2.okay());
  EXPECT_EQ(id, response2.id());
  EXPECT_EQ(1, response2.position());

  LearnRequest request3;
  request3.set_position(1);

  Future<LearnResponse> future3 = learn(replica, request3);

  future3.await(2.0);
  ASSERT_TRUE(future3.ready());

  LearnResponse response3 = future3.get();
  EXPECT_TRUE(response3.okay());
  EXPECT_TRUE(response3.has_action());

  Action action = response3.action();

  EXPECT_EQ(1, action.position());
  EXPECT_EQ(1, action.promised());
  EXPECT_TRUE(action.has_performed());
  EXPECT_EQ(1, action.performed());
  EXPECT_FALSE(action.has_learned());
  EXPECT_TRUE(action.has_type());
  EXPECT_EQ(Action::APPEND, action.type());
  EXPECT_FALSE(action.has_nop());
  EXPECT_TRUE(action.has_append());
  EXPECT_FALSE(action.has_truncate());
  EXPECT_EQ("hello world", action.append().bytes());

  terminate(replica);
  wait(replica);

  utils::os::rm(file);
}


TEST(LogTest, Recover)
{
  const std::string file = ".log_test_recover";

  ReplicaProcess replica1(file);
  spawn(replica1);

  const int id = 1;

  PromiseRequest request1;
  request1.set_id(id);

  Future<PromiseResponse> future1 = promise(replica1, request1);

  future1.await(2.0);
  ASSERT_TRUE(future1.ready());

  PromiseResponse response1 = future1.get();
  EXPECT_TRUE(response1.okay());
  EXPECT_EQ(id, response1.id());
  EXPECT_TRUE(response1.has_position());
  EXPECT_EQ(0, response1.position());
  EXPECT_FALSE(response1.has_action());

  AppendRequest request2;
  request2.set_id(id);
  request2.set_position(1);
  request2.set_bytes("hello world");

  Future<AppendResponse> future2 = append(replica1, request2);

  future2.await(2.0);
  ASSERT_TRUE(future2.ready());

  AppendResponse response2 = future2.get();
  EXPECT_TRUE(response2.okay());
  EXPECT_EQ(id, response2.id());
  EXPECT_EQ(1, response2.position());

  LearnRequest request3;
  request3.set_position(1);

  Future<LearnResponse> future3 = learn(replica1, request3);

  future3.await(2.0);
  ASSERT_TRUE(future3.ready());

  LearnResponse response3 = future3.get();
  EXPECT_TRUE(response3.okay());
  EXPECT_TRUE(response3.has_action());

  {
    Action action = response3.action();

    EXPECT_EQ(1, action.position());
    EXPECT_EQ(1, action.promised());
    EXPECT_TRUE(action.has_performed());
    EXPECT_EQ(1, action.performed());
    EXPECT_FALSE(action.has_learned());
    EXPECT_TRUE(action.has_type());
    EXPECT_EQ(Action::APPEND, action.type());
    EXPECT_FALSE(action.has_nop());
    EXPECT_TRUE(action.has_append());
    EXPECT_FALSE(action.has_truncate());
    EXPECT_EQ("hello world", action.append().bytes());
  }

  terminate(replica1);
  wait(replica1);

  ReplicaProcess replica2(file);
  spawn(replica2);

  LearnRequest request4;
  request4.set_position(1);

  Future<LearnResponse> future4 = learn(replica2, request4);

  future4.await(2.0);
  ASSERT_TRUE(future4.ready());

  LearnResponse response4 = future4.get();
  EXPECT_TRUE(response4.okay());
  EXPECT_TRUE(response4.has_action());

  {
    Action action = response4.action();

    EXPECT_EQ(1, action.position());
    EXPECT_EQ(1, action.promised());
    EXPECT_TRUE(action.has_performed());
    EXPECT_EQ(1, action.performed());
    EXPECT_FALSE(action.has_learned());
    EXPECT_TRUE(action.has_type());
    EXPECT_EQ(Action::APPEND, action.type());
    EXPECT_FALSE(action.has_nop());
    EXPECT_TRUE(action.has_append());
    EXPECT_FALSE(action.has_truncate());
    EXPECT_EQ("hello world", action.append().bytes());
  }

  terminate(replica2);
  wait(replica2);  

  utils::os::rm(file);
}
