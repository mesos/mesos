#include <gmock/gmock.h>

#include <set>
#include <string>

#include <process/future.hpp>
#include <process/protobuf.hpp>

#include "common/option.hpp"
#include "common/type_utils.hpp"
#include "common/utils.hpp"

#include "log/coordinator.hpp"
#include "log/replica.hpp"

#include "messages/messages.hpp"

#include "tests/utils.hpp"

using namespace mesos;
using namespace mesos::internal;
using namespace mesos::internal::log;
using namespace mesos::internal::test;

using namespace process;

using testing::_;
using testing::Eq;
using testing::Return;


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


TEST(ReplicaTest, Promise)
{
  const std::string file = ".log_tests_promise";

  utils::os::rm(file);

  ReplicaProcess replica(file);
  spawn(replica);

  PromiseRequest request;
  PromiseResponse response;
  Future<PromiseResponse> future;

  request.set_id(2);

  future = protocol::promise(replica, request);

  future.await(2.0);
  ASSERT_TRUE(future.isReady());

  response = future.get();
  EXPECT_TRUE(response.okay());
  EXPECT_EQ(2, response.id());
  EXPECT_TRUE(response.has_position());
  EXPECT_EQ(0, response.position());
  EXPECT_FALSE(response.has_action());

  request.set_id(1);

  future = protocol::promise(replica, request);

  future.await(2.0);
  ASSERT_TRUE(future.isReady());

  response = future.get();
  EXPECT_FALSE(response.okay());
  EXPECT_EQ(1, response.id());
  EXPECT_FALSE(response.has_position());
  EXPECT_FALSE(response.has_action());

  request.set_id(3);

  future = protocol::promise(replica, request);

  future.await(2.0);
  ASSERT_TRUE(future.isReady());

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


TEST(ReplicaTest, Append)
{
  const std::string file = ".log_tests_append";

  utils::os::rm(file);

  ReplicaProcess replica(file);
  spawn(replica);

  const int id = 1;

  PromiseRequest request1;
  request1.set_id(id);

  Future<PromiseResponse> future1 = protocol::promise(replica, request1);

  future1.await(2.0);
  ASSERT_TRUE(future1.isReady());

  PromiseResponse response1 = future1.get();
  EXPECT_TRUE(response1.okay());
  EXPECT_EQ(id, response1.id());
  EXPECT_TRUE(response1.has_position());
  EXPECT_EQ(0, response1.position());
  EXPECT_FALSE(response1.has_action());

  WriteRequest request2;
  request2.set_id(id);
  request2.set_position(1);
  request2.set_type(Action::APPEND);
  request2.mutable_append()->set_bytes("hello world");

  Future<WriteResponse> future2 = protocol::write(replica, request2);

  future2.await(2.0);
  ASSERT_TRUE(future2.isReady());

  WriteResponse response2 = future2.get();
  EXPECT_TRUE(response2.okay());
  EXPECT_EQ(id, response2.id());
  EXPECT_EQ(1, response2.position());

  Result<Action> result = call(replica, &ReplicaProcess::read, 1);
  ASSERT_TRUE(result.isSome());

  Action action = result.get();
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


TEST(ReplicaTest, Recover)
{
  const std::string file = ".log_tests_recover";

  utils::os::rm(file);

  ReplicaProcess replica1(file);
  spawn(replica1);

  const int id = 1;

  PromiseRequest request1;
  request1.set_id(id);

  Future<PromiseResponse> future1 = protocol::promise(replica1, request1);

  future1.await(2.0);
  ASSERT_TRUE(future1.isReady());

  PromiseResponse response1 = future1.get();
  EXPECT_TRUE(response1.okay());
  EXPECT_EQ(id, response1.id());
  EXPECT_TRUE(response1.has_position());
  EXPECT_EQ(0, response1.position());
  EXPECT_FALSE(response1.has_action());

  WriteRequest request2;
  request2.set_id(id);
  request2.set_position(1);
  request2.set_type(Action::APPEND);
  request2.mutable_append()->set_bytes("hello world");

  Future<WriteResponse> future2 = protocol::write(replica1, request2);

  future2.await(2.0);
  ASSERT_TRUE(future2.isReady());

  WriteResponse response2 = future2.get();
  EXPECT_TRUE(response2.okay());
  EXPECT_EQ(id, response2.id());
  EXPECT_EQ(1, response2.position());

  Result<Action> result1 = call(replica1, &ReplicaProcess::read, 1);
  ASSERT_TRUE(result1.isSome());

  {
    Action action = result1.get();
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

  Result<Action> result2 = call(replica2, &ReplicaProcess::read, 1);
  ASSERT_TRUE(result2.isSome());

  {
    Action action = result2.get();
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


TEST(ReplicaTest, RecoverAfterCrash)
{
  const std::string file = ".log_tests_recover_after_crash";

  utils::os::rm(file);

  ReplicaProcess replica1(file);
  spawn(replica1);

  const int id = 1;

  PromiseRequest request1;
  request1.set_id(id);

  Future<PromiseResponse> future1 = protocol::promise(replica1, request1);

  future1.await(2.0);
  ASSERT_TRUE(future1.isReady());

  PromiseResponse response1 = future1.get();
  EXPECT_TRUE(response1.okay());
  EXPECT_EQ(id, response1.id());
  EXPECT_TRUE(response1.has_position());
  EXPECT_EQ(0, response1.position());
  EXPECT_FALSE(response1.has_action());

  WriteRequest request2;
  request2.set_id(id);
  request2.set_position(1);
  request2.set_type(Action::APPEND);
  request2.mutable_append()->set_bytes("hello world");

  Future<WriteResponse> future2 = protocol::write(replica1, request2);

  future2.await(2.0);
  ASSERT_TRUE(future2.isReady());

  WriteResponse response2 = future2.get();
  EXPECT_TRUE(response2.okay());
  EXPECT_EQ(id, response2.id());
  EXPECT_EQ(1, response2.position());

  Result<Action> result1 = call(replica1, &ReplicaProcess::read, 1);
  ASSERT_TRUE(result1.isSome());

  {
    Action action = result1.get();
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

  // Write some random bytes to the end of the file.
  {
    Result<int> result = utils::os::open(file, O_WRONLY | O_APPEND);

    ASSERT_TRUE(result.isSome());
    int fd = result.get();

    for (int i = 0; i < 128; i++) {
      char c = rand() % 256;
      write(fd, &c, sizeof(c));
    }

    utils::os::close(fd);
  }

  ReplicaProcess replica2(file);
  spawn(replica2);

  Result<Action> result2 = call(replica2, &ReplicaProcess::read, 1);
  ASSERT_TRUE(result2.isSome());

  {
    Action action = result2.get();
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


TEST(CoordinatorTest, AppendRead)
{
  const std::string file1 = ".log_tests_append_read1";
  const std::string file2 = ".log_tests_append_read2";

  utils::os::rm(file1);
  utils::os::rm(file2);

  ReplicaProcess replica1(file1);
  spawn(replica1);

  ReplicaProcess replica2(file2);
  spawn(replica2);

  // TODO(benh): Use utils::set<n>(replica1.self(), replica2.self())
  GroupProcess group;
  spawn(group);

  dispatch(group, &GroupProcess::add, replica1.self());
  dispatch(group, &GroupProcess::add, replica2.self());

  Coordinator coord(2, &replica1, &group);

  {
    Result<uint64_t> result = coord.elect();
    ASSERT_TRUE(result.isSome());
    EXPECT_EQ(0, result.get());
  }

  uint64_t position;

  {
    Result<uint64_t> result2 = coord.append("hello world");
    ASSERT_TRUE(result2.isSome());
    position = result2.get();
    EXPECT_EQ(1, position);
  }

  {
    Result<std::list<std::pair<uint64_t, std::string> > > result =
      coord.read(position, position);
    ASSERT_TRUE(result.isSome());
    ASSERT_EQ(1, result.get().size());
    EXPECT_EQ(position, result.get().front().first);
    EXPECT_EQ("hello world", result.get().front().second);
  }

  terminate(group);
  wait(group);

  terminate(replica1);
  wait(replica1);

  terminate(replica2);
  wait(replica2);

  utils::os::rm(file1);
  utils::os::rm(file2);
}


TEST(CoordinatorTest, AppendReadError)
{
  const std::string file1 = ".log_tests_append_read_error1";
  const std::string file2 = ".log_tests_append_read_error2";

  utils::os::rm(file1);
  utils::os::rm(file2);

  ReplicaProcess replica1(file1);
  spawn(replica1);

  ReplicaProcess replica2(file2);
  spawn(replica2);

  GroupProcess group;
  spawn(group);

  dispatch(group, &GroupProcess::add, replica1.self());
  dispatch(group, &GroupProcess::add, replica2.self());

  Coordinator coord(2, &replica1, &group);

  {
    Result<uint64_t> result = coord.elect();
    ASSERT_TRUE(result.isSome());
    EXPECT_EQ(0, result.get());
  }

  uint64_t position;

  {
    Result<uint64_t> result2 = coord.append("hello world");
    ASSERT_TRUE(result2.isSome());
    position = result2.get();
    EXPECT_EQ(1, position);
  }

  {
    position += 1;
    Result<std::list<std::pair<uint64_t, std::string> > > result =
      coord.read(position, position);
    ASSERT_TRUE(result.isError());
    EXPECT_EQ("Bad read range (index <= from)", result.error());
  }

  terminate(group);
  wait(group);

  terminate(replica1);
  wait(replica1);

  terminate(replica2);
  wait(replica2);

  utils::os::rm(file1);
  utils::os::rm(file2);
}


// TODO(benh): The coordinator tests that rely on timeouts can't rely
// on pausing the clock because when they get run with other tests
// there could be some lingering timeouts that cause the clock to
// advance such that the timeout within Coordinator::elect or
// Coordinator::append get started beyond what we expect. If this
// happens it doesn't matter what we "advance" the clock by, since
// certain orderings might still cause the test to hang, waiting for a
// future that started later than expected because the clock got
// updated unknowingly. This would be solved if Coordinator was
// actually a Process because then it would have a time associated
// with it and all processes that it creates (transitively). There
// might be a way to fix this by giving threads a similar role in
// libprocess, but until that happens these tests do not use the clock
// and are therefore disabled by default so as not to pause the tests
// for random unknown periods of time (but can still be run manually).

TEST(CoordinatorTest, DISABLED_ElectNoQuorum)
{
  const std::string file = ".log_tests_elect_no_quorum";

  utils::os::rm(file);

  ReplicaProcess replica(file);
  spawn(replica);

  GroupProcess group;
  spawn(group);

  dispatch(group, &GroupProcess::add, replica.self());

  Coordinator coord(2, &replica, &group);

  {
    Result<uint64_t> result = coord.elect();
    ASSERT_TRUE(result.isNone());
  }

  terminate(group);
  wait(group);

  terminate(replica);
  wait(replica);

  utils::os::rm(file);
}


TEST(CoordinatorTest, DISABLED_AppendNoQuorum)
{
  const std::string file1 = ".log_tests_append_no_quorum1";
  const std::string file2 = ".log_tests_append_no_quorum2";

  utils::os::rm(file1);
  utils::os::rm(file2);

  ReplicaProcess replica1(file1);
  spawn(replica1);

  ReplicaProcess replica2(file2);
  spawn(replica2);

  GroupProcess group;
  spawn(group);

  dispatch(group, &GroupProcess::add, replica1.self());
  dispatch(group, &GroupProcess::add, replica2.self());

  Coordinator coord(2, &replica1, &group);

  {
    Result<uint64_t> result = coord.elect();
    ASSERT_TRUE(result.isSome());
    EXPECT_EQ(0, result.get());
  }

  terminate(replica1);
  wait(replica1);

  {
    Result<uint64_t> result = coord.append("hello world");
    ASSERT_TRUE(result.isNone());
  }

  terminate(group);
  wait(group);

  terminate(replica2);
  wait(replica2);

  utils::os::rm(file1);
  utils::os::rm(file2);
}


TEST(CoordinatorTest, Failover)
{
  const std::string file1 = ".log_tests_failover1";
  const std::string file2 = ".log_tests_failover2";

  utils::os::rm(file1);
  utils::os::rm(file2);

  ReplicaProcess replica1(file1);
  spawn(replica1);

  ReplicaProcess replica2(file2);
  spawn(replica2);

  GroupProcess group1;
  spawn(group1);

  dispatch(group1, &GroupProcess::add, replica1.self());
  dispatch(group1, &GroupProcess::add, replica2.self());

  Coordinator coord1(2, &replica1, &group1);

  {
    Result<uint64_t> result = coord1.elect();
    ASSERT_TRUE(result.isSome());
    EXPECT_EQ(0, result.get());
  }

  uint64_t position;

  {
    Result<uint64_t> result = coord1.append("hello world");
    ASSERT_TRUE(result.isSome());
    position = result.get();
    EXPECT_EQ(1, position);
  }

  terminate(group1);
  wait(group1);

  GroupProcess group2;
  spawn(group2);

  dispatch(group2, &GroupProcess::add, replica1.self());
  dispatch(group2, &GroupProcess::add, replica2.self());

  Coordinator coord2(2, &replica2, &group2);

  {
    Result<uint64_t> result = coord2.elect();
    ASSERT_TRUE(result.isSome());
    EXPECT_EQ(position, result.get());
  }

  {
    Result<std::list<std::pair<uint64_t, std::string> > > result =
      coord2.read(position, position);
    ASSERT_TRUE(result.isSome());
    ASSERT_EQ(1, result.get().size());
    EXPECT_EQ(position, result.get().front().first);
    EXPECT_EQ("hello world", result.get().front().second);
  }

  terminate(group2);
  wait(group2);

  terminate(replica1);
  wait(replica1);

  terminate(replica2);
  wait(replica2);

  utils::os::rm(file1);
  utils::os::rm(file2);
}


TEST(CoordinatorTest, Demoted)
{
  const std::string file1 = ".log_tests_demoted1";
  const std::string file2 = ".log_tests_demoted2";

  utils::os::rm(file1);
  utils::os::rm(file2);

  ReplicaProcess replica1(file1);
  spawn(replica1);

  ReplicaProcess replica2(file2);
  spawn(replica2);

  GroupProcess group1;
  spawn(group1);

  dispatch(group1, &GroupProcess::add, replica1.self());
  dispatch(group1, &GroupProcess::add, replica2.self());

  Coordinator coord1(2, &replica1, &group1);

  {
    Result<uint64_t> result = coord1.elect();
    ASSERT_TRUE(result.isSome());
    EXPECT_EQ(0, result.get());
  }

  uint64_t position;

  {
    Result<uint64_t> result = coord1.append("hello world");
    ASSERT_TRUE(result.isSome());
    position = result.get();
    EXPECT_EQ(1, position);
  }

  GroupProcess group2;
  spawn(group2);

  dispatch(group2, &GroupProcess::add, replica1.self());
  dispatch(group2, &GroupProcess::add, replica2.self());

  Coordinator coord2(2, &replica2, &group2);

  {
    Result<uint64_t> result = coord2.elect();
    ASSERT_TRUE(result.isSome());
    EXPECT_EQ(position, result.get());
  }

  {
    Result<uint64_t> result = coord1.append("hello moto");
    ASSERT_TRUE(result.isError());
    EXPECT_EQ("Coordinator demoted", result.error());
  }

  {
    Result<uint64_t> result = coord2.append("hello hello");
    ASSERT_TRUE(result.isSome());
    position = result.get();
    EXPECT_EQ(2, position);
  }

  {
    Result<std::list<std::pair<uint64_t, std::string> > > result =
      coord2.read(position, position);
    ASSERT_TRUE(result.isSome());
    ASSERT_EQ(1, result.get().size());
    EXPECT_EQ(position, result.get().front().first);
    EXPECT_EQ("hello hello", result.get().front().second);
  }

  terminate(group1);
  wait(group1);

  terminate(group2);
  wait(group2);

  terminate(replica1);
  wait(replica1);

  terminate(replica2);
  wait(replica2);

  utils::os::rm(file1);
  utils::os::rm(file2);
}


TEST(CoordinatorTest, Fill)
{
  const std::string file1 = ".log_tests_fill1";
  const std::string file2 = ".log_tests_fill2";
  const std::string file3 = ".log_tests_fill3";

  utils::os::rm(file1);
  utils::os::rm(file2);
  utils::os::rm(file3);

  ReplicaProcess replica1(file1);
  spawn(replica1);

  ReplicaProcess replica2(file2);
  spawn(replica2);

  GroupProcess group1;
  spawn(group1);

  dispatch(group1, &GroupProcess::add, replica1.self());
  dispatch(group1, &GroupProcess::add, replica2.self());

  Coordinator coord1(2, &replica1, &group1);

  {
    Result<uint64_t> result = coord1.elect();
    ASSERT_TRUE(result.isSome());
    EXPECT_EQ(0, result.get());
  }

  uint64_t position;

  {
    Result<uint64_t> result = coord1.append("hello world");
    ASSERT_TRUE(result.isSome());
    position = result.get();
    EXPECT_EQ(1, position);
  }

  terminate(group1);
  wait(group1);

  terminate(replica1);
  wait(replica1);

  ReplicaProcess replica3(file3);
  spawn(replica3);

  GroupProcess group2;
  spawn(group2);

  dispatch(group2, &GroupProcess::add, replica2.self());
  dispatch(group2, &GroupProcess::add, replica3.self());

  Coordinator coord2(2, &replica3, &group2);

  {
    Result<uint64_t> result = coord2.elect();
    ASSERT_TRUE(result.isNone());
    result = coord2.elect();
    ASSERT_TRUE(result.isSome());
    EXPECT_EQ(position, result.get());
  }

  {
    Result<std::list<std::pair<uint64_t, std::string> > > result =
      coord2.read(position, position);
    ASSERT_TRUE(result.isSome());
    ASSERT_EQ(1, result.get().size());
    EXPECT_EQ(position, result.get().front().first);
    EXPECT_EQ("hello world", result.get().front().second);
  }

  terminate(group2);
  wait(group2);

  terminate(replica2);
  wait(replica2);

  terminate(replica3);
  wait(replica3);

  utils::os::rm(file1);
  utils::os::rm(file2);
  utils::os::rm(file3);
}


TEST(CoordinatorTest, NotLearnedFill)
{
  MockFilter filter;
  process::filter(&filter);

  EXPECT_MSG(filter, _, _, _)
    .WillRepeatedly(Return(false));

  EXPECT_MSG(filter, Eq(LearnedMessage().GetTypeName()), _, _)
    .WillRepeatedly(Return(true));

  const std::string file1 = ".log_tests_not_learned_fill1";
  const std::string file2 = ".log_tests_not_learned_fill2";
  const std::string file3 = ".log_tests_not_learned_fill3";

  utils::os::rm(file1);
  utils::os::rm(file2);
  utils::os::rm(file3);

  ReplicaProcess replica1(file1);
  spawn(replica1);

  ReplicaProcess replica2(file2);
  spawn(replica2);

  GroupProcess group1;
  spawn(group1);

  dispatch(group1, &GroupProcess::add, replica1.self());
  dispatch(group1, &GroupProcess::add, replica2.self());

  Coordinator coord1(2, &replica1, &group1);

  {
    Result<uint64_t> result = coord1.elect();
    ASSERT_TRUE(result.isSome());
    EXPECT_EQ(0, result.get());
  }

  uint64_t position;

  {
    Result<uint64_t> result = coord1.append("hello world");
    ASSERT_TRUE(result.isSome());
    position = result.get();
    EXPECT_EQ(1, position);
  }

  terminate(group1);
  wait(group1);

  terminate(replica1);
  wait(replica1);

  ReplicaProcess replica3(file3);
  spawn(replica3);

  GroupProcess group2;
  spawn(group2);

  dispatch(group2, &GroupProcess::add, replica2.self());
  dispatch(group2, &GroupProcess::add, replica3.self());

  Coordinator coord2(2, &replica3, &group2);

  {
    Result<uint64_t> result = coord2.elect();
    ASSERT_TRUE(result.isNone());
    result = coord2.elect();
    ASSERT_TRUE(result.isSome());
    EXPECT_EQ(position, result.get());
  }

  {
    Result<std::list<std::pair<uint64_t, std::string> > > result =
      coord2.read(position, position);
    ASSERT_TRUE(result.isSome());
    ASSERT_EQ(1, result.get().size());
    EXPECT_EQ(position, result.get().front().first);
    EXPECT_EQ("hello world", result.get().front().second);
  }

  terminate(group2);
  wait(group2);

  terminate(replica2);
  wait(replica2);

  terminate(replica3);
  wait(replica3);

  utils::os::rm(file1);
  utils::os::rm(file2);
  utils::os::rm(file3);

  process::filter(NULL);
}


TEST(CoordinatorTest, MultipleAppends)
{
  const std::string file1 = ".log_tests_multiple_appends1";
  const std::string file2 = ".log_tests_multiple_appends2";

  utils::os::rm(file1);
  utils::os::rm(file2);

  ReplicaProcess replica1(file1);
  spawn(replica1);

  ReplicaProcess replica2(file2);
  spawn(replica2);

  GroupProcess group;
  spawn(group);

  dispatch(group, &GroupProcess::add, replica1.self());
  dispatch(group, &GroupProcess::add, replica2.self());

  Coordinator coord(2, &replica1, &group);

  {
    Result<uint64_t> result = coord.elect();
    ASSERT_TRUE(result.isSome());
    EXPECT_EQ(0, result.get());
  }

  for (uint64_t position = 1; position <= 10; position++) {
    Result<uint64_t> result = coord.append(utils::stringify(position));
    ASSERT_TRUE(result.isSome());
    EXPECT_EQ(position, result.get());
  }

  {
    Result<std::list<std::pair<uint64_t, std::string> > > result =
      coord.read(1, 10);
    ASSERT_TRUE(result.isSome());
    const std::list<std::pair<uint64_t, std::string> >& list = result.get();
    EXPECT_EQ(10, list.size());
    std::list<std::pair<uint64_t, std::string> >::const_iterator iterator;
    for (iterator = list.begin(); iterator != list.end(); ++iterator) {
      EXPECT_EQ(utils::stringify(iterator->first), iterator->second);
    }
  }

  terminate(group);
  wait(group);

  terminate(replica1);
  wait(replica1);

  terminate(replica2);
  wait(replica2);

  utils::os::rm(file1);
  utils::os::rm(file2);
}


TEST(CoordinatorTest, MultipleAppendsNotLearnedFill)
{
  MockFilter filter;
  process::filter(&filter);

  EXPECT_MSG(filter, _, _, _)
    .WillRepeatedly(Return(false));

  EXPECT_MSG(filter, Eq(LearnedMessage().GetTypeName()), _, _)
    .WillRepeatedly(Return(true));

  const std::string file1 = ".log_tests_multiple_appends_not_learned_fill1";
  const std::string file2 = ".log_tests_multiple_appends_not_learned_fill2";
  const std::string file3 = ".log_tests_multiple_appends_not_learned_fill3";

  utils::os::rm(file1);
  utils::os::rm(file2);
  utils::os::rm(file3);

  ReplicaProcess replica1(file1);
  spawn(replica1);

  ReplicaProcess replica2(file2);
  spawn(replica2);

  GroupProcess group1;
  spawn(group1);

  dispatch(group1, &GroupProcess::add, replica1.self());
  dispatch(group1, &GroupProcess::add, replica2.self());

  Coordinator coord1(2, &replica1, &group1);

  {
    Result<uint64_t> result = coord1.elect();
    ASSERT_TRUE(result.isSome());
    EXPECT_EQ(0, result.get());
  }

  for (uint64_t position = 1; position <= 10; position++) {
    Result<uint64_t> result = coord1.append(utils::stringify(position));
    ASSERT_TRUE(result.isSome());
    EXPECT_EQ(position, result.get());
  }

  terminate(group1);
  wait(group1);

  terminate(replica1);
  wait(replica1);

  ReplicaProcess replica3(file3);
  spawn(replica3);

  GroupProcess group2;
  spawn(group2);

  dispatch(group2, &GroupProcess::add, replica2.self());
  dispatch(group2, &GroupProcess::add, replica3.self());

  Coordinator coord2(2, &replica3, &group2);

  {
    Result<uint64_t> result = coord2.elect();
    ASSERT_TRUE(result.isNone());
    result = coord2.elect();
    ASSERT_TRUE(result.isSome());
    EXPECT_EQ(10, result.get());
  }

  {
    Result<std::list<std::pair<uint64_t, std::string> > > result =
      coord2.read(1, 10);
    ASSERT_TRUE(result.isSome());
    const std::list<std::pair<uint64_t, std::string> >& list = result.get();
    EXPECT_EQ(10, list.size());
    std::list<std::pair<uint64_t, std::string> >::const_iterator iterator;
    for (iterator = list.begin(); iterator != list.end(); ++iterator) {
      EXPECT_EQ(utils::stringify(iterator->first), iterator->second);
    }
  }

  terminate(group2);
  wait(group2);

  terminate(replica2);
  wait(replica2);

  terminate(replica3);
  wait(replica3);

  utils::os::rm(file1);
  utils::os::rm(file2);
  utils::os::rm(file3);

  process::filter(NULL);
}


TEST(CoordinatorTest, Truncate)
{
  const std::string file1 = ".log_tests_truncate1";
  const std::string file2 = ".log_tests_truncate2";

  utils::os::rm(file1);
  utils::os::rm(file2);

  ReplicaProcess replica1(file1);
  spawn(replica1);

  ReplicaProcess replica2(file2);
  spawn(replica2);

  std::set<UPID> pids;
  pids.insert(replica1.self());
  pids.insert(replica2.self());

  GroupProcess group;
  spawn(group);

  dispatch(group, &GroupProcess::add, replica1.self());
  dispatch(group, &GroupProcess::add, replica2.self());

  Coordinator coord(2, &replica1, &group);

  {
    Result<uint64_t> result = coord.elect();
    ASSERT_TRUE(result.isSome());
    EXPECT_EQ(0, result.get());
  }

  for (uint64_t position = 1; position <= 10; position++) {
    Result<uint64_t> result = coord.append(utils::stringify(position));
    ASSERT_TRUE(result.isSome());
    EXPECT_EQ(position, result.get());
  }

  {
    Result<uint64_t> result = coord.truncate(7);
    ASSERT_TRUE(result.isSome());
    EXPECT_EQ(11, result.get());
  }

  {
    Result<std::list<std::pair<uint64_t, std::string> > > result =
      coord.read(6, 10);
    EXPECT_TRUE(result.isError());
    EXPECT_EQ("Attempted to read truncated position", result.error());
  }

  {
    Result<std::list<std::pair<uint64_t, std::string> > > result =
      coord.read(7, 10);
    ASSERT_TRUE(result.isSome());
    const std::list<std::pair<uint64_t, std::string> >& list = result.get();
    EXPECT_EQ(4, list.size());
    std::list<std::pair<uint64_t, std::string> >::const_iterator iterator;
    for (iterator = list.begin(); iterator != list.end(); ++iterator) {
      EXPECT_EQ(utils::stringify(iterator->first), iterator->second);
    }
  }

  terminate(group);
  wait(group);

  terminate(replica1);
  wait(replica1);

  terminate(replica2);
  wait(replica2);

  utils::os::rm(file1);
  utils::os::rm(file2);
}


TEST(CoordinatorTest, TruncateNotLearnedFill)
{
  MockFilter filter;
  process::filter(&filter);

  EXPECT_MSG(filter, _, _, _)
    .WillRepeatedly(Return(false));

  EXPECT_MSG(filter, Eq(LearnedMessage().GetTypeName()), _, _)
    .WillRepeatedly(Return(true));

  const std::string file1 = ".log_tests_truncate_not_learned1";
  const std::string file2 = ".log_tests_truncate_not_learned2";
  const std::string file3 = ".log_tests_truncate_not_learned3";

  utils::os::rm(file1);
  utils::os::rm(file2);

  ReplicaProcess replica1(file1);
  spawn(replica1);

  ReplicaProcess replica2(file2);
  spawn(replica2);

  std::set<UPID> pids;
  pids.insert(replica1.self());
  pids.insert(replica2.self());

  GroupProcess group1;
  spawn(group1);

  dispatch(group1, &GroupProcess::add, replica1.self());
  dispatch(group1, &GroupProcess::add, replica2.self());

  Coordinator coord1(2, &replica1, &group1);

  {
    Result<uint64_t> result = coord1.elect();
    ASSERT_TRUE(result.isSome());
    EXPECT_EQ(0, result.get());
  }

  for (uint64_t position = 1; position <= 10; position++) {
    Result<uint64_t> result = coord1.append(utils::stringify(position));
    ASSERT_TRUE(result.isSome());
    EXPECT_EQ(position, result.get());
  }

  {
    Result<uint64_t> result = coord1.truncate(7);
    ASSERT_TRUE(result.isSome());
    EXPECT_EQ(11, result.get());
  }

  terminate(group1);
  wait(group1);

  terminate(replica1);
  wait(replica1);

  ReplicaProcess replica3(file3);
  spawn(replica3);

  GroupProcess group2;
  spawn(group2);

  dispatch(group2, &GroupProcess::add, replica2.self());
  dispatch(group2, &GroupProcess::add, replica3.self());

  Coordinator coord2(2, &replica3, &group2);

  {
    Result<uint64_t> result = coord2.elect();
    ASSERT_TRUE(result.isNone());
    result = coord2.elect();
    ASSERT_TRUE(result.isSome());
    EXPECT_EQ(11, result.get());
  }

  {
    Result<std::list<std::pair<uint64_t, std::string> > > result =
      coord2.read(6, 10);
    EXPECT_TRUE(result.isError());
    EXPECT_EQ("Attempted to read truncated position", result.error());
  }

  {
    Result<std::list<std::pair<uint64_t, std::string> > > result =
      coord2.read(7, 11);
    ASSERT_TRUE(result.isSome());
    const std::list<std::pair<uint64_t, std::string> >& list = result.get();
    EXPECT_EQ(4, list.size());
    std::list<std::pair<uint64_t, std::string> >::const_iterator iterator;
    for (iterator = list.begin(); iterator != list.end(); ++iterator) {
      EXPECT_EQ(utils::stringify(iterator->first), iterator->second);
    }
  }

  terminate(group2);
  wait(group2);

  terminate(replica2);
  wait(replica2);

  terminate(replica3);
  wait(replica3);

  utils::os::rm(file1);
  utils::os::rm(file2);
  utils::os::rm(file3);

  process::filter(NULL);
}


TEST(CoordinatorTest, RacingElect) {}

TEST(CoordinatorTest, FillNoQuorum) {}

TEST(CoordinatorTest, FillInconsistent) {}

TEST(CoordinatorTest, LearnedOnOneReplica_NotLearnedOnAnother) {}

TEST(CoordinatorTest, LearnedOnOneReplica_NotLearnedOnAnother_AnotherFailsAndRecovers) {}
