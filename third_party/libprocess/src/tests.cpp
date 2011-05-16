#include <gmock/gmock.h>

#include <process/latch.hpp>
#include <process/process.hpp>
#include <process/run.hpp>
#include <process/timer.hpp>

using process::Latch;
using process::Future;
using process::PID;
using process::Process;
using process::Promise;
using process::Timer;
using process::UPID;

using testing::_;
using testing::ReturnArg;


class SpawnMockProcess : public Process<SpawnMockProcess>
{
public:
  MOCK_METHOD0(__operator_call__, void());
  virtual void operator () () { __operator_call__(); }
};


TEST(libprocess, spawn)
{
  ASSERT_TRUE(GTEST_IS_THREADSAFE);

  SpawnMockProcess process;

  EXPECT_CALL(process, __operator_call__())
    .Times(1);

  PID<SpawnMockProcess> pid = process::spawn(&process);

  ASSERT_FALSE(!pid);

  process::wait(pid);
}


class DispatchMockProcess : public Process<DispatchMockProcess>
{
public:
  MOCK_METHOD0(func0, void());
  MOCK_METHOD1(func1, bool(bool));
  MOCK_METHOD1(func2, Promise<bool>(bool));
  MOCK_METHOD1(func3, int(int));
  MOCK_METHOD1(func4, Promise<int>(int));
};


TEST(libprocess, dispatch)
{
  ASSERT_TRUE(GTEST_IS_THREADSAFE);

  DispatchMockProcess process;

  EXPECT_CALL(process, func0())
    .Times(1);

  EXPECT_CALL(process, func1(_))
    .WillOnce(ReturnArg<0>());

  EXPECT_CALL(process, func2(_))
    .WillOnce(ReturnArg<0>());

  PID<DispatchMockProcess> pid = process::spawn(&process);

  ASSERT_FALSE(!pid);

  process::dispatch(pid, &DispatchMockProcess::func0);

  Future<bool> future;

  future = process::dispatch(pid, &DispatchMockProcess::func1, true);

  EXPECT_TRUE(future.get());
  
  future = process::dispatch(pid, &DispatchMockProcess::func2, true);

  EXPECT_TRUE(future.get());

  process::post(pid, process::TERMINATE);
  process::wait(pid);
}


TEST(libprocess, call)
{
  ASSERT_TRUE(GTEST_IS_THREADSAFE);

  DispatchMockProcess process;

  EXPECT_CALL(process, func3(_))
    .WillOnce(ReturnArg<0>());

  EXPECT_CALL(process, func4(_))
    .WillOnce(ReturnArg<0>());

  PID<DispatchMockProcess> pid = process::spawn(&process);

  ASSERT_FALSE(!pid);

  int result;

  result = process::call(pid, &DispatchMockProcess::func3, 42);

  EXPECT_EQ(42, result);
  
  result = process::call(pid, &DispatchMockProcess::func4, 43);

  EXPECT_EQ(43, result);

  process::post(pid, process::TERMINATE);
  process::wait(pid);
}


class HandlersMockProcess : public Process<HandlersMockProcess>
{
public:
  HandlersMockProcess()
  {
    installMessageHandler("func", &HandlersMockProcess::func);
  }

  MOCK_METHOD0(func, void());
};


TEST(libprocess, handlers)
{
  ASSERT_TRUE(GTEST_IS_THREADSAFE);

  HandlersMockProcess process;

  EXPECT_CALL(process, func())
    .Times(1);

  PID<HandlersMockProcess> pid = process::spawn(&process);

  ASSERT_FALSE(!pid);

  process::post(pid, "func");

  process::post(pid, process::TERMINATE);
  process::wait(pid);
}


class BaseMockProcess : public Process<BaseMockProcess>
{
public:
  virtual void func() = 0;
  MOCK_METHOD0(foo, void());
};


class DerivedMockProcess : public BaseMockProcess
{
public:
  DerivedMockProcess() {}
  MOCK_METHOD0(func, void());
};


TEST(libprocess, inheritance)
{
  ASSERT_TRUE(GTEST_IS_THREADSAFE);

  DerivedMockProcess process;

  EXPECT_CALL(process, func())
    .Times(2);

  EXPECT_CALL(process, foo())
    .Times(1);

  PID<DerivedMockProcess> pid1 = process::spawn(&process);

  ASSERT_FALSE(!pid1);

  process::dispatch(pid1, &DerivedMockProcess::func);

  PID<BaseMockProcess> pid2(process);
  PID<BaseMockProcess> pid3 = pid1;

  ASSERT_EQ(pid2, pid3);

  process::dispatch(pid3, &BaseMockProcess::func);
  process::dispatch(pid3, &BaseMockProcess::foo);

  process::post(pid1, process::TERMINATE);
  process::wait(pid1);
}


TEST(libprocess, thunk)
{
  ASSERT_TRUE(GTEST_IS_THREADSAFE);

  struct Thunk
  {
    static int run(int i)
    {
      return i;
    }

    static int run(int i, int j)
    {
      return run(i + j);
    }
  };

  int result = process::run(&Thunk::run, 21, 21);

  EXPECT_EQ(42, result);
}


class DelegatorProcess : public Process<DelegatorProcess>
{
public:
  DelegatorProcess(const UPID& delegatee)
  {
    delegate("func", delegatee);
  }
};


class DelegateeProcess : public Process<DelegateeProcess>
{
public:
  DelegateeProcess()
  {
    installMessageHandler("func", &DelegateeProcess::func);
  }

  MOCK_METHOD0(func, void());
};


TEST(libprocess, delegate)
{
  ASSERT_TRUE(GTEST_IS_THREADSAFE);

  DelegateeProcess delegatee;
  DelegatorProcess delegator(delegatee.self());

  EXPECT_CALL(delegatee, func())
    .Times(1);

  process::spawn(&delegator);
  process::spawn(&delegatee);

  process::post(delegator.self(), "func");

  process::post(delegator.self(), process::TERMINATE);
  process::post(delegatee.self(), process::TERMINATE);

  process::wait(delegator.self());
  process::wait(delegatee.self());
}


class TerminateProcess : public Process<TerminateProcess>
{
public:
  TerminateProcess(Latch* _latch) : latch(_latch) {}

protected:
  virtual void operator () ()
  {
    latch->await();
    receive();
    EXPECT_EQ(process::TERMINATE, name());
  }

private:
  Latch* latch;
};


TEST(libprocess, terminate)
{
  ASSERT_TRUE(GTEST_IS_THREADSAFE);

  Latch latch;

  TerminateProcess process(&latch);

  process::spawn(&process);

  process::post(process.self(), "one");
  process::post(process.self(), "two");
  process::post(process.self(), "three");

  process::terminate(process.self());

  latch.trigger();
  
  process::wait(process.self());
}


class TimeoutProcess : public Process<TimeoutProcess>
{
public:
  TimeoutProcess() {}
  MOCK_METHOD0(timeout, void());
};


TEST(libprocess, DISABLED_timer)
{
  ASSERT_TRUE(GTEST_IS_THREADSAFE);

  process::Clock::pause();

  TimeoutProcess process;

  EXPECT_CALL(process, timeout())
    .Times(1);

  process::spawn(&process);

  double timeout = 5.0;

  Timer timer =
    process::delay(timeout, process.self(), &TimeoutProcess::timeout);

  process::Clock::advance(timeout);

  process::post(process.self(), process::TERMINATE);
  process::wait(process.self());

  process::Clock::resume();
}


int main(int argc, char** argv)
{
  // Initialize Google Mock/Test.
  testing::InitGoogleMock(&argc, argv);

  return RUN_ALL_TESTS();
}
