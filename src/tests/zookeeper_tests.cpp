#include <string>

#include <gtest/gtest.h>

#include "tests/base_zookeeper_test.hpp"

#include "zookeeper/group.hpp"


class ZooKeeperTest : public mesos::internal::test::BaseZooKeeperTest {
protected:
  void assertGet(ZooKeeper* client,
                 const std::string& path,
                 const std::string& expected) {
    std::string result;
    ASSERT_EQ(ZOK, client->get(path, false, &result, NULL));
    ASSERT_EQ(expected, result);
  }

  void assertNotSet(ZooKeeper* client,
                    const std::string& path,
                    const std::string& value) {
    ASSERT_EQ(ZNOAUTH, client->set(path, value, -1));
  }
};


static ACL _EVERYONE_READ_CREATOR_ALL_ACL[] = {
    {ZOO_PERM_READ, ZOO_ANYONE_ID_UNSAFE},
    {ZOO_PERM_ALL, ZOO_AUTH_IDS}
};


static ACL_vector EVERYONE_READ_CREATOR_ALL = {
    2, _EVERYONE_READ_CREATOR_ALL_ACL
};


TEST_F(ZooKeeperTest, Auth)
{
  mesos::internal::test::BaseZooKeeperTest::TestWatcher watcher;

  ZooKeeper authenticatedZk(zks->connectString(), NO_TIMEOUT, &watcher);
  watcher.awaitSessionEvent(ZOO_CONNECTED_STATE);
  authenticatedZk.authenticate("creator", "creator");
  authenticatedZk.create("/test",
                         "42",
                         EVERYONE_READ_CREATOR_ALL,
                         0,
                         NULL);
  assertGet(&authenticatedZk, "/test", "42");

  ZooKeeper unauthenticatedZk(zks->connectString(), NO_TIMEOUT, &watcher);
  watcher.awaitSessionEvent(ZOO_CONNECTED_STATE);
  assertGet(&unauthenticatedZk, "/test", "42");
  assertNotSet(&unauthenticatedZk, "/test", "37");

  ZooKeeper nonOwnerZk(zks->connectString(), NO_TIMEOUT, &watcher);
  watcher.awaitSessionEvent(ZOO_CONNECTED_STATE);
  nonOwnerZk.authenticate("non-owner", "non-owner");
  assertGet(&nonOwnerZk, "/test", "42");
  assertNotSet(&nonOwnerZk, "/test", "37");
}


TEST_F(ZooKeeperTest, Group)
{
  zookeeper::Group group(zks->connectString(), NO_TIMEOUT, "/test/");

  process::Future<zookeeper::Group::Membership> membership =
    group.join("hello world");

  membership.await();

  ASSERT_FALSE(membership.failed()) << membership.failure().get();
  ASSERT_FALSE(membership.discarded());
  ASSERT_TRUE(membership.ready());

  process::Future<std::set<zookeeper::Group::Membership> > memberships =
    group.watch();

  memberships.await();

  ASSERT_TRUE(memberships.ready());
  EXPECT_EQ(1, memberships.get().size());
  EXPECT_EQ(1, memberships.get().count(membership.get()));

  process::Future<std::string> info = group.info(membership.get());

  info.await();

  ASSERT_FALSE(info.failed()) << info.failure().get();
  ASSERT_FALSE(info.discarded());
  ASSERT_TRUE(info.ready());
  EXPECT_EQ("hello world", info.get());

  process::Future<bool> cancellation = group.cancel(membership.get());

  cancellation.await();

  ASSERT_FALSE(cancellation.failed()) << cancellation.failure().get();
  ASSERT_FALSE(cancellation.discarded());
  ASSERT_TRUE(cancellation.ready());
  EXPECT_TRUE(cancellation.get());

  memberships = group.watch(memberships.get());

  memberships.await();

  ASSERT_TRUE(memberships.ready());
  EXPECT_EQ(0, memberships.get().size());
}


TEST_F(ZooKeeperTest, GroupJoinWithDisconnect)
{
  zookeeper::Group group(zks->connectString(), NO_TIMEOUT, "/test/");

  zks->shutdownNetwork();

  process::Future<zookeeper::Group::Membership> membership =
    group.join("hello world");

  EXPECT_TRUE(membership.pending());

  zks->startNetwork();

  membership.await();

  ASSERT_FALSE(membership.failed()) << membership.failure().get();
  ASSERT_FALSE(membership.discarded());
  ASSERT_TRUE(membership.ready());

  process::Future<std::set<zookeeper::Group::Membership> > memberships =
    group.watch();

  memberships.await();

  ASSERT_TRUE(memberships.ready());
  EXPECT_EQ(1, memberships.get().size());
  EXPECT_EQ(1, memberships.get().count(membership.get()));
}


TEST_F(ZooKeeperTest, GroupInfoWithDisconnect)
{
  zookeeper::Group group(zks->connectString(), NO_TIMEOUT, "/test/");

  process::Future<zookeeper::Group::Membership> membership =
    group.join("hello world");

  membership.await();

  ASSERT_FALSE(membership.failed()) << membership.failure().get();
  ASSERT_FALSE(membership.discarded());
  ASSERT_TRUE(membership.ready());

  process::Future<std::set<zookeeper::Group::Membership> > memberships =
    group.watch();

  memberships.await();

  ASSERT_TRUE(memberships.ready());
  EXPECT_EQ(1, memberships.get().size());
  EXPECT_EQ(1, memberships.get().count(membership.get()));

  zks->shutdownNetwork();

  process::Future<std::string> info = group.info(membership.get());

  EXPECT_TRUE(info.pending());

  zks->startNetwork();

  info.await();

  ASSERT_FALSE(info.failed()) << info.failure().get();
  ASSERT_FALSE(info.discarded());
  ASSERT_TRUE(info.ready());
  EXPECT_EQ("hello world", info.get());
}


TEST_F(ZooKeeperTest, GroupCancelWithDisconnect)
{
  zookeeper::Group group(zks->connectString(), NO_TIMEOUT, "/test/");

  process::Future<zookeeper::Group::Membership> membership =
    group.join("hello world");

  membership.await();

  ASSERT_FALSE(membership.failed()) << membership.failure().get();
  ASSERT_FALSE(membership.discarded());
  ASSERT_TRUE(membership.ready());

  process::Future<std::set<zookeeper::Group::Membership> > memberships =
    group.watch();

  memberships.await();

  ASSERT_TRUE(memberships.ready());
  EXPECT_EQ(1, memberships.get().size());
  EXPECT_EQ(1, memberships.get().count(membership.get()));

  process::Future<std::string> info = group.info(membership.get());

  EXPECT_TRUE(info.pending());

  info.await();

  ASSERT_FALSE(info.failed()) << info.failure().get();
  ASSERT_FALSE(info.discarded());
  ASSERT_TRUE(info.ready());
  EXPECT_EQ("hello world", info.get());

  zks->shutdownNetwork();

  process::Future<bool> cancellation = group.cancel(membership.get());

  EXPECT_TRUE(cancellation.pending());

  zks->startNetwork();

  cancellation.await();

  ASSERT_FALSE(cancellation.failed()) << cancellation.failure().get();
  ASSERT_FALSE(cancellation.discarded());
  ASSERT_TRUE(cancellation.ready());
  EXPECT_TRUE(cancellation.get());

  memberships = group.watch(memberships.get());

  memberships.await();

  ASSERT_TRUE(memberships.ready());
  EXPECT_EQ(0, memberships.get().size());
}


TEST_F(ZooKeeperTest, GroupWatchWithSessionExpiration)
{
  zookeeper::Group group(zks->connectString(), NO_TIMEOUT, "/test/");

  process::Future<zookeeper::Group::Membership> membership =
    group.join("hello world");

  membership.await();

  ASSERT_FALSE(membership.failed()) << membership.failure().get();
  ASSERT_FALSE(membership.discarded());
  ASSERT_TRUE(membership.ready());

  process::Future<std::set<zookeeper::Group::Membership> > memberships =
    group.watch();

  memberships.await();

  ASSERT_TRUE(memberships.ready());
  EXPECT_EQ(1, memberships.get().size());
  EXPECT_EQ(1, memberships.get().count(membership.get()));

  process::Future<Option<int64_t> > session = group.session();

  session.await();

  ASSERT_FALSE(session.failed()) << session.failure().get();
  ASSERT_FALSE(session.discarded());
  ASSERT_TRUE(session.ready());
  ASSERT_TRUE(session.get().isSome());

  memberships = group.watch(memberships.get());

  zks->expireSession(session.get().get());

  memberships.await();

  ASSERT_TRUE(memberships.ready());
  EXPECT_EQ(0, memberships.get().size());
}
