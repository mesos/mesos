#include <string>

#include <gtest/gtest.h>

#include <zookeeper.h>

#include "common/zookeeper.hpp"

#include "tests/base_zookeeper_test.hpp"

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
