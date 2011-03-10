#include <gtest/gtest.h>

#include <string>
#include <vector>

#include <common/date_utils.hpp>

using namespace mesos::internal;


TEST(DateUtilsTest, humanReadable)
{
  DateUtils::setMockDate("200102030405");
  ASSERT_EQ("200102030405", DateUtils::humanReadableDate());
}

TEST(DateUtilsTest, currentDateInMicro)
{
  DateUtils::setMockDate("200102030405");
// This number is too big for long representation in 32bit environments.
#if __LP64__ == 1  
  ASSERT_EQ(981201900000000, DateUtils::currentDateInMicro());
#endif  
}
