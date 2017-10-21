// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License

#include <gtest/gtest.h>

#include <stout/lambda.hpp>
#include <stout/numify.hpp>


struct OnlyMoveable
{
  OnlyMoveable(int i) : i(i) {}
  OnlyMoveable(OnlyMoveable&&) = default;
  OnlyMoveable(const OnlyMoveable&) = delete;
  OnlyMoveable& operator=(OnlyMoveable&&) = default;
  OnlyMoveable& operator=(const OnlyMoveable&) = delete;

  int i;
  int j = 0;
};


std::vector<std::string> function()
{
  return {"1", "2", "3"};
}


TEST(LambdaTest, Map)
{
  std::vector<int> expected = {1, 2, 3};

  EXPECT_EQ(
      expected,
      lambda::map(
          [](std::string s) {
            return numify<int>(s).get();
          },
          std::vector<std::string>{"1", "2", "3"}));

  EXPECT_EQ(
      expected,
      lambda::map(
          [](const std::string& s) {
            return numify<int>(s).get();
          },
          std::vector<std::string>{"1", "2", "3"}));

  EXPECT_EQ(
      expected,
      lambda::map(
          [](std::string&& s) {
            return numify<int>(s).get();
          },
          std::vector<std::string>{"1", "2", "3"}));

  std::vector<std::string> concat = {"11", "22", "33"};

  EXPECT_EQ(
      concat,
      lambda::map(
          [](std::string&& s) {
            return s + s;
          },
          function()));

  std::vector<OnlyMoveable> v;
  v.emplace_back(1);
  v.emplace_back(2);

  std::vector<OnlyMoveable> result = lambda::map(
      [](OnlyMoveable&& o) {
        o.j = o.i;
        return std::move(o);
      },
      std::move(v));

  for (const OnlyMoveable& o : result) {
    EXPECT_EQ(o.i, o.j);
  }
}


TEST(LambdaTest, Zip)
{
  std::vector<int> ints = {1, 2, 3, 4, 5, 6, 7, 8};
  std::list<std::string> strings = {"hello", "world"};

  hashmap<int, std::string> zip1 = lambda::zip(ints, strings);

  ASSERT_EQ(2, zip1.size());
  EXPECT_EQ(std::string("hello"), zip1[1]);
  EXPECT_EQ(std::string("world"), zip1[2]);

  ints = {1, 2};
  strings = {"hello", "world", "!"};

  std::vector<std::pair<int, std::string>> zip2 =
    lambda::zipto<std::vector>(ints, strings);

  ASSERT_EQ(2, zip2.size());
  EXPECT_EQ(std::make_pair(1, std::string("hello")), zip2.at(0));
  EXPECT_EQ(std::make_pair(2, std::string("world")), zip2.at(1));
}
