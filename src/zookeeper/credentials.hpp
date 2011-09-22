#ifndef __ZOOKEEPER_CREDENTIALS_HPP__
#define __ZOOKEEPER_CREDENTIALS_HPP__

#include <zookeeper.h>

#include <string>

namespace zookeeper {

struct Credentials
{
  std::string username;
  std::string password;
};


// An ACL that ensures we're the only authenticated user to mutate our
// nodes - others are welcome to read.
extern const ACL_vector EVERYONE_READ_CREATOR_ALL;

} // namespace zookeeper {

#endif // __ZOOKEEPER_CREDENTIALS_HPP__
