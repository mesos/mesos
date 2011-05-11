#ifndef __HASHMAP_HPP__
#define __HASHMAP_HPP__

#include <boost/unordered_map.hpp>


namespace mesos { namespace internal {

// Provides a hash map via Boost's 'unordered_map'. For most intensive
// purposes this could be accomplished with a templated typedef, but
// those don't exist (until C++-0x). Also, doing it this way allows us
// to add functionality, or better naming of existing functionality,
// etc.

template <class Key, class T>
class hashmap : public boost::unordered_map<Key, T>
{
public:
  bool contains(const Key& key) { return count(key) > 0; }
};

}} // namespace mesos { namespace internal {

#endif // __HASHMAP_HPP__
