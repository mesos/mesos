#ifndef __NEXUS_TYPES_HPP__
#define __NEXUS_TYPES_HPP__

#include <iostream>
#include <string>

#include "nexus.h"

// TODO(benh): Eliminate this dependency!
#include <boost/unordered_map.hpp>

namespace nexus {


template <typename T>
class ID
{
public:
  ID(const char *_s)
    : s(_s) {}

  ID(const std::string& _s)
    : s(_s) {}

  bool operator == (const ID<T> &that) const
  {
    return s == that.s;
  }

  bool operator != (const ID<T> &that) const
  {
    return s != that.s;
  }

  bool operator < (const ID<T> &that) const
  {
    return s < that.s;
  }

  operator std::string () const
  {
    return s;
  }

  // TODO(benh): Eliminate this backwards compatibility dependency.
  const char * c_str() const
  {
    return s.c_str();
  }

  std::string s;
};


template <typename T>
std::ostream& operator << (std::ostream& out, const ID<T>& id)
{
  out << id.s;
}


template <typename T>
std::istream& operator >> (std::istream& in, ID<T>& id)
{
  in >> id.s;
}


template <typename T>
std::size_t hash_value(const ID<T>& id)
{
  // TODO(benh): Removed boost dependency here.
  return boost::hash_value(id.s);
}


class FrameworkID : public ID<FrameworkID>
{
public:
  FrameworkID(const char *s = "") : ID<FrameworkID>(s) {}
  FrameworkID(const std::string& s) : ID<FrameworkID>(s) {}
};


class SlaveID : public ID<SlaveID>
{
public:
  SlaveID(const char *s = "") : ID<SlaveID>(s) {}
  SlaveID(const std::string& s) : ID<SlaveID>(s) {}
};


class OfferID : public ID<OfferID>
{
public:
  OfferID(const char *s = "") : ID<OfferID>(s) {}
  OfferID(const std::string& s) : ID<OfferID>(s) {}
};


typedef task_id TaskID;
typedef task_state TaskState;


} /* namespace nexus { */

#endif /* __NEXUS_TYPES_HPP__ */
