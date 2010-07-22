#ifndef __NEXUS_TYPES_HPP__
#define __NEXUS_TYPES_HPP__

#include <iostream>
#include <string>

#include "nexus.h"

namespace nexus {

class FrameworkID
{
public:
  FrameworkID(const char *s = "") { this->s = s; }
  FrameworkID(const std::string& s) { this->s = s; }

  bool operator == (const FrameworkID& that) const
  {
    return s == that.s;
  }

  bool operator != (const FrameworkID& that) const
  {
    return s != that.s;
  }

  bool operator < (const FrameworkID& that) const
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


class SlaveID
{
public:
  SlaveID(const char *s = "") { this->s = s; }
  SlaveID(const std::string& s) { this->s = s; }

  bool operator == (const SlaveID& that) const
  {
    return s == that.s;
  }

  bool operator != (const SlaveID& that) const
  {
    return s != that.s;
  }

  bool operator < (const SlaveID& that) const
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


class OfferID
{
public:
  OfferID(const char *s = "") { this->s = s; }
  OfferID(const std::string& s) { this->s = s; }

  bool operator == (const OfferID& that) const
  {
    return s == that.s;
  }

  bool operator != (const OfferID& that) const
  {
    return s != that.s;
  }

  bool operator < (const OfferID& that) const
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


typedef task_id TaskID;
typedef task_state TaskState;


std::ostream& operator << (std::ostream& out, const FrameworkID& id);
std::istream& operator >> (std::istream& in, FrameworkID& id);


std::ostream& operator << (std::ostream& out, const SlaveID& id);
std::istream& operator >> (std::istream& in, SlaveID& id);


std::ostream& operator << (std::ostream& out, const OfferID& id);
std::istream& operator >> (std::istream& in, OfferID& id);


std::size_t hash_value(const FrameworkID& id);
std::size_t hash_value(const SlaveID& id);
std::size_t hash_value(const OfferID& id);

} /* namespace nexus { */

#endif /* __NEXUS_TYPES_HPP__ */
