#ifndef __LOG_HPP__
#define __LOG_HPP__

#include <list>
#include <set>
#include <string>

#include <process/process.hpp>

#include "common/foreach.hpp"
#include "common/result.hpp"
#include "common/try.hpp"

#include "log/coordinator.hpp"
#include "log/replica.hpp"

namespace mesos {
namespace internal {
namespace log {

class Log
{
public:
  // Forward declarations.
  class Reader;
  class Writer;

  class Position
  {
  public:
    bool operator == (const Position& that) const
    {
      return value == that.value;
    }

    bool operator < (const Position& that) const
    {
      return value < that.value;
    }

    bool operator <= (const Position& that) const
    {
      return value <= that.value;
    }

    bool operator > (const Position& that) const
    {
      return value > that.value;
    }

    bool operator >= (const Position& that) const
    {
      return value >= that.value;
    }

    // Returns an "identity" off this position, useful for serializing
    // to logs or across communication mediums.
    std::string identity() const
    {
      char bytes[8];
      bytes[0] =(0xff & (value >> 56));
      bytes[1] = (0xff & (value >> 48));
      bytes[2] = (0xff & (value >> 40));
      bytes[3] = (0xff & (value >> 32));
      bytes[4] = (0xff & (value >> 24));
      bytes[5] = (0xff & (value >> 16));
      bytes[6] = (0xff & (value >> 8));
      bytes[7] = (0xff & value);
      return std::string(bytes, sizeof(bytes));
    }

  private:
    friend class Log;
    friend class Reader;
    friend class Writer;
    Position(uint64_t _value) : value(_value) {}
    uint64_t value;
  };

  class Entry
  {
  public:
    Position position;
    std::string data;

  private:
    friend class Reader;
    friend class Writer;
    Entry(const Position& _position, const std::string& _data)
      : position(_position), data(_data) {}
  };

  class Reader
  {
  public:
    Reader(Log* log);
    ~Reader();

    // Returns all entries between the specified positions, unless
    // those positions are invalid, in which case returns an error.
    Result<std::list<Entry> > read(const Position& from, const Position& to);

    // Returns the beginning position of the log from the perspective
    // of the local replica (which may be out of date if the log has
    // been opened and truncated while this replica was partitioned).
    Position beginning();

    // Returns the ending (i.e., last) position of the log from the
    // perspective of the local replica (which may be out of date if
    // the log has been opened and appended to while this replica was
    // partitioned).
    Position ending();

  private:
    Replica* replica;
  };

  class Writer
  {
  public:
    // Creates a new writer associated with the specified log. Only
    // one writer (local and remote) is valid at a time. A writer
    // becomes invalid if any operation returns an error, and a new
    // writer must be created in order perform subsequent operations.
    Writer(Log* log, int retries = 0);
    ~Writer();

    // Attempts to append the specified data to the log. A none result
    // means the operation timed out, otherwise the new ending
    // position of the log is returned or an error. Upon error a new
    // Writer must be created.
    Result<Position> append(const std::string& data);

    // Attempts to truncate the log up to but not including the
    // specificed position. A none result means the operation timed
    // out, otherwise the new ending position of the log is returned
    // or an error. Upon error a new Writer must be created.
    Result<Position> truncate(const Position& to);

  private:
    Option<std::string> error;
    Coordinator* coordinator;
  };

  // Creates a new replicated log that assumes the specified quorum
  // size, is backed by a file at the specified path, and coordiantes
  // with other replicas via the set of process PIDs.
  Log(int quorum,
      const std::string& path,
      const std::set<process::UPID>& pids)
  {
    replica = new Replica(path);
    network = new Network(pids);
    network->add(replica->pid()); // Don't forget to add our own replica!
    coordinator = new Coordinator(quorum, replica, network);
  }

#ifdef WITH_ZOOKEEPER
  // Creates a new replicated log that assumes the specified quorum
  // size, is backed by a file at the specified path, and coordiantes
  // with other replicas associated with the specified ZooKeeper
  // servers, timeout, and znode.
  Log(int quorum,
      const std::string& path,
      const std::string& servers,
      const seconds& timeout,
      const std::string& znode)
  {
    replica = new Replica(path);
    network = new ZooKeeperNetwork(servers, timeout, znode);
    coordinator = new Coordinator(quorum, replica, network);
  }
#endif // WITH_ZOOKEEPER

  ~Log()
  {
    delete coordinator;
    delete network;
    delete replica;
  }

  // Returns a position based off of the bytes recovered from
  // Position.identity().
  Position position(const std::string& identity) const
  {
    const char* bytes = identity.c_str();
    uint64_t value =
      ((uint64_t) (bytes[0] & 0xff) << 56) |
      ((uint64_t) (bytes[1] & 0xff) << 48) |
      ((uint64_t) (bytes[2] & 0xff) << 40) |
      ((uint64_t) (bytes[3] & 0xff) << 32) |
      ((uint64_t) (bytes[4] & 0xff) << 24) |
      ((uint64_t) (bytes[5] & 0xff) << 16) |
      ((uint64_t) (bytes[6] & 0xff) << 8) |
      ((uint64_t) (bytes[7] & 0xff));
    return Position(value);
  }

private:
  friend class Reader;
  friend class Writer;

  Replica* replica;
  Network* network;
  Coordinator* coordinator;
};


Log::Reader::Reader(Log* log)
  : replica(log->replica) {}


Log::Reader::~Reader() {}


Result<std::list<Log::Entry> > Log::Reader::read(
    const Log::Position& from,
    const Log::Position& to)
{
  process::Future<std::list<Action> > actions =
    replica->read(from.value, to.value);

  // TODO(benh): Take a timeout!
  actions.await();

  if (actions.isFailed()) {
    return Result<std::list<Log::Entry> >::error(actions.failure());
  }

  CHECK(actions.isReady()) << "Not expecting discarded future!"; 

  std::list<Log::Entry> entries;

  uint64_t position = from.value;

  foreach (const Action& action, actions.get()) {
    // Ensure read range is valid.
    if (!action.has_performed() ||
        !action.has_learned() ||
        !action.learned()) {
      return Result<std::list<Log::Entry> >::error(
          "Bad read range (includes pending entries)");
    } else if (position++ != action.position()) {
      return Result<std::list<Log::Entry> >::error(
          "Bad read range (includes missing entries)");
    }

    // And only return appends.
    CHECK(action.has_type());
    if (action.type() == Action::APPEND) {
      entries.push_back(Entry(action.position(), action.append().bytes()));
    }
  }

  return entries;
}


Log::Position Log::Reader::beginning()
{
  // TODO(benh): Take a timeout and return an Option.
  process::Future<uint64_t> value = replica->beginning();
  value.await();
  CHECK(value.isReady()) << "Not expecting a failed or discarded future!";
  return Log::Position(value.get());
}


Log::Position Log::Reader::ending()
{
  // TODO(benh): Take a timeout and return an Option.
  process::Future<uint64_t> value = replica->ending();
  value.await();
  CHECK(value.isReady()) << "Not expecting a failed or discarded future!";
  return Log::Position(value.get());
}


Log::Writer::Writer(Log* log, int retries)
  : coordinator(log->coordinator),
    error(Option<std::string>::none())
{
  do {
    Result<uint64_t> result = coordinator->elect();
    if (result.isNone()) {
      retries--;
      continue;
    } else if (result.isSome()) {
      return;
    } else {
      error = result.error();
      return;
    }
  } while (retries > 0);
}


Log::Writer::~Writer()
{
  coordinator->demote();
}


Result<Log::Position> Log::Writer::append(const std::string& data)
{
  if (error.isSome()) {
    return Result<Log::Position>::error(error.get());
  }

  Result<uint64_t> result = coordinator->append(data);

  if (result.isError()) {
    error = result.error();
    return Result<Log::Position>::error(error.get());
  } else if (result.isNone()) {
    return Result<Log::Position>::none();
  }

  CHECK(result.isSome());

  return Log::Position(result.get());
}


Result<Log::Position> Log::Writer::truncate(const Log::Position& to)
{
  if (error.isSome()) {
    return Result<Log::Position>::error(error.get());
  }

  Result<uint64_t> result = coordinator->truncate(to.value);

  if (result.isError()) {
    error = result.error();
    return Result<Log::Position>::error(error.get());
  } else if (result.isNone()) {
    return Result<Log::Position>::none();
  }

  CHECK(result.isSome());

  return Log::Position(result.get());
}

} // namespace log {
} // namespace internal {
} // namespace mesos {

#endif // __LOG_HPP__
