#ifndef __MULTIHASHMAP_HPP__
#define __MULTIHASHMAP_HPP__

#include <iterator>

#include "common/hashmap.hpp"
#include "common/hashset.hpp"


// Forward declarations of multihashmap iterators.
template <typename K, typename V>
struct multihashmap_iterator;

template <typename K, typename V>
struct const_multihashmap_iterator;


// Implementation of a multimap using hashmap and hashset. The
// rationale for creating this is that the std::multimap
// implementation is painful to use (requires lots of iterator
// garbage, as well as the use of 'equal_range' which makes for
// cluttered code). Note that this implementation doesn't actually
// provide certain STL operations such as 'find' or 'insert' which may
// make it hard(er) to use in some STL data structures (this is
// intentional, it is meant to provide functions with "higher-level"
// semantics such as 'contains'). In the future we might consider
// extending boost::unordered_multimap to recover the STL interface
// (as is done with hashmap and hashset) .
template <typename K, typename V>
class multihashmap
{
public:
  void put(const K& key, const V& value);
  const hashset<V>& get(const K& key) const;
  void clear();
  bool empty() const;
  size_t size() const;
  bool remove(const K& key);
  bool remove(const K& key, const V& value);
  bool contains(const K& key) const;
  bool contains(const K& key, const V& value) const;

  typedef multihashmap_iterator<K, V> iterator;
  typedef const_multihashmap_iterator<K, V> const_iterator;

  iterator begin();
  iterator end();

  const_iterator begin() const;
  const_iterator end() const;

private:
  friend class multihashmap_iterator<K, V>;
  friend class const_multihashmap_iterator<K, V>;

  hashset<V> EMPTY; // Used when no key is present.

  typedef hashmap<K, hashset<V> > type;
  type map;
};


template <typename K, typename V>
struct multihashmap_iterator
  : std::iterator_traits<typename hashmap<K, V>::iterator>
{
  typedef std::forward_iterator_tag iterator_category;

  multihashmap_iterator()
    : current(NULL),
      outer_iterator(outer_type().end()),
      outer_end(outer_type().end()),
      inner_iterator(inner_type().end()),
      inner_end(inner_type().end()) {}

  explicit multihashmap_iterator(multihashmap<K, V>& map)
    : current(NULL),
      outer_iterator(map.map.begin()),
      outer_end(map.map.end()),
      inner_iterator(inner_type().end()),
      inner_end(inner_type().end())
  {
    update();
  }

  multihashmap_iterator(const multihashmap_iterator<K, V>& that)
    : current(NULL),
      outer_iterator(that.outer_iterator),
      outer_end(that.outer_end),
      inner_iterator(that.inner_iterator),
      inner_end(that.inner_end)
  {
    if (that.current != NULL) {
      current =
        new std::pair<const K, V>(that.current->first, that.current->second);
    }
  }

  virtual ~multihashmap_iterator()
  {
    if (current != NULL) {
      delete current;
    }
  }

  multihashmap_iterator& operator ++ ()
  {
    ++inner_iterator;
    if (inner_iterator == inner_end) {
      ++outer_iterator;
      update();
    } else {
      if (current != NULL) {
        delete current;
        current = NULL;
      }
      current =
        new std::pair<const K, V>(outer_iterator->first, *inner_iterator);
    }

    return *this;
  }

  std::pair<const K, V>& operator * ()
  {
    return *current;
  }

  std::pair<const K, V>* operator -> ()
  {
    return current;
  }

  bool operator == (const multihashmap_iterator& that) const
  {
    bool this_end = outer_iterator == outer_end;
    bool that_end = that.outer_iterator == that.outer_end;
    if (this_end && that_end) {
      return true;
    } else if (this_end != that_end) {
      return false;
    }

    return outer_iterator == that.outer_iterator
      && inner_iterator == that.inner_iterator;
  }

  bool operator != (const multihashmap_iterator& that) const
  {
    return !((*this) == (that));
  }

private:
  typedef hashmap<K, hashset<V> > outer_type;
  typedef hashset<V> inner_type;

  multihashmap_iterator& operator = (const multihashmap_iterator<K, V>&);

  void update()
  {
    while (outer_iterator != outer_end) {
      inner_iterator = outer_iterator->second.begin();
      inner_end = outer_iterator->second.end();
      if (inner_iterator == inner_end) {
        ++outer_iterator;
      } else {
        if (current != NULL) {
          delete current;
          current = NULL;
        }
        current =
          new std::pair<const K, V>(outer_iterator->first, *inner_iterator);
        break;
      }
    }
  }

  typename outer_type::iterator outer_iterator, outer_end;
  typename inner_type::iterator inner_iterator, inner_end;
  std::pair<const K, V>* current;
};


template <typename K, typename V>
struct const_multihashmap_iterator
  : std::iterator_traits<typename hashmap<K, V>::const_iterator>
{
  typedef std::forward_iterator_tag iterator_category;

  const_multihashmap_iterator()
    : current(NULL),
      outer_iterator(outer_type().end()),
      outer_end(outer_type().end()),
      inner_iterator(inner_type().end()),
      inner_end(inner_type().end()) {}

  explicit const_multihashmap_iterator(const multihashmap<K, V>& map)
  : current(NULL),
      outer_iterator(map.map.begin()),
      outer_end(map.map.end()),
      inner_iterator(inner_type().end()),
      inner_end(inner_type().end())
  {
    update();
  }

  const_multihashmap_iterator(const const_multihashmap_iterator<K, V>& that)
    : current(NULL),
      outer_iterator(that.outer_iterator),
      outer_end(that.outer_end),
      inner_iterator(that.inner_iterator),
      inner_end(that.inner_end)
  {
    if (that.current != NULL) {
      current =
        new std::pair<const K, V>(that.current->first, that.current->second);
    }
  }

  virtual ~const_multihashmap_iterator()
  {
    if (current != NULL) {
      delete current;
    }
  }

  const_multihashmap_iterator& operator ++ ()
  {
    ++inner_iterator;
    if (inner_iterator == inner_end) {
      ++outer_iterator;
      update();
    } else {
      if (current != NULL) {
        delete current;
        current = NULL;
      }
      current =
        new std::pair<const K, V>(outer_iterator->first, *inner_iterator);
    }

    return *this;
  }

  const std::pair<const K, V>& operator * () const
  {
    return *current;
  }

  const std::pair<const K, V>* operator -> () const
  {
    return current;
  }

  bool operator == (const const_multihashmap_iterator& that) const
  {
    bool this_end = outer_iterator == outer_end;
    bool that_end = that.outer_iterator == that.outer_end;
    if (this_end && that_end) {
      return true;
    } else if (this_end != that_end) {
      return false;
    }

    return outer_iterator == that.outer_iterator
      && inner_iterator == that.inner_iterator;
  }

  bool operator != (const const_multihashmap_iterator& that) const
  {
    return !((*this) == (that));
  }

private:
  typedef hashmap<K, hashset<V> > outer_type;
  typedef hashset<V> inner_type;

  // Not assignable.
  const_multihashmap_iterator& operator = (
      const const_multihashmap_iterator<K, V>&);

  void update()
  {
    while (outer_iterator != outer_end) {
      inner_iterator = outer_iterator->second.begin();
      inner_end = outer_iterator->second.end();
      if (inner_iterator == inner_end) {
        ++outer_iterator;
      } else {
        if (current != NULL) {
          delete current;
          current = NULL;
        }
        current =
          new std::pair<const K, V>(outer_iterator->first, *inner_iterator);
        break;
      }
    }
  }

  typename outer_type::const_iterator outer_iterator, outer_end;
  typename inner_type::const_iterator inner_iterator, inner_end;
  std::pair<const K, V>* current;
};


template <typename K, typename V>
void multihashmap<K, V>::put(const K& key, const V& value)
{
  map[key].insert(value);
}


template <typename K, typename V>
const hashset<V>& multihashmap<K, V>::get(const K& key) const
{
  typename type::const_iterator i = map.find(key);
  if (i != map.end()) {
    return (*i).second;
  }
  return EMPTY;
}


template <typename K, typename V>
void multihashmap<K, V>::clear()
{
  map.clear();
}


template <typename K, typename V>
bool multihashmap<K, V>::empty() const
{
  return map.empty();
}


template <typename K, typename V>
size_t multihashmap<K, V>::size() const
{
  return map.size();
}


template <typename K, typename V>
bool multihashmap<K, V>::remove(const K& key)
{
  size_t result = map.count(key);
  map.erase(key);
  return result > 0;
}


template <typename K, typename V>
bool multihashmap<K, V>::remove(const K& key, const V& value)
{
  size_t result = map.count(key);
  if (result > 0) {
    map[key].erase(value);
    if (map[key].size() == 0) {
      map.erase(key);
    }
  }
  return result > 0;
}


template <typename K, typename V>
bool multihashmap<K, V>::contains(const K& key) const
{
  typename type::const_iterator i = map.find(key);
  if (i != map.end()) {
    return i->second.size() > 0;
  }
  return false;
}


template <typename K, typename V>
bool multihashmap<K, V>::contains(const K& key, const V& value) const
{
  typename type::const_iterator i = map.find(key);
  if (i != map.end()) {
    return i->second.count(value) > 0;
  }
  return false;
}


template <typename K, typename V>
typename multihashmap<K, V>::iterator multihashmap<K, V>::begin()
{
  return multihashmap_iterator<K, V>(*this);
}


template <typename K, typename V>
typename multihashmap<K, V>::iterator multihashmap<K, V>::end()
{
  return multihashmap_iterator<K, V>();
}


template <typename K, typename V>
typename multihashmap<K, V>::const_iterator multihashmap<K, V>::begin() const
{
  return const_multihashmap_iterator<K, V>(*this);
}


template <typename K, typename V>
typename multihashmap<K, V>::const_iterator multihashmap<K, V>::end() const
{
  return const_multihashmap_iterator<K, V>();
}

#endif // __MULTIHASHMAP_HPP__
