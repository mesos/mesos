#ifndef __PROCESS_FUTURE_HPP__
#define __PROCESS_FUTURE_HPP__

#include <queue>
#include <set>

#include <tr1/functional>

#include <process/latch.hpp>
#include <process/option.hpp>


namespace process {

// Forward declaration of Promise.
template <typename T>
class Promise;


// Definition of a "shared" future. A future can hold any
// copy-constructible value. A future is considered "shared" because
// by default a future can be accessed concurrently.
template <typename T>
class Future
{
public:
  Future();
  Future(const T& _t);
  Future(const Future<T>& that);
  ~Future();

  // Futures are assignable. This results in the reference to the
  // previous future data being decremented and a reference to 'that'
  // being incremented.
  Future<T>& operator = (const Future<T>& that);

  // Comparision operators useful for using futures in collections.
  bool operator == (const Future<T>& that) const;
  bool operator < (const Future<T>& that) const;

  // Helpers to get the current state of this future.
  bool pending() const;
  bool ready() const;
  bool discarded() const;

  // Discards this future. This is similar to cancelling a future,
  // however it also occurs when the last reference to this future
  // gets cleaned up. Returns false if the future could not be
  // discarded (for example, because it is ready).
  bool discard();

  // Waits for this future to either become ready or discarded.
  bool await(double secs = 0) const;

  // Return the value associated with this future, waits indefinitely
  // until a value gets associated or until the future is discarded.
  T get() const;

  // Type of the callback function that can get invoked when the
  // future gets set or discarded.
  typedef std::tr1::function<void(const Future<T>&)> Callback;

  // Installs callbacks for the specified events.
  void onReady(const Callback& callback) const;
  void onDiscarded(const Callback& callback) const;

// private:
  friend class Promise<T>;

  bool set(const T& _t);
  void copy(const Future<T>& that);
  void cleanup();

  enum State {
    PENDING,
    READY,
    DISCARDED,
  };

  int* refs;
  int* lock;
  State* state;
  T** t;
  std::queue<Callback>* ready_callbacks;
  std::queue<Callback>* discarded_callbacks;
  Latch* latch;
};


// Internal helper utilities.
namespace internal {

inline void acquire(int* lock)
{
  while (!__sync_bool_compare_and_swap(lock, 0, 1)) {
    asm volatile ("pause");
  }
}

inline void release(int* lock)
{
  // Unlock via a compare-and-swap so we get a memory barrier too.
  bool unlocked = __sync_bool_compare_and_swap(lock, 1, 0);
  assert(unlocked);
}

namespace callbacks {

template <typename T>
void select(const Future<T>& future, Promise<Future<T> > promise)
{
  assert(future.ready());
  // Don't set the promise if it's already ready or discarded.
  if (!promise.future().ready() && !promise.future().discarded()) {
    promise.set(future);
  }
}

} // namespace callback {
} // namespace internal {


// Returns an option of a ready future or none in the event of
// timeout. Note that select DOES NOT return for a future that has
// been discarded.
template <typename T>
Option<Future<T> > select(const std::set<Future<T> >& futures, double secs)
{
  Promise<Future<T> > promise;

  std::tr1::function<void(const Future<T>&)> callback =
    std::tr1::bind(internal::callbacks::select<T>,
                   std::tr1::placeholders::_1, promise);

  typename std::set<Future<T> >::iterator iterator;
  for (iterator = futures.begin(); iterator != futures.end(); ++iterator) {
    const Future<T>& future = *iterator;
    future.onReady(callback);
  }

  Future<Future<T> > future = promise.future();

  if (future.await(secs)) {
    return Option<Future<T> >::some(future.get());
  } else {
    future.discard();
    return Option<Future<T> >::none();
  }
}


template <typename T>
void discard(const std::set<Future<T> >& futures)
{
  typename std::set<Future<T> >::const_iterator iterator;
  for (iterator = futures.begin(); iterator != futures.end(); ++iterator) {
    Future<T> future = *iterator;
    future.discard();
  }
}


template <typename T>
Future<T>::Future()
  : refs(new int),
    lock(new int),
    state(new State),
    t(new T*),
    ready_callbacks(new std::queue<Callback>),
    discarded_callbacks(new std::queue<Callback>),
    latch(new Latch)
{
  *refs = 1;
  *lock = 0;
  *state = PENDING;
  *t = NULL;
}


template <typename T>
Future<T>::Future(const T& _t)
  : refs(new int),
    lock(new int),
    state(new State),
    t(new T*),
    ready_callbacks(new std::queue<Callback>),
    discarded_callbacks(new std::queue<Callback>),
    latch(new Latch)
{
  *refs = 1;
  *lock = 0;
  *state = PENDING;
  *t = NULL;
  set(_t);
}


template <typename T>
Future<T>::Future(const Future<T>& that)
{
  copy(that);
}


template <typename T>
Future<T>::~Future()
{
  cleanup();
}


template <typename T>
Future<T>& Future<T>::operator = (const Future<T>& that)
{
  if (this != &that) {
    cleanup();
    copy(that);
  }
  return *this;
}


template <typename T>
bool Future<T>::operator == (const Future<T>& that) const
{
  assert(latch != NULL);
  assert(that.latch != NULL);
  return latch == that.latch;
}


template <typename T>
bool Future<T>::operator < (const Future<T>& that) const
{
  assert(latch != NULL);
  assert(that.latch != NULL);
  return latch < that.latch;
}


template <typename T>
bool Future<T>::discard()
{
  bool result = false;

  assert(lock != NULL);
  internal::acquire(lock);
  {
    assert(state != NULL);
    if (*state == PENDING) {
      latch->trigger();
      *state = DISCARDED;
      result = true;
    }
  }
  internal::release(lock);

  // Invoke all callbacks associated with this future being
  // DISCARDED. We don't need a lock because the state is now in
  // DISCARDED so there should not be any concurrent modications.
  if (result) {
    while (!discarded_callbacks->empty()) {
      const Callback& callback = discarded_callbacks->front();
      // TODO(*): Invoke callbacks in another execution context.
      callback(*this);
      discarded_callbacks->pop();
    }
  }

  return result;
}


template <typename T>
bool Future<T>::pending() const
{
  assert(state != NULL);
  return *state == PENDING;
}


template <typename T>
bool Future<T>::ready() const
{
  assert(state != NULL);
  return *state == READY;
}


template <typename T>
bool Future<T>::discarded() const
{
  assert(state != NULL);
  return *state == DISCARDED;
}


template <typename T>
bool Future<T>::await(double secs) const
{
  if (!ready()) {
    assert(latch != NULL);
    return latch->await(secs);
  }
  return true;
}


template <typename T>
T Future<T>::get() const
{
  if (ready())
    return **t;
  assert(latch != NULL);
  await();
  assert(t != NULL);
  assert(*t != NULL);
  return **t;
}


template <typename T>
void Future<T>::onReady(const Callback& callback) const
{
  bool run = false;

  assert(lock != NULL);
  internal::acquire(lock);
  {
    assert(state != NULL);
    if (*state == READY) {
      run = true;
    } else {
      ready_callbacks->push(callback);
    }
  }
  internal::release(lock);

  // TODO(*): Invoke callback in another execution context.
  if (run) {
    callback(*this);
  }
}


template <typename T>
void Future<T>::onDiscarded(const Callback& callback) const
{
  bool run = false;

  assert(lock != NULL);
  internal::acquire(lock);
  {
    assert(state != NULL);
    if (*state == DISCARDED) {
      run = true;
    } else {
      discarded_callbacks->push(callback);
    }
  }
  internal::release(lock);

  // TODO(*): Invoke callback in another execution context.
  if (run) {
    callback(*this);
  }
}

template <typename T>
bool Future<T>::set(const T& _t)
{
  bool result = false;

  // TODO(benh): For now, you can't set a future more than once, and
  // we trigger a rather harse assertion violation when that occurs.

  assert(lock != NULL);
  internal::acquire(lock);
  {
    assert(state != NULL);
    assert(*state != READY);
    if (*state == PENDING) {
      *t = new T(_t);
      latch->trigger();
      *state = READY;
      result = true;
    } else {
      assert(*state == DISCARDED);
    }
  }
  internal::release(lock);

  // Invoke all callbacks associated with this future being READY. We
  // don't need a lock because the state is now in READY so there
  // should not be any concurrent modications.
  if (result) {
    while (!ready_callbacks->empty()) {
      const Callback& callback = ready_callbacks->front();
      // TODO(*): Invoke callbacks in another execution context.
      callback(*this);
      ready_callbacks->pop();
    }
  }

  return result;
}


template <typename T>
void Future<T>::copy(const Future<T>& that)
{
  assert(that.refs > 0);
  __sync_fetch_and_add(that.refs, 1);
  refs = that.refs;
  lock = that.lock;
  state = that.state;
  t = that.t;
  ready_callbacks = that.ready_callbacks;
  discarded_callbacks = that.discarded_callbacks;
  latch = that.latch;
}


template <typename T>
void Future<T>::cleanup()
{
  assert(refs != NULL);
  if (__sync_sub_and_fetch(refs, 1) == 0) {
    // Discard the future if it is still pending (so we invoke any
    // discarded callbacks that have been setup). Note that we put the
    // reference count back at 1 here in case one of the callbacks
    // decides it wants to keep a reference.
    assert(state != NULL);
    if (*state == PENDING) {
      *refs = 1;
      discard();
    }

    // Now try and cleanup again (this time we know the future has
    // either been discarded or was not pending). Note that one of the
    // callbacks might have stored the future, in which case we'll
    // just return without doing anything, but the state will forever
    // be "discarded".
    assert(refs != NULL);
    if (__sync_sub_and_fetch(refs, 1) == 0) {
      delete refs;
      refs = NULL;
      assert(lock != NULL);
      delete lock;
      lock = NULL;
      assert(state != NULL);
      delete state;
      state = NULL;
      assert(t != NULL);
      if (*t != NULL) {
        delete *t;
      }
      delete t;
      t = NULL;
      assert(ready_callbacks != NULL);
      delete ready_callbacks;
      ready_callbacks = NULL;
      assert(discarded_callbacks != NULL);
      delete discarded_callbacks;
      discarded_callbacks = NULL;
      assert(latch != NULL);
      delete latch;
      latch = NULL;
    }
  }
}

}  // namespace process {

#endif // __PROCESS_FUTURE_HPP__
