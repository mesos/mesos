#ifndef __PROCESS_OPTION_HPP__
#define __PROCESS_OPTION_HPP__


namespace process {

template <typename T>
class Option
{
public:
  static Option<T> none()
  {
    return Option<T>(NONE);
  }

  static Option<T> some(const T& t)
  {
    return Option<T>(SOME, new T(t));
  }

  Option(const Option<T>& that)
  {
    state = that.state;
    if (that.t != NULL) {
      t = new T(*that.t);
    } else {
      t = NULL;
    }
  }

  ~Option()
  {
    if (t != NULL) {
      delete t;
    }
  }

  Option<T>& operator = (const Option<T>& that)
  {
    if (this != &that) {
      state = that.state;
      if (that.t != NULL) {
        t = new T(*that.t);
      } else {
        t = NULL;
      }
    }

    return *this;
  }

  bool isSome() { return state == SOME; }
  bool isNone() { return state == NONE; }

  T get() { assert(state == SOME); return *t; }

  enum State {
    SOME,
    NONE,
  };

private:
  Option(State _state, T* _t = NULL)
    : state(_state), t(_t) {}

  State state;
  T* t;
};

}  // namespace process {

#endif // __PROCESS_FUTURE_HPP__

