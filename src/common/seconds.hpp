#ifndef __SECONDS_HPP__
#define __SECONDS_HPP__

struct seconds
{
  seconds(double _value) : value(_value) {}
  int millis() const { return value * 1000; }
  int micros() const { return value * 1000000; }
  int nanos() const { return value * 1000000000; }
  const double value;
};

#endif // __SECONDS_HPP__
