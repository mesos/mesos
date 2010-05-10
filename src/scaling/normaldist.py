#!/usr/bin/env python

import random

# Scheduler configurations are pairs of (todo, duration) to run. A
# todo with value -1 means continually run tasks.

config = []

for i in range(200):
  todo = -1
  duration = 0
  while duration == 0:
    duration = random.normalvariate(30, 10)
  config.append((todo, duration))
