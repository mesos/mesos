#!/usr/bin/env python

import random

# Scheduler configurations are pairs of (todo, duration) to run. A
# todo with value -1 means continually run tasks.

config = []

for i in range(200):
  todo = -1
  duration = random.normalvariate(10, 2)
  config.append((todo, duration))
