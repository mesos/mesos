# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License

# Add 'subdir-objects' to build objects in respective subdirectories
# which is necessary for dealing with naming conflicts, i.e,,
# master/http.cpp and slave/http.cpp.
AUTOMAKE_OPTIONS = subdir-objects

BOOST = third_party/boost-1.37.0
PROTOBUF = third_party/protobuf-2.3.0
GLOG = third_party/glog-0.3.1
GMOCK = third_party/gmock-1.6.0
GTEST = $(GMOCK)/gtest
LEVELDB = third_party/leveldb
ZOOKEEPER = third_party/zookeeper-3.3.1/src/c
LIBPROCESS = third_party/libprocess
LIBEV = $(LIBPROCESS)/third_party/libev-3.8

# Used for building Python eggs.
SETUPTOOLS = third_party/distribute-0.6.19/distribute-0.6.19-py2.6.egg


# Protocol buffer compiler.
PROTOC = $(top_builddir)/$(PROTOBUF)/src/protoc

# Initialize here so we can use += operator everywhere else.
lib_LTLIBRARIES =
noinst_LTLIBRARIES =
bin_PROGRAMS =
check_PROGRAMS =
BUILT_SOURCES =
TESTS =
CLEANFILES =


# TODO(benh): Add optimization flags when building the core library.
# TODO(benh): Remove $(PTHREAD_LIBS) from each LDADD.

# Our library!
lib_LTLIBRARIES += libmesos.la

libmesos_la_SOURCES = mesos.pb.cc messages/messages.pb.cc		\
                      messages/log.pb.cc sched/sched.cpp		\
                      local/local.cpp master/master.cpp			\
                      master/http.cpp master/slaves_manager.cpp		\
                      master/frameworks_manager.cpp			\
                      master/allocator_factory.cpp			\
                      master/simple_allocator.cpp slave/slave.cpp	\
                      slave/http.cpp slave/isolation_module.cpp		\
                      slave/process_based_isolation_module.cpp		\
                      slave/reaper.cpp launcher/launcher.cpp		\
                      exec/exec.cpp common/fatal.cpp common/lock.cpp	\
                      detector/detector.cpp detector/url_processor.cpp	\
                      configurator/configurator.cpp common/logging.cpp	\
                      common/utils.cpp common/date_utils.cpp		\
                      common/resources.cpp zookeeper/zookeeper.cpp	\
                      zookeeper/authentication.cpp zookeeper/group.cpp

libmesos_la_CPPFLAGS = -I$(top_srcdir)/include                          \
                       -I$(top_srcdir)/$(BOOST)                         \
                       -I$(top_srcdir)/$(PROTOBUF)/src                  \
                       -I$(top_srcdir)/$(GLOG)/src                      \
                       -I$(top_builddir)/$(GLOG)/src                    \
                       -I$(top_srcdir)/$(LIBPROCESS)/include            \
                       $(AM_CPPFLAGS)

# TODO(benh): Make libprocess be autotoolized too!
libmesos_la_LIBADD = libbuild.la					  \
                     liblog.la     				          \
                     $(top_builddir)/$(PROTOBUF)/src/.libs/libprotobuf.la \
                     $(top_builddir)/$(GLOG)/.libs/libglog.la		  \
                     $(top_builddir)/$(LIBPROCESS)/libprocess.a 	  \
                     $(top_builddir)/$(LIBEV)/.libs/libev.la


if WITH_INCLUDED_ZOOKEEPER
  libmesos_la_CPPFLAGS += -I$(top_srcdir)/$(ZOOKEEPER)/include \
                          -I$(top_srcdir)/$(ZOOKEEPER)/generated
  libmesos_la_LIBADD += $(top_builddir)/$(ZOOKEEPER)/.libs/libzookeeper_mt.la
endif


# Convenience library that *always* gets rebuilt to ensure accurate info.
noinst_LTLIBRARIES += libbuild.la
libbuild_la_SOURCES = common/build.cpp
libbuild_la_CPPFLAGS = -DBUILD_DATE="\"$$(date '+%Y-%m-%d %H:%M:%S')\"" \
                       -DBUILD_USER="\"$$USER\""                        \
                       -DBUILD_FLAGS="\"$(AM_CFLAGS) $(AM_CXXFLAGS)\""  \
                       $(AM_CPPFLAGS)
libbuild_la_DEPENDENCIES = FORCE


# Convenience library for building the log that includes the leveldb headers.
noinst_LTLIBRARIES += liblog.la
liblog_la_SOURCES = log/coordinator.cpp log/replica.cpp
liblog_la_CPPFLAGS = -I$(top_builddir)/$(LEVELDB)/include	\
                     $(libmesos_la_CPPFLAGS)
liblog_la_LDFLAGS = -L$(top_builddir)/$(LEVELDB)
liblog_la_LIBADD = -lleveldb


# Convenience library for the webui files so we can easily add the
# Python specific flags needed for building.
noinst_LTLIBRARIES += libwebui.la
libwebui_la_SOURCES = master/webui.cpp slave/webui.cpp
libwebui_la_CPPFLAGS = $(WEBUI_CPPFLAGS) $(libmesos_la_CPPFLAGS)


# TODO(benh): "Build" the Python webui files so that we can run the
# binaries directly out of the build directory.

BUILT_SOURCES += examples/java/test_framework

examples/java/test_framework:
	@cp -f $(srcdir)/$@ $@

examples/java/test_exception_framework:
	@cp -f $(srcdir)/$@ $@


# Targets for generating the protocol buffer code we generate the C++,
# Java, and Python code even if we aren't building Java or Python.
mesos.pb.cc: $(top_srcdir)/include/mesos/mesos.proto
	@mkdir -p java/generated
	@mkdir -p python
	$(PROTOC) -I$(top_srcdir)/include/mesos                  \
            --cpp_out=.                                          \
            --java_out=java/generated                            \
            --python_out=python                                  \
            $(top_srcdir)/include/mesos/mesos.proto

messages/messages.pb.cc: $(srcdir)/messages/messages.proto
	@mkdir -p messages
	$(PROTOC) -I$(srcdir)/messages -I$(top_srcdir)/include/mesos \
            --cpp_out=messages $(srcdir)/messages/messages.proto

messages/log.pb.cc: $(srcdir)/messages/log.proto
	@mkdir -p messages
	$(PROTOC) -I$(srcdir)/messages -I$(top_srcdir)/include/mesos \
            --cpp_out=messages $(srcdir)/messages/log.proto

CLEANFILES += mesos.pb.h mesos.pb.cc messages/messages.pb.h	\
              messages/messages.pb.cc messages/log.pb.h		\
              messages/log.pb.cc				\
              java/generated/org/apache/mesos/*.java            \
              python/mesos_pb2.py


# Binaries.
bin_PROGRAMS += mesos-master
mesos_master_SOURCES = master/main.cpp
mesos_master_CPPFLAGS = $(libmesos_la_CPPFLAGS)
mesos_master_LDFLAGS = $(WEBUI_LDFLAGS) $(AM_LDFLAGS)
mesos_master_LDADD = $(PTHREAD_LIBS) libwebui.la libmesos.la

bin_PROGRAMS += mesos-slave
mesos_slave_SOURCES = slave/main.cpp
mesos_slave_CPPFLAGS = $(libmesos_la_CPPFLAGS)
mesos_slave_LDFLAGS = $(WEBUI_LDFLAGS) $(AM_LDFLAGS)
mesos_slave_LDADD = $(PTHREAD_LIBS) libwebui.la libmesos.la

bin_PROGRAMS += mesos-local
mesos_local_SOURCES = local/main.cpp
mesos_local_CPPFLAGS = $(libmesos_la_CPPFLAGS)
mesos_local_LDADD = $(PTHREAD_LIBS) libmesos.la

bin_PROGRAMS += mesos-launcher
mesos_launcher_SOURCES = launcher/main.cpp launcher/launcher.cpp
mesos_launcher_CPPFLAGS = $(libmesos_la_CPPFLAGS)
mesos_launcher_LDADD = $(PTHREAD_LIBS) libmesos.la

bin_PROGRAMS += mesos-log
mesos_log_SOURCES = log/main.cpp
mesos_log_CPPFLAGS = $(libmesos_la_CPPFLAGS)
mesos_log_LDADD = $(PTHREAD_LIBS) libmesos.la

# TODO(benh): Support Solaris.
# bin_PROGRAMS += mesos-projd
# mesos_projd_SOURCES = slave/projd.cpp
# mesos_projd_CPPFLAGS = $(libmesos_la_CPPFLAGS)
# mesos_projd_LDADD = $(PTHREAD_LIBS) libmesos.la

bin_PROGRAMS += mesos-mesos
mesos_mesos_SOURCES = mesos/main.cpp
mesos_mesos_CPPFLAGS = $(libmesos_la_CPPFLAGS)
mesos_mesos_LDADD = $(PTHREAD_LIBS) libmesos.la


# Protocol buffers JAR for Java.
PROTOBUF_JAR = $(top_builddir)/protobuf.jar

$(PROTOBUF_JAR):
if HAS_JAVA
	@mkdir -p $(top_builddir)/$(PROTOBUF)/java/src/main/java
	$(PROTOC) --java_out=$(top_builddir)/$(PROTOBUF)/java/src/main/java \
          -I$(top_srcdir)/$(PROTOBUF)/src                                   \
          $(top_srcdir)/$(PROTOBUF)/src/google/protobuf/descriptor.proto
	$(JAVA_HOME)/bin/javac                                                      \
          -d $(top_builddir)/$(PROTOBUF)/java/src/main/java                         \
          $(top_srcdir)/$(PROTOBUF)/java/src/main/java/com/google/protobuf/*.java   \
          $(top_builddir)/$(PROTOBUF)/java/src/main/java/com/google/protobuf/*.java
	$(JAVA_HOME)/bin/jar cf $@ -C $(top_builddir)/$(PROTOBUF)/java/src/main/java com
endif

CLEANFILES += $(PROTOBUF_JAR)

# Java Mesos JAR.
MESOS_JAR = $(top_builddir)/src/mesos.jar

$(MESOS_JAR): $(PROTOBUF_JAR) $(srcdir)/java/src/org/apache/mesos/*.java mesos.pb.cc
if HAS_JAVA
	@rm -f java/classes/org/apache/mesos/*
	@mkdir -p java/classes/org/apache/mesos
	$(JAVA_HOME)/bin/javac -cp $(PROTOBUF_JAR) -sourcepath java/generated \
          -d java/classes java/generated/org/apache/mesos/*.java
	$(JAVA_HOME)/bin/javac -cp $(PROTOBUF_JAR):java/classes               \
          -sourcepath $(srcdir)/java/src -d java/classes                      \
          $(srcdir)/java/src/org/apache/mesos/*.java
	$(JAVA_HOME)/bin/jar cf $@ -C java/classes org
endif

CLEANFILES += java/classes/org/apache/mesos/*.java $(MESOS_JAR)


# Support library for Java bindings.
if HAS_JAVA
  noinst_LTLIBRARIES += libjava.la
  libjava_la_SOURCES = java/jni/convert.cpp java/jni/construct.cpp	  \
	               java/jni/org_apache_mesos_MesosSchedulerDriver.cpp \
	               java/jni/org_apache_mesos_MesosExecutorDriver.cpp  \
	               java/jni/org_apache_mesos_Log.cpp                  \
                       jvm/jvm.cpp
  libjava_la_CPPFLAGS = -I$(srcdir)/java/jni -Ijava/jni                 \
                        -I$(JAVA_HOME)/include                          \
                        -I$(JAVA_HOME)/include/$(OS_NAME)               \
                        -I$(JAVA_HEADERS) $(libmesos_la_CPPFLAGS)
  libmesos_la_LIBADD += libjava.la

  # We need to generate the JNI header files before we build the
  # sources and the easiest way to do that with autotools is with
  # the BUILT_SOURCES mechanism. The downside of this is that a user
  # cannot run 'make libjava.la' (or any target that uses BUILT_SOURCES)
  # but that should not matter as libjava.la is just a convenience
  # library and should not get exposed.

  nodist_libjava_la_SOURCES =				\
    java/jni/org_apache_mesos_MesosSchedulerDriver.h	\
    java/jni/org_apache_mesos_MesosExecutorDriver.h	\
    java/jni/org_apache_mesos_Log.h

  BUILT_SOURCES += java/jni/org_apache_mesos_MesosSchedulerDriver.h	\
                   java/jni/org_apache_mesos_MesosExecutorDriver.h	\
                   java/jni/org_apache_mesos_Log.h

java/jni/org_apache_mesos_MesosSchedulerDriver.h: $(MESOS_JAR)
	$(JAVA_HOME)/bin/javah -d java/jni -classpath                         \
          $(PROTOBUF_JAR):java/classes org.apache.mesos.MesosSchedulerDriver

java/jni/org_apache_mesos_MesosExecutorDriver.h: $(MESOS_JAR)
	$(JAVA_HOME)/bin/javah -d java/jni -classpath                         \
          $(PROTOBUF_JAR):java/classes org.apache.mesos.MesosExecutorDriver

java/jni/org_apache_mesos_Log.h: $(MESOS_JAR)
	$(JAVA_HOME)/bin/javah -d java/jni -classpath                         \
          $(PROTOBUF_JAR):java/classes org.apache.mesos.Log
endif


# TODO(benh): Create a protobuf egg.
# PROTOBUF_EGG = $(PROTOBUF)/dist/

PYTHON_EGG = dist/mesos.egg # TODO(benh) Figure out how to names this.

$(PYTHON_EGG): $(srcdir)/python/src/mesos.py mesos.pb.cc
if HAS_PYTHON
#	@mkdir -p python/src
#	@cp $(srcdir)/python/src/mesos.py python/src
#	@cp python/mesos_pb2.py python/src
#	$(PROTOC) --python_out=$(top_builddir)/$(PROTOBUF)/python -I@top_srcdir@/$(PROTOBUF)/src @top_srcdir@/$(PROTOBUF)/src/google/protobuf/descriptor.proto
#        PYTHONPATH=$(top_builddir)/$(SETUPTOOLS) $(PYTHON)	\
#        python/setup.py bdist_egg
#if test $(top_srcdir) = $(top_builddir); then
#	@cp -r $(top_srcdir)/$(PROTOBUF)/python $(top_builddir)/$(PROTOBUF)/python
#	@mkdir -p @top_builddir@/$(PROTOBUF)/src/google/protobuf
#	@cp -f @top_srcdir@/$(PROTOBUF)/src/google/protobuf/descriptor.proto @top_builddir@/$(PROTOBUF)/src/google/protobuf/descriptor.proto
#fi
endif

# TODO(*): Build standalone replicated log library and jar file.

all-local: $(MESOS_JAR) $(PYTHON_EGG)

check_PROGRAMS += test-scheduler
test_scheduler_SOURCES = examples/test_scheduler.cpp
test_scheduler_CPPFLAGS = $(libmesos_la_CPPFLAGS)
test_scheduler_LDADD = $(PTHREAD_LIBS) libmesos.la

check_PROGRAMS += test-executor
test_executor_SOURCES = examples/test_executor.cpp
test_executor_CPPFLAGS = $(libmesos_la_CPPFLAGS)
test_executor_LDADD = $(PTHREAD_LIBS) libmesos.la

check_PROGRAMS += long-lived-scheduler
long_lived_scheduler_SOURCES = examples/long_lived_scheduler.cpp
long_lived_scheduler_CPPFLAGS = $(libmesos_la_CPPFLAGS)
long_lived_scheduler_LDADD = $(PTHREAD_LIBS) libmesos.la

check_PROGRAMS += long-lived-executor
long_lived_executor_SOURCES = examples/long_lived_executor.cpp
long_lived_executor_CPPFLAGS = $(libmesos_la_CPPFLAGS)
long_lived_executor_LDADD = $(PTHREAD_LIBS) libmesos.la

check_PROGRAMS += memhog
memhog_SOURCES = examples/memhog.cpp
memhog_CPPFLAGS = $(libmesos_la_CPPFLAGS)
memhog_LDADD = $(PTHREAD_LIBS) libmesos.la

check_PROGRAMS += scheduled-memhog
scheduled_memhog_SOURCES = examples/scheduled_memhog.cpp
scheduled_memhog_CPPFLAGS = $(libmesos_la_CPPFLAGS)
scheduled_memhog_LDADD = $(PTHREAD_LIBS) libmesos.la

check_PROGRAMS += memhog-executor
memhog_executor_SOURCES = examples/memhog_executor.cpp
memhog_executor_CPPFLAGS = $(libmesos_la_CPPFLAGS)
memhog_executor_LDADD = $(PTHREAD_LIBS) libmesos.la


# Build Java examples into their own JAR.
EXAMPLES_JAR = examples.jar

$(EXAMPLES_JAR): $(srcdir)/examples/java/*.java
if HAS_JAVA
	@mkdir -p examples/java
	$(JAVA_HOME)/bin/javac -cp					\
          $(PROTOBUF_JAR):$(MESOS_JAR):$(srcdir)/examples/java		\
          -sourcepath $(srcdir)/examples/java -d examples/java	        \
          $(srcdir)/examples/java/*.java
	$(JAVA_HOME)/bin/jar cf $@ -C examples/java .
endif


# TODO(benh): Build Python examples on check.


check_PROGRAMS += mesos-tests

mesos_tests_SOURCES = tests/main.cpp tests/utils.cpp			\
	              tests/master_tests.cpp				\
	              tests/resource_offers_tests.cpp			\
	              tests/fault_tolerance_tests.cpp			\
	              tests/log_tests.cpp tests/resources_tests.cpp	\
	              tests/uuid_tests.cpp tests/external_tests.cpp	\
	              tests/sample_frameworks_tests.cpp			\
	              tests/configurator_tests.cpp			\
	              tests/strings_tests.cpp				\
	              tests/multihashmap_tests.cpp			\
	              tests/protobuf_io_tests.cpp			\
	              tests/lxc_isolation_tests.cpp			\
	              tests/utils_tests.cpp 				\
	              tests/url_processor_tests.cpp			\
	              tests/killtree_tests.cpp				\
	              tests/exception_tests.cpp

mesos_tests_CPPFLAGS = -DROOT_DIR=\"$(abs_top_srcdir)\"                 \
                       -DBUILD_DIR=\"$(abs_top_builddir)\"              \
                       -I$(top_srcdir)/$(GTEST)/include	                \
                       -I$(top_srcdir)/$(GMOCK)/include                 \
                       $(libmesos_la_CPPFLAGS)

mesos_tests_LDADD = $(PTHREAD_LIBS) $(top_builddir)/.libs/libgmock.la	\
                    libmesos.la

if HAS_JAVA
  mesos_tests_SOURCES += tests/zookeeper_server.cpp		\
                         tests/base_zookeeper_test.cpp		\
                         tests/zookeeper_server_tests.cpp	\
                         tests/zookeeper_tests.cpp
  mesos_tests_CPPFLAGS += -I$(JAVA_HOME)/include                          \
                          -I$(JAVA_HOME)/include/$(OS_NAME)               \
                          -I$(JAVA_HEADERS)

  mesos_tests_LDFLAGS = $(JAVA_LDFLAGS) $(AM_LDFLAGS)

  # TODO(benh): Figure out how to build the Java examples independently.
  mesos_tests_DEPENDENCIES = $(EXAMPLES_JAR)
endif


# TODO(benh): Get external (i.e., script based) tests working.


TESTS += mesos-tests

FORCE: