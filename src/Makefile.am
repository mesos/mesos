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

SUFFIXES = .pb.cc .pb.h .proto

BOOST = third_party/boost-1.37.0
PROTOBUF = third_party/protobuf-2.3.0
GLOG = third_party/glog-0.3.1
GMOCK = third_party/gmock-1.6.0
GTEST = $(GMOCK)/gtest
LEVELDB = third_party/leveldb
ZOOKEEPER = third_party/zookeeper-3.3.1/src/c
LIBPROCESS = third_party/libprocess
LIBEV = $(LIBPROCESS)/third_party/libev-3.8

webuidir = $(pkgdatadir)
pythoneggdir = $(pythondir)
javalibdir = $(libdir)/java

AM_CPPFLAGS = -DMESOS_WEBUIDIR=\"$(webuidir)\" \
	      -DMESOS_LIBEXECDIR=\"$(pkglibexecdir)\"

# Used for building Python eggs.
SETUPTOOLS = third_party/distribute-0.6.19/distribute-0.6.19-py2.6.egg

# Protocol buffer compiler.
PROTOC = $(top_builddir)/$(PROTOBUF)/src/protoc
PROTOCFLAGS = -I$(top_srcdir)/include/mesos -I$(srcdir)

# Initialize variables here so we can use += operator everywhere else.
lib_LTLIBRARIES =
noinst_LTLIBRARIES =
sbin_PROGRAMS =
bin_PROGRAMS =
pkglibexec_PROGRAMS =
dist_pkglibexec_SCRIPTS = 
webui_SCRIPTS =
nobase_dist_webui_SCRIPTS =
check_PROGRAMS =
dist_check_SCRIPTS = 
BUILT_SOURCES =
TESTS =
CLEANFILES =
EXTRA_DIST =
PHONY_TARGETS = 

# TODO(benh): Remove $(PTHREAD_LIBS) from each LDADD.

# Our library!
lib_LTLIBRARIES += libmesos.la

# Necessary so automake doesn't try to guess source files.
libmesos_la_SOURCES = 

# We put everything in an convenience library, so we can build a self-contained
# python binding (which links in everything).
libmesos_la_LIBADD = libmesos_pre.la

libmesos_la_LDFLAGS = -version-info 0:0 -shared
# Since we don't have any sources, we need to tell automake that this is a
# C++ library
libmesos_la_LIBTOOLFLAGS = --tag=CXX

# Convenience version of libmesos for building python extension.
noinst_LTLIBRARIES += libmesos_pre.la

nodist_libmesos_pre_la_SOURCES = messages/messages.pb.cc        \
	   		         messages/log.pb.cc \
			         messages/messages.pb.h messages/log.pb.h

libmesos_pre_la_SOURCES = sched/sched.cpp local/local.cpp master/master.cpp \
                          master/http.cpp master/slaves_manager.cpp	\
                          master/frameworks_manager.cpp			\
                          master/allocator_factory.cpp			\
                          master/simple_allocator.cpp slave/slave.cpp	\
                          slave/http.cpp slave/isolation_module.cpp	\
                          slave/process_based_isolation_module.cpp	\
                          slave/reaper.cpp launcher/launcher.cpp	\
                          exec/exec.cpp common/fatal.cpp common/lock.cpp\
                          detector/detector.cpp				\
			  detector/url_processor.cpp			\
                          configurator/configurator.cpp			\
			  common/logging.cpp				\
                          common/utils.cpp common/date_utils.cpp	\
                          common/resources.cpp zookeeper/zookeeper.cpp	\
                          zookeeper/authentication.cpp zookeeper/group.cpp

if OS_LINUX
libmesos_pre_la_SOURCES += slave/lxc_isolation_module.cpp
else
EXTRA_DIST += slave/lxc_isolation_module.cpp
endif

EXTRA_DIST += slave/solaris_project_isolation_module.cpp

libmesos_pre_la_SOURCES += common/build.hpp common/date_utils.hpp	\
		           common/factory.hpp common/fatal.hpp 		\
		           common/foreach.hpp common/hashmap.hpp 	\
		           common/hashset.hpp common/json.hpp 		\
		           common/lock.hpp common/logging.hpp		\
		           common/lambda.hpp common/option.hpp		\
		           common/resources.hpp common/result.hpp 	\
		           common/multihashmap.hpp common/process_utils.hpp \
		           common/seconds.hpp common/try.hpp 		\
		           common/type_utils.hpp common/units.hpp 	\
		           common/uuid.hpp common/strings.hpp		\
		           configurator/configuration.hpp		\
		           configurator/configurator.hpp		\
		           configurator/option.hpp detector/detector.hpp\
		           detector/url_processor.hpp launcher/launcher.hpp \
		           local/local.hpp log/cache.hpp log/coordinator.hpp\
		           log/replica.hpp log/log.hpp log/network.hpp 	\
		           master/allocator.hpp master/allocator_factory.hpp\
		           master/constants.hpp				\
		           master/frameworks_manager.hpp		\
		           master/http.hpp master/master.hpp 		\
		           master/simple_allocator.hpp 			\
		           master/slaves_manager.hpp master/webui.hpp	\
		           messages/log.hpp messages/messages.hpp	\
		           slave/constants.hpp slave/http.hpp		\
		           slave/isolation_module.hpp			\
		           slave/isolation_module_factory.hpp		\
		           slave/lxc_isolation_module.hpp		\
		           slave/process_based_isolation_module.hpp	\
		           slave/reaper.hpp slave/slave.hpp		\
		           slave/solaris_project_isolation_module.hpp	\
		           slave/webui.hpp tests/external_test.hpp	\
		           tests/base_zookeeper_test.hpp tests/jvm.hpp	\
		           tests/utils.hpp tests/zookeeper_server.hpp	\
		           zookeeper/authentication.hpp zookeeper/group.hpp \
		           zookeeper/watcher.hpp zookeeper/zookeeper.hpp\
                           jvm/jvm.hpp

MESOS_CPPFLAGS = -I$(top_srcdir)/include                          \
                 -I$(top_srcdir)/$(BOOST)                         \
                 -I$(top_srcdir)/$(PROTOBUF)/src                  \
                 -I$(top_srcdir)/$(GLOG)/src                      \
                 -I$(top_builddir)/$(GLOG)/src                    \
                 -I$(top_srcdir)/$(LIBPROCESS)/include            \
                 $(AM_CPPFLAGS)
if WITH_INCLUDED_ZOOKEEPER
  MESOS_CPPFLAGS += -I$(top_srcdir)/$(ZOOKEEPER)/include \
		    -I$(top_srcdir)/$(ZOOKEEPER)/generated
endif
libmesos_pre_la_CPPFLAGS = $(MESOS_CPPFLAGS)

libmesos_pre_la_CXXFLAGS = -O2 -g2 $(AM_CXXFLAGS)

# TODO(benh): Make libprocess be autotoolized too!
libmesos_pre_la_LIBADD = libbuild.la					  \
                         liblog.la     				          \
		         libmesos_proto.la

# For non-convenience libraries we need to link them in to make the shared
# library each time. (Currently, we don't support platforms where this is not
# possible.)
LIBMESOS_LIBS = 

LIBMESOS_LIBS += $(top_builddir)/$(PROTOBUF)/src/.libs/libprotobuf.la
LIBMESOS_LIBS += $(top_builddir)/$(GLOG)/.libs/libglog.la
LIBMESOS_LIBS += $(top_builddir)/$(LIBPROCESS)/libprocess.a
LIBMESOS_LIBS += $(top_builddir)/$(LIBEV)/.libs/libev.la

if WITH_INCLUDED_ZOOKEEPER
  LIBMESOS_LIBS += $(top_builddir)/$(ZOOKEEPER)/.libs/libzookeeper_mt.a
endif

libmesos_la_LIBADD += $(LIBMESOS_LIBS)


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
                     $(MESOS_CPPFLAGS)
liblog_la_LDFLAGS = -L$(top_builddir)/$(LEVELDB)
liblog_la_LIBADD = -lleveldb


# Convenience library for the webui files so we can easily add the
# Python specific flags needed for building.
noinst_LTLIBRARIES += libwebui.la
libwebui_la_SOURCES = master/webui.cpp slave/webui.cpp common/webui_utils.cpp
libwebui_la_SOURCES += master/webui.hpp slave/webui.hpp common/webui_utils.hpp
libwebui_la_CPPFLAGS = $(WEBUI_CPPFLAGS) $(MESOS_CPPFLAGS)

WEBUISCRIPTS = webui/bottle-0.8.3/bottle.py		\
               webui/common/webui_lib.py		\
               webui/master/framework.tpl		\
               webui/master/index.tpl			\
               webui/master/webui.py			\
               webui/slave/framework.tpl		\
               webui/slave/executor.tpl			\
               webui/slave/index.tpl			\
               webui/slave/webui.py			\
               webui/static/stylesheet.css

nobase_dist_webui_SCRIPTS += $(WEBUISCRIPTS)

# Targets for generating the protocol buffer code we generate the C++,
# Java, and Python code even if we aren't building Java or Python.
#
# TODO(charles): These targets need to include _all_ built files.
# TODO(charles): do protobuf header files belong in sources lists for libraries
# below?

PROTOS_JAVA_GEN = java/generated/org/apache/mesos/Protos.java
PROTOS_PYTHON_GEN = python/mesos_pb2.py

MESOS_PROTO_GEN_FILES = mesos.pb.cc \
			mesos.pb.h \
			$(PROTOS_JAVA_GEN) \
			$(PROTOS_PYTHON_GEN)

MESOS_PROTO = $(top_srcdir)/include/mesos/mesos.proto

BUILT_SOURCES += $(MESOS_PROTO_GEN_FILES)

%.pb.cc %.pb.h: $(top_srcdir)/include/mesos/%.proto
	$(MKDIR_P) $(@D)
	$(PROTOC) $(PROTOCFLAGS) --cpp_out=. $^

%.pb.cc %.pb.h: %.proto
	$(MKDIR_P) $(@D)
	$(PROTOC) $(PROTOCFLAGS) --cpp_out=. $^

java/generated/org/apache/mesos/Protos.java: $(MESOS_PROTO)

$(PROTOS_JAVA_GEN): $(MESOS_PROTO)
	$(MKDIR_P)  $(@D)
	$(PROTOC) $(PROTOCFLAGS) --java_out=java/generated $^

$(PROTOS_PYTHON_GEN): $(MESOS_PROTO)
	$(MKDIR_P) $(@D)
	$(PROTOC) $(PROTOCFLAGS) --python_out=python $^

# Convenience library so other convenience libraries can depend on mesos.proto
noinst_LTLIBRARIES += libmesos_proto.la

libmesos_proto_la_SOURCES = mesos.pb.h $(MESOS_PROTO)
nodist_libmesos_proto_la_SOURCES = mesos.pb.cc
libmesos_proto_la_CPPFLAGS = $(MESOS_CPPFLAGS)

MESSAGES_PROTO_GEN_FILES = messages/messages.pb.cc messages/messages.pb.h
BUILT_SOURCES += $(MESSAGES_PROTO_GEN_FILES)

LOG_PROTO_GEN_FILES = messages/log.pb.cc messages/log.pb.h
BUILT_SOURCES += $(LOG_PROTO_GEN_FILES)

CLEANFILES += $(MESOS_PROTO_GEN_FILES) $(MESSAGES_PROTO_GEN_FILES) \
	      $(LOG_PROTO_GEN_FILES)

# Binaries.
sbin_PROGRAMS += mesos-master
mesos_master_SOURCES = master/main.cpp
mesos_master_CPPFLAGS = $(MESOS_CPPFLAGS)
mesos_master_LDFLAGS = $(WEBUI_LDFLAGS) $(AM_LDFLAGS)
mesos_master_LDADD = $(PTHREAD_LIBS) libwebui.la libmesos.la

sbin_PROGRAMS += mesos-slave
mesos_slave_SOURCES = slave/main.cpp
mesos_slave_CPPFLAGS = $(MESOS_CPPFLAGS)
mesos_slave_LDFLAGS = $(WEBUI_LDFLAGS) $(AM_LDFLAGS)
mesos_slave_LDADD = $(PTHREAD_LIBS) libwebui.la libmesos.la

bin_PROGRAMS += mesos-local
mesos_local_SOURCES = local/main.cpp
mesos_local_CPPFLAGS = $(MESOS_CPPFLAGS)
mesos_local_LDADD = $(PTHREAD_LIBS) libmesos.la

pkglibexec_PROGRAMS += mesos-launcher
mesos_launcher_SOURCES = launcher/main.cpp launcher/launcher.cpp
mesos_launcher_CPPFLAGS = $(MESOS_CPPFLAGS)
mesos_launcher_LDADD = $(PTHREAD_LIBS) libmesos.la

bin_PROGRAMS += mesos-log
mesos_log_SOURCES = log/main.cpp
mesos_log_CPPFLAGS = $(MESOS_CPPFLAGS)
mesos_log_LDADD = $(PTHREAD_LIBS) libmesos.la

# TODO(benh): Support Solaris.
# bin_PROGRAMS += mesos-projd
# mesos_projd_SOURCES = slave/projd.cpp
# mesos_projd_CPPFLAGS = $(MESOS_CPPFLAGS)
# mesos_projd_LDADD = $(PTHREAD_LIBS) libmesos.la

bin_PROGRAMS += mesos-mesos
mesos_mesos_SOURCES = mesos/main.cpp
mesos_mesos_CPPFLAGS = $(MESOS_CPPFLAGS)
mesos_mesos_LDADD = $(PTHREAD_LIBS) libmesos.la

dist_pkglibexec_SCRIPTS += scripts/killtree.sh

# Protocol buffers JAR for Java.
PROTOBUF_JAR = $(top_builddir)/protobuf.jar

if HAS_JAVA
# TODO(charles): Specify dependencies for the jar.
$(PROTOBUF_JAR):
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

MESOS_SOURCES_JAR = $(top_builddir)/src/mesos-sources.jar

MESOS_JAR_SRCROOT = $(srcdir)/java/src
MESOS_JAR_SRCDIR = $(srcdir)/java/src/org/apache/mesos
MESOS_JAR_SRC = $(MESOS_JAR_SRCDIR)/*.java $(PROTOS_JAVA_GEN)

EXTRA_DIST += $(MESOS_JAR_SRCDIR)/*.java

if HAS_JAVA
$(MESOS_JAR): $(PROTOBUF_JAR) $(MESOS_JAR_SRC)
	@rm -f java/classes/org/apache/mesos/*
	@mkdir -p java/classes/org/apache/mesos
	$(JAVA_HOME)/bin/javac -cp $(PROTOBUF_JAR) -sourcepath java/generated \
          -d java/classes $(MESOS_JAR_SRC)
	$(JAVA_HOME)/bin/jar cf $@ -C java/classes org

$(MESOS_SOURCES_JAR): $(MESOS_JAR_SRC)
	rm -f $(MESOS_SOURCES_JAR)
	$(JAVA_HOME)/bin/jar cf $@ -C $(MESOS_JAR_SRCROOT) org
	$(JAVA_HOME)/bin/jar uf $@ -C java/generated org

javalib_DATA = $(MESOS_JAR)
endif

CLEANFILES += java/classes $(MESOS_JAR) $(MESOS_SOURCES_JAR)

# Support library for Java bindings.
if HAS_JAVA
  # TODO(Charles Reiss): We really should be building the Java library
  # with -module, which means it should be a different library than the main
  # libmesos.so
  noinst_LTLIBRARIES += libjava.la
  libjava_la_SOURCES = java/jni/convert.cpp java/jni/construct.cpp	  \
	               java/jni/org_apache_mesos_MesosSchedulerDriver.cpp \
	               java/jni/org_apache_mesos_MesosExecutorDriver.cpp  \
	               java/jni/org_apache_mesos_Log.cpp                  \
                       jvm/jvm.cpp
  libjava_la_SOURCES += java/jni/convert.hpp java/jni/construct.hpp \
		       	jvm/jvm.hpp
  libjava_la_CPPFLAGS = -I$(srcdir)/java/jni -Ijava/jni                 \
                        -I$(JAVA_HOME)/include                          \
                        -I$(JAVA_HOME)/include/$(OS_NAME)               \
                        -I$(JAVA_HEADERS) $(MESOS_CPPFLAGS)
  libjava_la_DEPENDENCIES = libmesos_proto.la
  libjava_la_LIBADD = $(JAVA_LDFLAGS)
  libmesos_pre_la_LIBADD += libjava.la

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
	@$(JAVA_HOME)/bin/javah -d java/jni -classpath                        \
          $(PROTOBUF_JAR):java/classes org.apache.mesos.MesosSchedulerDriver

java/jni/org_apache_mesos_MesosExecutorDriver.h: $(MESOS_JAR)
	@$(JAVA_HOME)/bin/javah -d java/jni -classpath                        \
          $(PROTOBUF_JAR):java/classes org.apache.mesos.MesosExecutorDriver

java/jni/org_apache_mesos_Log.h: $(MESOS_JAR)
	@$(JAVA_HOME)/bin/javah -d java/jni -classpath                        \
          $(PROTOBUF_JAR):java/classes org.apache.mesos.Log

maven-install: $(MESOS_JAR) $(MESOS_SOURCES_JAR) java/mesos.pom
	mvn install:install-file \
	  -Dfile=$(MESOS_JAR) -Dsources=$(MESOS_SOURCES_JAR) \
	  -DpomFile=java/mesos.pom

PHONY_TARGETS += maven-install
endif

if HAS_PYTHON
PYTHON_EGG = python/dist/mesos-$(PACKAGE_VERSION)$(PYTHON_EGG_POSTFIX).egg
# TODO(Charles Reiss): Don't hard code this verison number
PROTOBUF_EGG = @top_builddir@/$(PROTOBUF)/python/dist/protobuf-2.3.0$(PYTHON_EGG_PUREPY_POSTFIX).egg

pythonegg_DATA = $(PYTHON_EGG)

# TODO(Charles Reiss): Figure out how to do a proper out-of-source build 
# of these python eggs. Right now we copy the entire source directories into
# the build directory.

# This builds a python egg against that is self-contained. It currently
# depends on the libraries in third_party being built as .a's. (If this is
# changed, the setup.py will need to be edited.
#
# TODO(Charles Reiss): Additionally support building and installing python
# libraries that link against the system libmesos.so. This should be the
# default mode if we eventually are able to rely on system versions of our
# dependencies (since we won't be able to create a standalone egg anyways).
$(PYTHON_EGG): python/setup.py $(srcdir)/python/src/mesos.py libmesos_pre.la $(PROTOBUF_EGG)
	mkdir -p python/src
	cp -p $(srcdir)/python/src/mesos.py python/src
	cp -p python/mesos_pb2.py python/src
	mkdir -p $(top_builddir)/$(PROTOBUF)/python
	$(PROTOC) --python_out=$(top_builddir)/$(PROTOBUF)/python -I@top_srcdir@/$(PROTOBUF)/src @top_srcdir@/$(PROTOBUF)/src/google/protobuf/descriptor.proto
	PYTHONPATH=$(top_builddir)/$(SETUPTOOLS) $(PYTHON)	\
        python/setup.py bdist_egg
	if test "$(top_srcdir)" != "$(top_builddir)"; then \
	  cp -r $(top_srcdir)/$(PROTOBUF)/python $(top_builddir)/$(PROTOBUF)/python; \
	  mkdir -p @top_builddir@/$(PROTOBUF)/src/google/protobuf; \
	  cp -f @top_srcdir@/$(PROTOBUF)/src/google/protobuf/descriptor.proto @top_builddir@/$(PROTOBUF)/src/google/protobuf/descriptor.proto; \
	fi

$(PROTOBUF_EGG):
	rm -r -f @top_builddir@/$(PROTOBUF)/python
	cp -r @top_srcdir@/$(PROTOBUF)/python @top_builddir@/$(PROTOBUF)/python
	mkdir -p @top_builddir@/$(PROTOBUF)/src/google/protobuf
	cd @top_builddir@/$(PROTOBUF)/python && PYTHONPATH=@abs_top_builddir@/$(SETUPTOOLS) $(PYTHON) setup.py bdist_egg

CLEANFILES += $(PYTHON_EGG) $(PROTOBUF_EGG)

all-local: $(PYTHON_EGG)
endif

# TODO(*): Build standalone replicated log library and jar file.


check_PROGRAMS += test-framework
test_framework_SOURCES = examples/test_framework.cpp
test_framework_CPPFLAGS = $(MESOS_CPPFLAGS)
test_framework_LDADD = $(PTHREAD_LIBS) libmesos.la

check_PROGRAMS += test-executor
test_executor_SOURCES = examples/test_executor.cpp
test_executor_CPPFLAGS = $(MESOS_CPPFLAGS)
test_executor_LDADD = $(PTHREAD_LIBS) libmesos.la

check_PROGRAMS += long-lived-framework
long_lived_framework_SOURCES = examples/long_lived_framework.cpp
long_lived_framework_CPPFLAGS = $(MESOS_CPPFLAGS)
long_lived_framework_LDADD = $(PTHREAD_LIBS) libmesos.la

check_PROGRAMS += long-lived-executor
long_lived_executor_SOURCES = examples/long_lived_executor.cpp
long_lived_executor_CPPFLAGS = $(MESOS_CPPFLAGS)
long_lived_executor_LDADD = $(PTHREAD_LIBS) libmesos.la

check_PROGRAMS += memhog
memhog_SOURCES = examples/memhog.cpp
memhog_CPPFLAGS = $(MESOS_CPPFLAGS)
memhog_LDADD = $(PTHREAD_LIBS) libmesos.la

check_PROGRAMS += scheduled-memhog
scheduled_memhog_SOURCES = examples/scheduled_memhog.cpp
scheduled_memhog_CPPFLAGS = $(MESOS_CPPFLAGS)
scheduled_memhog_LDADD = $(PTHREAD_LIBS) libmesos.la

check_PROGRAMS += memhog-executor
memhog_executor_SOURCES = examples/memhog_executor.cpp
memhog_executor_CPPFLAGS = $(MESOS_CPPFLAGS)
memhog_executor_LDADD = $(PTHREAD_LIBS) libmesos.la

# Build Java examples into their own JAR.
EXAMPLES_JAR = examples.jar
JAVA_EXAMPLES_SRC = $(srcdir)/examples/TestExceptionFramework.java \
		    $(srcdir)/examples/TestExecutor.java \
		    $(srcdir)/examples/TestFramework.java \
		    $(srcidr)/examples/TestMultipleExecutorsFramework.java

EXTRA_DIST += $(JAVA_EXAMPLES_SOURCES)

$(EXAMPLES_JAR): $(JAVA_EXAMPLES_SOURCES)
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
                       $(MESOS_CPPFLAGS)

mesos_tests_CXXFLAGS = -g2 $(AM_CXXFLAGS)

mesos_tests_LDADD = $(PTHREAD_LIBS) $(top_builddir)/.libs/libgmock.la	\
                    libmesos.la

mesos_tests_DEPENDENCIES = # Empty definition to allow += below.

if HAS_JAVA
  mesos_tests_SOURCES += tests/zookeeper_server.cpp		\
                         tests/base_zookeeper_test.cpp		\
                         tests/zookeeper_server_tests.cpp	\
                         tests/zookeeper_tests.cpp
  mesos_tests_CPPFLAGS += -I$(JAVA_HOME)/include                          \
                          -I$(JAVA_HOME)/include/$(OS_NAME)               \
                          -I$(JAVA_HEADERS)

  mesos_tests_LDFLAGS = $(JAVA_LDFLAGS) $(AM_LDFLAGS)

  # TODO(benh): Consider building the Java examples independently.
  mesos_tests_DEPENDENCIES += $(EXAMPLES_JAR)
endif

if HAS_PYTHON
  mesos_tests_DEPENDENCIES += $(PYTHON_EGG)
endif

EXAMPLESSCRIPTS = examples/java/test-framework				\
                  examples/java/test-executor				\
                  examples/java/test-exception-framework		\
                  examples/java/test-multiple-executors-framework	\
                  examples/python/test_framework.py			\
                  examples/python/test-framework			\
                  examples/python/test_executor.py			\
                  examples/python/test-executor

# For now, we don't support running the example Java framework with its scripts
# outside of the build directory.

COPIEDEXAMPLESSCRIPTS = examples/java/test-framework		        \
                        examples/java/test-executor		        \
                        examples/java/test-exception-framework		\
                        examples/java/test-multiple-executors-framework

copyexamplescripts:
	 test $(srcdir) == $(builddir) ||        \
         for path in $(COPIEDEXAMPLESSCRIPTS); do\
             mkdir -p `dirname $$path`;          \
             cp -f $(srcdir)/$$path $$path; done

PHONY_TARGETS += copyexamplescripts

mesos_tests_DEPENDENCIES += copyexamplescripts

dist_check_SCRIPTS += $(EXAMPLESSCRIPTS)

EXTERNALSCRIPTS =						\
  tests/external/LxcIsolation/HoldMoreMemThanRequested.sh	\
  tests/external/LxcIsolation/ScaleUpAndDown.sh			\
  tests/external/LxcIsolation/TwoSeparateTasks.sh		\
  tests/external/LxcIsolation/run_scheduled_memhog_test.sh	\
  tests/external/SampleFrameworks/CppFramework.sh		\
  tests/external/SampleFrameworks/JavaExceptionFramework.sh	\
  tests/external/SampleFrameworks/JavaFramework.sh		\
  tests/external/SampleFrameworks/PythonFramework.sh		\
  tests/external/KillTree/KillTreeTest.sh

dist_check_SCRIPTS += $(EXTERNALSCRIPTS)

TESTS += mesos-tests

FORCE:

.PHONY: $(PHONY_TARGETS)
