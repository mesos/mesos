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
# limitations under the License.

# Generated with autoscan, then modified appropriately.
# Process this file with autoconf to produce a configure script.

AC_PREREQ(2.61)
AC_INIT([mesos], [1.0])

# Have autoconf setup some variables related to the system.
AC_CANONICAL_SYSTEM

# Initialize automake.
AM_INIT_AUTOMAKE([-Wall -Werror silent-rules foreign])

# Allow less verbose output when building.
AM_SILENT_RULES

# Initialize libtool.
LT_PREREQ([2.2])
LT_INIT

AC_LANG(C++)

AC_CONFIG_MACRO_DIR([m4])

# Update CFLAGS and CXXFLAGS with environment args.
# TODO(benh): Stop passing -fPIC, instead assume libtool will do the
# right thing and get libprocess to correctly use autotools.
dnl CFLAGS="-O2 -fno-strict-aliasing -fPIC ${CFLAGS}"
dnl CXXFLAGS="-O2 -fno-strict-aliasing -fPIC ${CXXFLAGS}"

dnl export CFLAGS
dnl export CXXFLAGS

# Update CFLAGS and CXXFLAGS in configure args.
# TODO(benh): Don't send args just for us (e.g., --with-webui).
dnl ac_configure_args=`echo $ac_configure_args | \
dnl                    sed -e "s/CFLAGS=/CFLAGS=-O2 -fno-strict-aliasing /"`
dnl ac_configure_args=`echo $ac_configure_args | \
dnl                    sed -e "s/CXXFLAGS=/CXXFLAGS=-O2 -fno-strict-aliasing /"`

dnl ac_configure_args="$ac_configure_args --enable-shared=no --with-pic"

# Force all third_party libraries to only build static libraries so
# that we can produce a final shared library which includes everything
# necessary (this is a well recognized hack for passing configure
# arguments to subdirs).
old_ac_configure_args="$ac_configure_args"
ac_configure_args="$ac_configure_args --enable-shared=no"

AC_CONFIG_SUBDIRS([third_party/protobuf-2.3.0])
AC_CONFIG_SUBDIRS([third_party/glog-0.3.1])
AC_CONFIG_SUBDIRS([third_party/zookeeper-3.3.1/src/c])
AC_CONFIG_SUBDIRS([third_party/libprocess])

ac_configure_args="$old_ac_configure_args"

AC_CONFIG_FILES([Makefile])
AC_CONFIG_FILES([src/Makefile])
dnl AC_CONFIG_FILES([src/examples/Makefile])
dnl AC_CONFIG_FILES([src/examples/java/Makefile])
dnl AC_CONFIG_FILES([src/examples/python/Makefile])
dnl AC_CONFIG_FILES([src/tests/Makefile])
AC_CONFIG_FILES([src/config/config.hpp])


# TODO(benh): Force Python 2.6?
if test -z "$PYTHON"; then
  PYTHON=python2.6
fi


# Determine the current OS (TODO(benh): Does autotools do this for us?).
case "${target_os}" in
  linux*)
    echo ===========================================================
    echo Setting up build environment for ${target_cpu} ${target_os}
    echo ===========================================================
    OS_NAME=linux
    ;;
  darwin*)
    echo ===========================================================
    echo Setting up build environment for ${target_cpu} ${target_os}
    echo ===========================================================
    OS_NAME=darwin
    CFLAGS="${CFLAGS} -D_XOPEN_SOURCE"
    CXXFLAGS="${CXXFLAGS} -D_XOPEN_SOURCE"
    ;;
  solaris*)
    echo ===========================================================
    echo Setting up build environment for ${target_cpu} ${target_os}
    echo ===========================================================
    CC=gcc-4.3.2
    CXX=g++-4.3.2
    export CC
    export CXX
    CFLAGS="${CFLAGS} -pthreads -march=i586"
    CXXFLAGS="${CXXFLAGS} -pthreads -march=i586"
    LIBS="-lsocket -lnsl -lproject -lproc"
    OS_NAME=solaris
    ;;
  *)
    AC_MSG_ERROR("Mesos is currently unsupported on your platform.")
  esac

# Export OS name to Makefiles
AC_SUBST(OS_NAME)

AM_CONDITIONAL([OS_LINUX], [test x$OS_NAME = xlinux])

JAVA_HOME=""
JAVA_LDFLAGS=""
JAVA_HEADERS=""
HAS_JAVA="0"
AC_ARG_WITH([java-home],
  AC_HELP_STRING([--with-java-home=DIR],
                 [find Java Development Kit in DIR]),
	         [case "$with_java_home" in
		    yes) 
                      AC_MSG_ERROR([
*** --with-java-home requires an argument]) ;;
		    *)
                      JAVA_HOME="$withval"
                      HAS_JAVA="1"
                      has_java=true
                      if test -z "$JAVA_HEADERS"; then
                        JAVA_HEADERS="$JAVA_HOME/include/$OS_NAME"
                      fi
                      if test "$OS_NAME" = "darwin"; then
                        JAVA_LDFLAGS="-framework JavaVM"
                      elif test "$OS_NAME" = "linux"; then
                        # TODO(John Sirois): Select directory based on arch.
                        JAVA_LDFLAGS="-L$JAVA_HOME/jre/lib/amd64/server -ljvm"
                      fi
                      ;;
	          esac],
	         [])
AC_SUBST(JAVA_HOME)
AC_SUBST(JAVA_LDFLAGS)
AC_SUBST(HAS_JAVA)
AM_CONDITIONAL([HAS_JAVA], [test x$has_java = xtrue])

AC_ARG_WITH([java-headers],
  AC_HELP_STRING([--with-java-headers=DIR],
                 [find Java headers in DIR (if different from JAVA_HOME/include)]),
	         [case "$with_java_headers" in
		    yes) 
                      AC_MSG_ERROR([
*** --with-java-headers requires an argument]) ;;
		    *)
                      JAVA_HEADERS="$withval"
                      ;;
	          esac],
	         [])
AC_SUBST(JAVA_HEADERS)

# TODO(benh): Make sure that JAVA_HEADERS is set iff JAVA_HOME is set.


PYTHON_HEADERS=""
PYTHON_LDFLAGS=""
HAS_PYTHON="0"
AC_ARG_WITH([python-headers],
  AC_HELP_STRING([--with-python-headers=DIR],
                 [find Python headers in DIR]),
	         [case "$with_python_headers" in
		    yes)
                      AC_MSG_ERROR([
*** --with-python-headers requires an argument]) ;;
		    *)
                      PYTHON_HEADERS="$withval"
                      echo running $PYTHON to find compiler flags for creating the Mesos Python library...
                      PYTHON_LDFLAGS=`$PYTHON -c 'from distutils import sysconfig; print sysconfig.get_config_var("LINKFORSHARED") + " -lpython" + sysconfig.get_config_var("VERSION")'`
                      HAS_PYTHON="1"
                      has_python=true
                      ;;
	          esac],
	         [])
AC_SUBST(PYTHON_HEADERS)
AC_SUBST(PYTHON_LDFLAGS)
AC_SUBST(HAS_PYTHON)
AM_CONDITIONAL([HAS_PYTHON], [test x$has_python = xtrue])


WITH_WEBUI="0"
AC_ARG_WITH([webui],
  AC_HELP_STRING([--with-webui],
                 [enable web UI (requires Python)]),
            [case "$with_webui" in
              yes) webui=true
                   WITH_WEBUI=1
                   echo running $PYTHON to find compiler flags for embedding it...
                   WEBUI_CPPFLAGS=`$PYTHON -c 'from distutils import sysconfig; print "-I" + sysconfig.get_config_var("INCLUDEPY")'`
                   WEBUI_LDFLAGS=`$PYTHON -c 'from distutils import sysconfig; print sysconfig.get_config_var("LINKFORSHARED") + " -lpython" + sysconfig.get_config_var("VERSION")'`
                   if [[ "$?" -ne "0" ]]; then
                     AC_MSG_ERROR([
*** Failed to determine compiler flags for embedding Python, needed by web UI.
*** Check that you have Python 2.6 development packages installed.])
                   fi ;;
              no) webui=false
                  WITH_WEBUI=0 ;;
              *) AC_MSG_ERROR([
*** --with-webui does not take arguments]) ;;
	    esac],
	    [])
AC_SUBST(WEBUI_CPPFLAGS)
AC_SUBST(WEBUI_LDFLAGS)
AC_SUBST(WITH_WEBUI)
AM_CONDITIONAL([WITH_WEBUI], [test x$webui = xtrue])


AC_ARG_WITH([included-zookeeper],
  AC_HELP_STRING([--with-included-zookeeper],
                 [build with included ZooKeeper]),
	    [case "$with_included_zookeeper" in
              yes) included_zookeeper=true ;;
              no) included_zookeeper=false ;;
              *) AC_MSG_ERROR([
*** --with-included-zookeeper does not take arguments]) ;;
	    esac],
	    [])
AM_CONDITIONAL([WITH_INCLUDED_ZOOKEEPER],
               [test x$included_zookeeper = xtrue])

# Checks for programs.
AC_PROG_CXX([g++])
AC_PROG_CC([gcc])

AC_OUTPUT
