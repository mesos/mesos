#include <gtest/gtest.h>

#include "config.hpp"
#include "external_test.hpp"

// Run each of the sample frameworks in local mode
TEST_EXTERNAL(SampleFrameworks, CFramework)
TEST_EXTERNAL(SampleFrameworks, CppFramework)
#if MESOS_HAS_JAVA
  TEST_EXTERNAL(SampleFrameworks, JavaSwigFramework)
  TEST_EXTERNAL(SampleFrameworks, JavaSwigExceptionFramework)
  TEST_EXTERNAL(SampleFrameworks, JavaJNIFramework)
#endif 
#if MESOS_HAS_PYTHON
  TEST_EXTERNAL(SampleFrameworks, PythonFramework)
#endif

// Some tests for command-line and environment configuration
TEST_EXTERNAL(SampleFrameworks, CFrameworkCmdlineParsing)
TEST_EXTERNAL(SampleFrameworks, CFrameworkInvalidCmdline)
TEST_EXTERNAL(SampleFrameworks, CFrameworkInvalidEnv)
