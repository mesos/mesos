#ifndef __TESTING_JVM_HPP__
#define __TESTING_JVM_HPP__

#include <jni.h>

#include <vector>

namespace mesos {
namespace internal {
namespace test {

// Facilitates embedding a jvm and calling into it.
//
// TODO(John Sirois): Fix variadic methods.  Possibly a way to do this with
// typelists, type concatenation and unwinding builder inheritance
//
// TODO(John Sirois): Support finding static methods.
class Jvm {
public:


  // Forward declarations.
  class ConstructorFinder;
  class MethodFinder;
  class JConstructor;
  class MethodSignature;
  class JMethod;

  // An opaque class descriptor obtained via Jvm::findClass and used to
  // find constructors and methods.
  class JClass
  {
  public:
    JClass(const JClass& other);

    // Returns the class of an array of the current class.
    const JClass arrayOf() const;

    // Creates a builder that can be used to locate a constructor of this
    // class with Jvm::findConstructor.
    ConstructorFinder constructor() const;

    // Creates a builder that can be used to locate an instance method of this
    // class with Jvm::findMethod.
    MethodFinder method(const std::string& name) const;

  private:
    friend class Jvm;

    JClass(const jclass clazz,
           const std::string& nativeName,
           int arrayCount = 0);

    std::string signature() const;

    jclass clazz;
    std::string nativeName;
    int arrayCount;
  };


  // A builder that is used to specify a constructor by specifying its parameter
  // list with zero or more calls to ConstructorFinder::parameter.
  class ConstructorFinder
  {
  public:
    // Adds a parameter to the constructor parameter list.
    ConstructorFinder& parameter(const JClass& type);

  private:
    friend class JClass;
    friend class Jvm;

    ConstructorFinder(const JClass& type);

    const JClass type;
    std::vector<JClass> parameters;
  };


  // An opaque constructor descriptor that can be used to create new instances
  // of a class using Jvm::invokeConstructor.
  class JConstructor
  {
  public:
    JConstructor(const JConstructor& other);

  private:
    friend class Jvm;

    JConstructor(const JClass& clazz, const jmethodID id);

    const JClass clazz;
    const jmethodID id;
  };


  // A builder that is used to specify an instance method by specifying its
  // parameter list with zero or more calls to MethodFinder::parameter and a
  // final call to MethodFinder::returns to get an opaque specification of the
  // method for use with Jvm::findMethod.
  class MethodFinder
  {
  public:
    // Adds a parameter to the method parameter list.
    MethodFinder& parameter(const JClass& type);

    // Terminates description of a method by specifying its return type.
    MethodSignature returns(const JClass& type) const;

  private:
    friend class JClass;

    MethodFinder(const JClass& clazz, const std::string& name);

    const JClass clazz;
    const std::string name;
    std::vector<JClass> parameters;
  };


  // An opaque method specification for use with Jvm::findMethod.
  class MethodSignature
  {
  public:
    MethodSignature(const MethodSignature& other);

  private:
    friend class Jvm;
    friend class MethodFinder;

    MethodSignature(const JClass& clazz,
                    const std::string& name,
                    const JClass& returnType,
                    const std::vector<JClass>& parameters);

    const JClass clazz;
    const std::string name;
    const JClass returnType;
    std::vector<JClass> parameters;
  };


  // An opaque method descriptor that can be used to invoke instance methods
  // using Jvm::invokeMethod.
  class JMethod
  {
  public:
    JMethod(const JMethod& other);

  private:
    friend class Jvm;
    friend class MethodSignature;

    JMethod(const jmethodID method);

    const jmethodID id;
  };


  // RAII container for c++/jvm thread binding management.
  class Attach
  {
  public:
    Attach(Jvm* jvm, bool daemon = true);
    ~Attach();

  private:
      Jvm* _jvm;
  };

  friend class Attach;

  enum JNIVersion
  {
    v_1_1 = JNI_VERSION_1_1,
    v_1_2 = JNI_VERSION_1_2,
    v_1_4 = JNI_VERSION_1_4,
    v_1_6 = JNI_VERSION_1_6
  };

  // Starts a new embedded jvm with the given -D options.  Each option supplied
  // should be of the standard form: '-Dproperty=value'.
  //
  // TODO(John Sirois): Consider elevating classpath as a top level jvm
  // configuration parameter since it will likely always need to be specified.
  // Ditto for and non -X java option.
  Jvm(const std::vector<std::string>& options,
      JNIVersion jniVersion = Jvm::v_1_6);
  ~Jvm();

  // Finds a class with the given native name, ie: 'java/lang/String'.
  JClass findClass(const std::string& name);

  const JClass voidClass;
  const JClass booleanClass;
  const JClass byteClass;
  const JClass charClass;
  const JClass shortClass;
  const JClass intClass;
  const JClass longClass;
  const JClass floatClass;
  const JClass doubleClass;
  const JClass& stringClass() const;

  jobject string(const std::string& str);

  JConstructor findConstructor(const ConstructorFinder& constructor);
  JMethod findMethod(const MethodSignature& signature);

  jobject invoke(const JConstructor& ctor, ...);

  template <typename T>
  T invoke(const jobject receiver, const JMethod& method, ...);

  jobject newGlobalRef(const jobject object);
  void deleteGlobalRef(const jobject object);
  void deleteGlobalRefSafe(const jobject object);

private:
  jmethodID findMethod(const Jvm::JClass& clazz,
                       const std::string& name,
                       const Jvm::JClass& returnType,
                       const std::vector<Jvm::JClass> argTypes);

  template <typename T>
  T invokeV(const jobject receiver, const jmethodID id, va_list args);

  void attachDaemon();
  void attach();
  void detach();

  JavaVM* jvm;
  JNIEnv* env;

  JClass* _stringClass;
};


template <>
void Jvm::invoke<void>(const jobject receiver, const JMethod& method, ...);


template <typename T>
T Jvm::invoke(const jobject receiver, const JMethod& method, ...)
{
  va_list args;
  va_start(args, method);
  const T result = invokeV<T>(receiver, method.id, args);
  va_end(args);
  return result;
}

} // namespace test
} // namespace internal
} // namespace mesos

#endif // __TESTING_JVM_HPP__
