#include <jni.h>
#include <stdarg.h>

#include <glog/logging.h>

#include <map>
#include <memory>
#include <sstream>
#include <vector>

#include "common/hashmap.hpp"
#include "common/foreach.hpp"
#include "common/utils.hpp"

#include "tests/jvm.hpp"


namespace mesos {
namespace internal {
namespace test {

jmethodID Jvm::findMethod(const Jvm::JClass& clazz,
                          const std::string& name,
                          const Jvm::JClass& returnType,
                          const std::vector<Jvm::JClass> argTypes)
{
  std::ostringstream signature;
  signature << "(";
  std::vector<Jvm::JClass>::iterator args;
  foreach (Jvm::JClass type, argTypes) {
    signature << type.signature();
  }
  signature << ")" << returnType.signature();

  LOG(INFO) << "looking up method " << signature;
  jmethodID id = env->GetMethodID(clazz.clazz,
                                  name.c_str(),
                                  signature.str().c_str());

  // TODO(John Sirois): consider CHECK -> return Option if re-purposing this
  // code outside of tests.
  CHECK(id != NULL);
  return id;
}


Jvm::ConstructorFinder::ConstructorFinder(
    const Jvm::JClass& _type)
    : type(_type),
      parameters() {}


Jvm::ConstructorFinder&
Jvm::ConstructorFinder::parameter(const Jvm::JClass& type)
{
  parameters.push_back(type);
  return *this;
}


Jvm::JConstructor Jvm::findConstructor(const ConstructorFinder& signature)
{
  jmethodID id =
      findMethod(signature.type, "<init>", voidClass, signature.parameters);
  return Jvm::JConstructor(signature.type, id);
}


Jvm::JConstructor::JConstructor(const JConstructor& other) : clazz(other.clazz),
                                                             id(other.id) {}


Jvm::JConstructor::JConstructor(const JClass& _clazz,
                                const jmethodID _id) : clazz(_clazz),
                                                       id(_id) {}


jobject Jvm::invoke(const JConstructor& ctor, ...)
{
  va_list args;
  va_start(args, ctor);
  const jobject result = env->NewObjectV(ctor.clazz.clazz, ctor.id, args);
  va_end(args);
  CHECK(result != NULL);
  return result;
}


Jvm::MethodFinder::MethodFinder(
    const Jvm::JClass& _clazz,
    const std::string& _name)
    : clazz(_clazz),
      name(_name),
      parameters() {}


Jvm::MethodFinder& Jvm::MethodFinder::parameter(const JClass& type)
{
  parameters.push_back(type);
  return *this;
}


Jvm::MethodSignature Jvm::MethodFinder::returns(const JClass& returnType) const
{
  return Jvm::MethodSignature(clazz, name, returnType, parameters);
}


Jvm::JMethod Jvm::findMethod(const MethodSignature& signature)
{
  jmethodID id = findMethod(signature.clazz,
                            signature.name,
                            signature.returnType,
                            signature.parameters);
  return Jvm::JMethod(id);
}


Jvm::MethodSignature::MethodSignature(const MethodSignature& other) :
    clazz(other.clazz),
    name(other.name),
    returnType(other.returnType),
    parameters(other.parameters) {}


Jvm::MethodSignature::MethodSignature(const JClass& _clazz,
                                      const std::string& _name,
                                      const JClass& _returnType,
                                      const std::vector<JClass>& _parameters) :
    clazz(_clazz),
    name(_name),
    returnType(_returnType),
    parameters(_parameters) {}


Jvm::JMethod::JMethod(const JMethod& other) : id(other.id) {}


Jvm::JMethod::JMethod(const jmethodID _id) : id(_id) {}


template <>
jobject Jvm::invokeV<jobject>(const jobject receiver,
                              const jmethodID id,
                              va_list args)
{
  const jobject result = env->CallObjectMethodV(receiver, id, args);
  CHECK(result != NULL);
  return result;
}


template<>
void Jvm::invokeV<void>(const jobject receiver,
                        const jmethodID id,
                        va_list args)
{
  env->CallVoidMethodV(receiver, id, args);
}


template <>
bool Jvm::invokeV<bool>(const jobject receiver,
                        const jmethodID id,
                        va_list args)
{
  return env->CallBooleanMethodV(receiver, id, args);
}


template <>
char Jvm::invokeV<char>(const jobject receiver,
                        const jmethodID id,
                        va_list args)
{
  return env->CallCharMethodV(receiver, id, args);
}


template <>
short Jvm::invokeV<short>(const jobject receiver,
                          const jmethodID id,
                          va_list args)
{
  return env->CallShortMethodV(receiver, id, args);
}


template <>
int Jvm::invokeV<int>(const jobject receiver,
                      const jmethodID id,
                      va_list args)
{
  return env->CallIntMethodV(receiver, id, args);
}


template <>
long Jvm::invokeV<long>(const jobject receiver,
                        const jmethodID id,
                        va_list args)
{
  return env->CallLongMethodV(receiver, id, args);
}


template <>
float Jvm::invokeV<float>(const jobject receiver,
                          const jmethodID id,
                          va_list args)
{
  return env->CallFloatMethodV(receiver, id, args);
}


template <>
double Jvm::invokeV<double>(const jobject receiver,
                            const jmethodID id,
                            va_list args)
{
  return env->CallDoubleMethodV(receiver, id, args);
}


template <>
void Jvm::invoke<void>(const jobject receiver, const JMethod& method, ...)
{
  va_list args;
  va_start(args, method);
  invokeV<void>(receiver, method.id, args);
  va_end(args);
}


Jvm::JClass::JClass(const JClass& other) : clazz(other.clazz),
                                           nativeName(other.nativeName),
                                           arrayCount(other.arrayCount) {}


Jvm::JClass::JClass(const jclass _clazz,
                    const std::string& _nativeName,
                    int _arrayCount) : clazz(_clazz),
                                       nativeName(_nativeName),
                                       arrayCount(_arrayCount) {}


const Jvm::JClass Jvm::JClass::arrayOf() const
{
  return Jvm::JClass(clazz, nativeName, arrayCount + 1);
}


Jvm::ConstructorFinder Jvm::JClass::constructor() const
{
  return Jvm::ConstructorFinder(*this);
}


Jvm::MethodFinder Jvm::JClass::method(const std::string& name) const
{
  return Jvm::MethodFinder(*this, name);
}


std::string Jvm::JClass::signature() const
{
  if (clazz == NULL) {
    return nativeName;
  }

  std::ostringstream signature;
  for (int i = 0; i < arrayCount; ++i) {
    signature << "[";
  }
  signature << "L" << nativeName << ";";
  return signature.str();
}


Jvm::Jvm(const std::vector<std::string>& options, JNIVersion jniVersion)
  : jvm(NULL),
    env(NULL),
    voidClass(NULL, "V"),
    booleanClass(NULL, "Z"),
    byteClass(NULL, "B"),
    charClass(NULL, "C"),
    shortClass(NULL, "S"),
    intClass(NULL, "I"),
    longClass(NULL, "J"),
    floatClass(NULL, "F"),
    doubleClass(NULL, "D")
{
  JavaVMInitArgs vmArgs;
  vmArgs.version = jniVersion;
  vmArgs.ignoreUnrecognized = false;

  JavaVMOption* opts = new JavaVMOption[options.size()];
  for (int i = 0; i < options.size(); i++) {
    opts[i].optionString = const_cast<char*>(options[i].c_str());
  }
  vmArgs.nOptions = options.size();
  vmArgs.options = opts;

  int result = JNI_CreateJavaVM(&jvm, (void**) &env, &vmArgs);
  CHECK(result != JNI_ERR) << "Failed to create JVM!";

  delete[] opts;

  _stringClass = new Jvm::JClass(findClass("java/lang/String"));
}


Jvm::~Jvm()
{
  delete _stringClass;
  CHECK(0 == jvm->DestroyJavaVM()) << "Failed to destroy JVM";
}


void Jvm::attachDaemon()
{
  jvm->AttachCurrentThreadAsDaemon((void**) &env, NULL);
}


void Jvm::attach()
{
  jvm->AttachCurrentThread((void**) &env, NULL);
}


void Jvm::detach()
{
  jvm->DetachCurrentThread();
}


Jvm::JClass Jvm::findClass(const std::string& name)
{
  jclass clazz = env->FindClass(name.c_str());
  // TODO(John Sirois): consider CHECK -> return Option if re-purposing this
  // code outside of tests.
  CHECK(clazz != NULL);
  return JClass(clazz, name);
}


jobject Jvm::string(const std::string& str)
{
  return env->NewStringUTF(str.c_str());
}


const Jvm::JClass& Jvm::stringClass() const
{
  return *_stringClass;
}


jobject Jvm::newGlobalRef(const jobject object)
{
  return env->NewGlobalRef(object);
}


void Jvm::deleteGlobalRef(const jobject object)
{
  env->DeleteGlobalRef(object);
}


void Jvm::deleteGlobalRefSafe(const jobject object)
{
  if (object != NULL) {
    deleteGlobalRef(object);
  }
}

Jvm::Attach::Attach(Jvm* jvm, bool daemon) : _jvm(jvm)
{
  if (daemon) {
    _jvm->attachDaemon();
  } else {
    _jvm->attach();
  }
}


Jvm::Attach::~Attach()
{
  // TODO(John Sirois): this detaches too early under nested use, attach by a
  // given thread should incr, this should decr and only detach on 0
  _jvm->detach();
}

} // namespace test
} // namespace internal
} // namespace mesos

