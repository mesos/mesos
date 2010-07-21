#include <iostream>

#include <map>
#include <string>
#include <vector>

#include "fatal.hpp"
#include "foreach.hpp"
#include "nexus_sched.hpp"

#include "mesos_MesosSchedulerDriver.h"

using std::map;
using std::string;
using std::vector;

using namespace nexus;


template <typename T>
T construct(JNIEnv *env, jobject jobj);


template <typename T>
jobject convert(JNIEnv *env, const T &t);


template <>
string construct(JNIEnv *env, jobject jobj)
{
  string s;
  jstring jstr = (jstring) jobj;
  const char *str = (const char *) env->GetStringUTFChars(jstr, NULL);
  s = str;
  env->ReleaseStringUTFChars(jstr, str);
  return s;
}


template <>
bytes construct(JNIEnv *env, jobject jobj)
{
  jbyteArray jarray = (jbyteArray) jobj;
  const jsize length = env->GetArrayLength(jarray);
  jbyte *array = env->GetByteArrayElements(jarray, NULL);
  bytes data(array, length);
  env->ReleaseByteArrayElements(jarray, array, NULL);
  return data;
}


template <>
map<string, string> construct(JNIEnv *env, jobject jobj)
{
  map<string, string> result;

  jclass clazz = env->GetObjectClass(jobj);

  // Set entrySet = map.entrySet();
  jmethodID entrySet = env->GetMethodID(clazz, "entrySet", "()Ljava/util/Set;");
  jobject jentrySet = env->CallObjectMethod(jobj, entrySet);

  clazz = env->GetObjectClass(jentrySet);

  // Iterator iterator = entrySet.iterator();
  jmethodID iterator = env->GetMethodID(clazz, "iterator", "()Ljava/util/Iterator;");
  jobject jiterator = env->CallObjectMethod(jentrySet, iterator);

  clazz = env->GetObjectClass(jiterator);

  // while (iterator.hasNext()) {
  jmethodID hasNext = env->GetMethodID(clazz, "hasNext", "()Z");

  jmethodID next = env->GetMethodID(clazz, "next", "()Ljava/lang/Object;");

  while (env->CallBooleanMethod(jiterator, hasNext)) {
    // Map.Entry entry = iterator.next();
    jobject jentry = env->CallObjectMethod(jiterator, next);

    clazz = env->GetObjectClass(jentry);

    // String key = entry.getKey();
    jmethodID getKey = env->GetMethodID(clazz, "getKey", "()Ljava/lang/Object;");
    jobject jkey = env->CallObjectMethod(jentry, getKey);

    // String value = entry.getValue();
    jmethodID getValue = env->GetMethodID(clazz, "getValue", "()Ljava/lang/Object;");
    jobject jvalue = env->CallObjectMethod(jentry, getValue);

    const string& key = construct<string>(env, jkey);
    const string& value = construct<string>(env, jvalue);

    result[key] = value;
  }

  return result;
}


template <>
FrameworkID construct(JNIEnv *env, jobject jobj)
{
  jclass clazz = env->GetObjectClass(jobj);
  jfieldID id = env->GetFieldID(clazz, "s", "Ljava/lang/String;");
  jstring jstr = (jstring) env->GetObjectField(jobj, id);
  return FrameworkID(construct<string>(env, jstr));
}


template <>
TaskID construct(JNIEnv *env, jobject jobj)
{
  jclass clazz = env->GetObjectClass(jobj);
  jfieldID id = env->GetFieldID(clazz, "i", "I");
  jint jtaskId = env->GetIntField(jobj, id);
  TaskID taskId = jtaskId;
  return taskId;
}


template <>
SlaveID construct(JNIEnv *env, jobject jobj)
{
  jclass clazz = env->GetObjectClass(jobj);
  jfieldID id = env->GetFieldID(clazz, "s", "Ljava/lang/String;");
  jstring jstr = (jstring) env->GetObjectField(jobj, id);
  return SlaveID(construct<string>(env, jstr));
}


template <>
OfferID construct(JNIEnv *env, jobject jobj)
{
  jclass clazz = env->GetObjectClass(jobj);
  jfieldID id = env->GetFieldID(clazz, "s", "Ljava/lang/String;");
  jstring jstr = (jstring) env->GetObjectField(jobj, id);
  return OfferID(construct<string>(env, jstr));
}


template <>
TaskState construct(JNIEnv *env, jobject jobj)
{
  jclass clazz = env->GetObjectClass(jobj);

  jmethodID name = env->GetMethodID(clazz, "name", "()Ljava/lang/String;");
  jstring jstr = (jstring) env->CallObjectMethod(jobj, name);

  const string& str = construct<string>(env, jstr);

  if (str == "TASK_STARTING") {
    return TASK_STARTING;
  } else if (str == "TASK_RUNNING") {
    return TASK_RUNNING;
  } else if (str == "TASK_FINISHED") {
    return TASK_FINISHED;
  } else if (str == "TASK_FAILED") {
    return TASK_FAILED;
  } else if (str == "TASK_KILLED") {
    return TASK_KILLED;
  } else if (str == "TASK_LOST") {
    return TASK_LOST;
  }

  fatal("Bad enum value while converting between Java and C++.");
}


template <>
TaskDescription construct(JNIEnv *env, jobject jobj)
{
  TaskDescription desc;

  jclass clazz = env->GetObjectClass(jobj);

  jfieldID taskId = env->GetFieldID(clazz, "taskId", "Lmesos/TaskID;");
  jobject jtaskId = env->GetObjectField(jobj, taskId);
  desc.taskId = construct<TaskID>(env, jtaskId);

  jfieldID slaveId = env->GetFieldID(clazz, "slaveId", "Lmesos/SlaveID;");
  jobject jslaveId = env->GetObjectField(jobj, slaveId);
  desc.slaveId = construct<SlaveID>(env, jslaveId);

  jfieldID name = env->GetFieldID(clazz, "name", "Ljava/lang/String;");
  jstring jstr = (jstring) env->GetObjectField(jobj, name);
  desc.name = construct<string>(env, jstr);

  jfieldID params = env->GetFieldID(clazz, "params", "Ljava/util/Map;");
  jobject jparams = env->GetObjectField(jobj, params);
  desc.params = construct< map<string, string> >(env, jparams);

  jfieldID data = env->GetFieldID(clazz, "data", "[B");
  jobject jdata = env->GetObjectField(jobj, data);
  desc.data = construct<bytes>(env, (jbyteArray) jdata);

  return desc;
}


template <>
TaskStatus construct(JNIEnv *env, jobject jobj)
{
  TaskStatus status;

  jclass clazz = env->GetObjectClass(jobj);

  jfieldID taskId = env->GetFieldID(clazz, "taskId", "Lmesos/TaskID;");
  jobject jtaskId = env->GetObjectField(jobj, taskId);
  status.taskId = construct<TaskID>(env, jtaskId);

  jfieldID state = env->GetFieldID(clazz, "state", "Lmesos/TaskState;");
  jobject jstate = env->GetObjectField(jobj, state);
  status.state = construct<TaskState>(env, jstate);

  jfieldID data = env->GetFieldID(clazz, "data", "[B");
  jobject jdata = env->GetObjectField(jobj, data);
  status.data = construct<bytes>(env, jdata);

  return status;
}


template <>
FrameworkMessage construct(JNIEnv *env, jobject jobj)
{
  FrameworkMessage message;

  jclass clazz = env->GetObjectClass(jobj);

  jfieldID slaveId = env->GetFieldID(clazz, "slaveId", "Lmesos/SlaveID;");
  jobject jslaveId = env->GetObjectField(jobj, slaveId);
  message.slaveId = construct<SlaveID>(env, jslaveId);

  jfieldID taskId = env->GetFieldID(clazz, "taskId", "Lmesos/TaskID;");
  jobject jtaskId = env->GetObjectField(jobj, taskId);
  message.taskId = construct<TaskID>(env, jtaskId);

  jfieldID data = env->GetFieldID(clazz, "data", "[B");
  jobject jdata = env->GetObjectField(jobj, data);
  message.data = construct<bytes>(env, jdata);

  return message;
}


template <>
ExecutorInfo construct(JNIEnv *env, jobject jobj)
{
  ExecutorInfo execInfo;

  jclass clazz = env->GetObjectClass(jobj);

  jfieldID uri = env->GetFieldID(clazz, "uri", "Ljava/lang/String;");
  jobject juri = env->GetObjectField(jobj, uri);
  execInfo.uri = construct<string>(env, (jstring) juri);

  jfieldID data = env->GetFieldID(clazz, "data", "[B");
  jobject jdata = env->GetObjectField(jobj, data);
  execInfo.data = construct<bytes>(env, jdata);

  jfieldID params = env->GetFieldID(clazz, "params", "Ljava/util/Map;");
  jobject jparams = env->GetObjectField(jobj, params);
  execInfo.params = construct< map<string, string> >(env, jparams);

  return execInfo;
}


template <>
jobject convert(JNIEnv *env, const string &s)
{
  return env->NewStringUTF(s.c_str());
}


template <>
 jobject convert(JNIEnv *env, const bytes &data)
  {
    jbyteArray jarray = env->NewByteArray(data.size());
    env->SetByteArrayRegion(jarray, 0, data.size(), (jbyte *) data.data());
    return jarray;
  }


template <>
 jobject convert(JNIEnv *env, const map<string, string> &m)
  {
    // HashMap m = new HashMap();
    jclass clazz = env->FindClass("java/util/HashMap");

    jmethodID _init_ = env->GetMethodID(clazz, "<init>", "()V");
    jobject jm = env->NewObject(clazz, _init_);

    jmethodID put = env->GetMethodID(clazz, "put",
      "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;");
    
    // Loop through C++ map and add each pair to the Java map.
    foreachpair (const string &key, const string &value, m) {
      jobject jkey = convert<string>(env, key);
      jobject jvalue = convert<string>(env, value);
      env->CallObjectMethod(jm, put, jkey, jvalue);
    }

    return jm;
  }


template <>
 jobject convert(JNIEnv *env, const FrameworkID &frameworkId)
  {
    // String id = ...;
    jobject jid = convert<string>(env, frameworkId);

    // FrameworkID frameworkId = new FrameworkID(id);
    jclass clazz = env->FindClass("mesos/FrameworkID");

    jmethodID _init_ =
      env->GetMethodID(clazz, "<init>", "(Ljava/lang/String;)V");

    jobject jframeworkId = env->NewObject(clazz, _init_, jid);

    return jframeworkId;
  }


template <>
 jobject convert(JNIEnv *env, const TaskID &taskId)
  {
    // int id = ...;
    jint jid = taskId;

    // TaskID taskId = new TaskID(id);
    jclass clazz = env->FindClass("mesos/TaskID");

    jmethodID _init_ = env->GetMethodID(clazz, "<init>", "(I)V");

    jobject jtaskId = env->NewObject(clazz, _init_, jid);

    return jtaskId;
  }


template <>
 jobject convert(JNIEnv *env, const SlaveID &slaveId)
  {
    // String id = ...;
    jobject jid = convert<string>(env, slaveId);

    // SlaveID taskId = new SlaveID(id);
    jclass clazz = env->FindClass("mesos/SlaveID");

    jmethodID _init_ =
      env->GetMethodID(clazz, "<init>", "(Ljava/lang/String;)V");

    jobject jslaveId = env->NewObject(clazz, _init_, jid);

    return jslaveId;
  }


template <>
 jobject convert(JNIEnv *env, const OfferID &offerId)
  {
    // String id = ...;
    jobject jid = convert<string>(env, offerId);

    // OfferID taskId = new OfferID(id);
    jclass clazz = env->FindClass("mesos/OfferID");

    jmethodID _init_ =
      env->GetMethodID(clazz, "<init>", "(Ljava/lang/String;)V");

    jobject jofferId = env->NewObject(clazz, _init_, jid);

    return jofferId;
  }


template <>
 jobject convert(JNIEnv *env, const TaskState &state)
  {
    jclass clazz = env->FindClass("mesos/TaskState");

    const char *name = NULL;

    if (state == TASK_STARTING) {
      name = "TASK_STARTING";
    } else if (state == TASK_RUNNING) {
      name = "TASK_RUNNING";
    } else if (state == TASK_FINISHED) {
      name = "TASK_FINISHED";
    } else if (state == TASK_FAILED) {
      name = "TASK_FAILED";
    } else if (state == TASK_KILLED) {
      name = "TASK_KILLED";
    } else if (state == TASK_LOST) {
      name = "TASK_LOST";
    } else {
      fatal("bad enum value while converting between C++ and Java.");
    }

    jfieldID TASK_X = env->GetStaticFieldID(clazz, name, "Lmesos/TaskState;");
    return env->GetStaticObjectField(clazz, TASK_X);
  }


template <>
 jobject convert(JNIEnv *env, const TaskDescription &desc)
  {
    jobject jtaskId = convert<TaskID>(env, desc.taskId);
    jobject jslaveId = convert<SlaveID>(env, desc.slaveId);
    jobject jname = convert<string>(env, desc.name);
    jobject jparams = convert< map<string, string> >(env, desc.params);
    jobject jdata = convert<bytes>(env, desc.data);

    // ... desc = TaskDescription(taskId, slaveId, params, data);
    jclass clazz = env->FindClass("mesos/TaskDescription");

    jmethodID _init_ = env->GetMethodID(clazz, "<init>",
      "(Lmesos/TaskID;Lmesos/SlaveID;Ljava/lang/String;Ljava/util/Map;[B)V");

    jobject jdesc = env->NewObject(clazz, _init_, jtaskId, jslaveId,
                                   jname, jparams, jdata);

    return jdesc;
  }


template <>
 jobject convert(JNIEnv *env, const TaskStatus &status)
  {
    jobject jtaskId = convert<TaskID>(env, status.taskId);
    jobject jstate = convert<TaskState>(env, status.state);
    jobject jdata = convert<bytes>(env, status.data);

    // ... status = TaskStatus(taskId, state, data);
    jclass clazz = env->FindClass("mesos/TaskStatus");

    jmethodID _init_ = env->GetMethodID(clazz, "<init>",
      "(Lmesos/TaskID;Lmesos/TaskState;[B)V");

    jobject jstatus = env->NewObject(clazz, _init_, jtaskId, jstate, jdata);

    return jstatus;
  }


template <>
 jobject convert(JNIEnv *env, const FrameworkMessage &message)
  {
    jobject jslaveId = convert<SlaveID>(env, message.slaveId);
    jobject jtaskId = convert<TaskID>(env, message.taskId);
    jobject jdata = convert<bytes>(env, message.data);

    // ... message = FrameworkMessage(slaveId, taskId, data);
    jclass clazz = env->FindClass("mesos/FrameworkMessage");

    jmethodID _init_ = env->GetMethodID(clazz, "<init>",
      "(Lmesos/SlaveID;Lmesos/TaskID;[B)V");

    jobject jmessage = env->NewObject(clazz, _init_, jslaveId, jtaskId, jdata);

    return jmessage;
  }


template <>
 jobject convert(JNIEnv *env, const ExecutorInfo &execInfo)
  {
    jobject juri = convert<string>(env, execInfo.uri);
    jobject jdata = convert<bytes>(env, execInfo.data);
    jobject jparams = convert< map<string, string> >(env, execInfo.params);

    // ... execInfo = ExecutorInfo(uri, data, params);
    jclass clazz = env->FindClass("mesos/ExecutorInfo");

    jmethodID _init_ = env->GetMethodID(clazz, "<init>",
      "(Ljava/lang/String;[BLjava/util/Map;)V");

    jobject jexecInfo = env->NewObject(clazz, _init_, juri, jdata, jparams);

    return jexecInfo;
  }


class JNIScheduler : public Scheduler
{
public:
  JNIScheduler(JNIEnv *_env, jobject _jdriver)
    : jvm(NULL), env(_env), jdriver(_jdriver)
  {
    env->GetJavaVM(&jvm);
  }

  virtual ~JNIScheduler() {}

  virtual string getFrameworkName(SchedulerDriver*);
  virtual ExecutorInfo getExecutorInfo(SchedulerDriver*);
  virtual void registered(SchedulerDriver*, FrameworkID);
  virtual void resourceOffer(SchedulerDriver*, OfferID,
                             const vector<SlaveOffer>&);
  virtual void offerRescinded(SchedulerDriver*, OfferID);
  virtual void statusUpdate(SchedulerDriver*, const TaskStatus&);
  virtual void frameworkMessage(SchedulerDriver*, const FrameworkMessage&);
  virtual void slaveLost(SchedulerDriver*, SlaveID);
  virtual void error(SchedulerDriver*, int code, const string& message);

  JavaVM *jvm;
  JNIEnv *env;
  jobject jdriver;
};


string JNIScheduler::getFrameworkName(SchedulerDriver* driver)
{
  jvm->AttachCurrentThread((void **) &env, NULL);

  string name;

  jclass clazz = env->GetObjectClass(jdriver);

  jfieldID sched = env->GetFieldID(clazz, "sched", "Lmesos/Scheduler;");
  jobject jsched = env->GetObjectField(jdriver, sched);

  clazz = env->GetObjectClass(jsched);

  // String name = sched.getFrameworkName();
  jmethodID getFrameworkName =
    env->GetMethodID(clazz, "getFrameworkName", "(Lmesos/SchedulerDriver;)Ljava/lang/String;");

  env->ExceptionClear();

  jobject jname = env->CallObjectMethod(jsched, getFrameworkName, jdriver);

  if (!env->ExceptionOccurred()) {
    name = construct<string>(env, (jstring) jname);
    jvm->DetachCurrentThread();
  } else {
    env->ExceptionDescribe();
    env->ExceptionClear();
    jvm->DetachCurrentThread();
    driver->stop();
    this->error(driver, -1, "Java exception caught");
  }

  return name;
}


ExecutorInfo JNIScheduler::getExecutorInfo(SchedulerDriver* driver)
{
  jvm->AttachCurrentThread((void **) &env, NULL);

  ExecutorInfo execInfo;

  jclass clazz = env->GetObjectClass(jdriver);

  jfieldID sched = env->GetFieldID(clazz, "sched", "Lmesos/Scheduler;");
  jobject jsched = env->GetObjectField(jdriver, sched);

  clazz = env->GetObjectClass(jsched);

  // ExecutorInfo execInfo = sched.getExecutorInfo();
  jmethodID getExecutorInfo =
    env->GetMethodID(clazz, "getExecutorInfo", "(Lmesos/SchedulerDriver;)Lmesos/ExecutorInfo;");

  env->ExceptionClear();

  jobject jexecInfo = env->CallObjectMethod(jsched, getExecutorInfo, jdriver);

  if (!env->ExceptionOccurred()) {
    execInfo = construct<ExecutorInfo>(env, jexecInfo);
    jvm->DetachCurrentThread();
  } else {
    env->ExceptionDescribe();
    env->ExceptionClear();
    jvm->DetachCurrentThread();
    driver->stop();
    this->error(driver, -1, "Java exception caught");
  }

  return execInfo;
}


void JNIScheduler::registered(SchedulerDriver* driver, FrameworkID frameworkId)
{
  jvm->AttachCurrentThread((void **) &env, NULL);

  jclass clazz = env->GetObjectClass(jdriver);

  jfieldID sched = env->GetFieldID(clazz, "sched", "Lmesos/Scheduler;");
  jobject jsched = env->GetObjectField(jdriver, sched);

  clazz = env->GetObjectClass(jsched);

  // sched.registered(driver, frameworkId);
  jmethodID registered = env->GetMethodID(clazz, "registered",
    "(Lmesos/SchedulerDriver;Lmesos/FrameworkID;)V");

  jobject jframeworkId = convert<FrameworkID>(env, frameworkId);

  env->ExceptionClear();

  env->CallVoidMethod(jsched, registered, jdriver, jframeworkId);

  if (!env->ExceptionOccurred()) {
    jvm->DetachCurrentThread();
  } else {
    env->ExceptionDescribe();
    env->ExceptionClear();
    jvm->DetachCurrentThread();
    driver->stop();
    this->error(driver, -1, "Java exception caught");
  }
}


void JNIScheduler::resourceOffer(SchedulerDriver* driver, OfferID offerId,
                                 const vector<SlaveOffer>& offers)
{
  jvm->AttachCurrentThread((void **) &env, NULL);

  jclass clazz = env->GetObjectClass(jdriver);

  jfieldID sched = env->GetFieldID(clazz, "sched", "Lmesos/Scheduler;");
  jobject jsched = env->GetObjectField(jdriver, sched);

  clazz = env->GetObjectClass(jsched);

  // sched.resourceOffer(driver, offerId, offers);
  jmethodID resourceOffer = env->GetMethodID(clazz, "resourceOffer",
    "(Lmesos/SchedulerDriver;Lmesos/OfferID;Ljava/util/Collection;)V");

  jobject jofferId = convert<OfferID>(env, offerId);

  // Vector offers = new Vector();
  clazz = env->FindClass("java/util/Vector");

  jmethodID _init_ = env->GetMethodID(clazz, "<init>", "()V");
  jobject joffers = env->NewObject(clazz, _init_);

  jmethodID add = env->GetMethodID(clazz, "add", "(Ljava/lang/Object;)Z");
    
  // Loop through C++ vector and add each offer to the Java vector.
  foreach (const SlaveOffer &offer, offers) {
    jobject jslaveId = convert<SlaveID>(env, offer.slaveId);
    jobject jhost = convert<string>(env, offer.host);
    jobject jparams = convert< map<string, string> >(env, offer.params);

    // SlaveOffer offer = new SlaveOffer(slaveId, host, params);
    clazz = env->FindClass("mesos/SlaveOffer");

    _init_ = env->GetMethodID(clazz, "<init>",
      "(Lmesos/SlaveID;Ljava/lang/String;Ljava/util/Map;)V");

    jobject joffer = env->NewObject(clazz, _init_, jslaveId, jhost, jparams);
    env->CallBooleanMethod(joffers, add, joffer);
  }

  env->ExceptionClear();

  env->CallVoidMethod(jsched, resourceOffer, jdriver, jofferId, joffers);

  if (!env->ExceptionOccurred()) {
    jvm->DetachCurrentThread();
  } else {
    env->ExceptionDescribe();
    env->ExceptionClear();
    jvm->DetachCurrentThread();
    driver->stop();
    this->error(driver, -1, "Java exception caught");
  }
}


void JNIScheduler::offerRescinded(SchedulerDriver* driver, OfferID offerId)
{
  jvm->AttachCurrentThread((void **) &env, NULL);

  jclass clazz = env->GetObjectClass(jdriver);

  jfieldID sched = env->GetFieldID(clazz, "sched", "Lmesos/Scheduler;");
  jobject jsched = env->GetObjectField(jdriver, sched);

  clazz = env->GetObjectClass(jsched);

  // sched.offerRescinded(driver, offerId);
  jmethodID offerRescinded = env->GetMethodID(clazz, "offerRescinded",
    "(Lmesos/SchedulerDriver;Lmesos/OfferID;)V");

  jobject jofferId = convert<OfferID>(env, offerId);

  env->ExceptionClear();

  env->CallVoidMethod(jsched, offerRescinded, jdriver, jofferId);

  if (!env->ExceptionOccurred()) {
    jvm->DetachCurrentThread();
  } else {
    env->ExceptionDescribe();
    env->ExceptionClear();
    jvm->DetachCurrentThread();
    driver->stop();
    this->error(driver, -1, "Java exception caught");
  }
}


void JNIScheduler::statusUpdate(SchedulerDriver* driver, const TaskStatus& status)
{
  jvm->AttachCurrentThread((void **) &env, NULL);

  jclass clazz = env->GetObjectClass(jdriver);

  jfieldID sched = env->GetFieldID(clazz, "sched", "Lmesos/Scheduler;");
  jobject jsched = env->GetObjectField(jdriver, sched);

  clazz = env->GetObjectClass(jsched);

  // sched.statusUpdate(driver, status);
  jmethodID statusUpdate = env->GetMethodID(clazz, "statusUpdate",
    "(Lmesos/SchedulerDriver;Lmesos/TaskStatus;)V");

  jobject jstatus = convert<TaskStatus>(env, status);

  env->ExceptionClear();

  env->CallVoidMethod(jsched, statusUpdate, jdriver, jstatus);

  if (!env->ExceptionOccurred()) {
    jvm->DetachCurrentThread();
  } else {
    env->ExceptionDescribe();
    env->ExceptionClear();
    jvm->DetachCurrentThread();
    driver->stop();
    this->error(driver, -1, "Java exception caught");
  }
}


void JNIScheduler::frameworkMessage(SchedulerDriver* driver, const FrameworkMessage& message)
{
  jvm->AttachCurrentThread((void **) &env, NULL);

  jclass clazz = env->GetObjectClass(jdriver);

  jfieldID sched = env->GetFieldID(clazz, "sched", "Lmesos/Scheduler;");
  jobject jsched = env->GetObjectField(jdriver, sched);

  clazz = env->GetObjectClass(jsched);

  // sched.frameworkMessage(driver, message);
  jmethodID frameworkMessage = env->GetMethodID(clazz, "frameworkMessage",
    "(Lmesos/SchedulerDriver;Lmesos/FrameworkMessage;)V");

  jobject jmessage = convert<FrameworkMessage>(env, message);

  env->ExceptionClear();

  env->CallVoidMethod(jsched, frameworkMessage, jdriver, jmessage);

  if (!env->ExceptionOccurred()) {
    jvm->DetachCurrentThread();
  } else {
    env->ExceptionDescribe();
    env->ExceptionClear();
    jvm->DetachCurrentThread();
    driver->stop();
    this->error(driver, -1, "Java exception caught");
  }
}


void JNIScheduler::slaveLost(SchedulerDriver* driver, SlaveID slaveId)
{
  jvm->AttachCurrentThread((void **) &env, NULL);

  jclass clazz = env->GetObjectClass(jdriver);

  jfieldID sched = env->GetFieldID(clazz, "sched", "Lmesos/Scheduler;");
  jobject jsched = env->GetObjectField(jdriver, sched);

  clazz = env->GetObjectClass(jsched);

  // sched.slaveLost(driver, slaveId);
  jmethodID slaveLost = env->GetMethodID(clazz, "slaveLost",
    "(Lmesos/SchedulerDriver;Lmesos/SlaveID;)V");

  jobject jslaveId = convert<SlaveID>(env, slaveId);

  env->ExceptionClear();

  env->CallVoidMethod(jsched, slaveLost, jdriver, jslaveId);

  if (!env->ExceptionOccurred()) {
    jvm->DetachCurrentThread();
  } else {
    env->ExceptionDescribe();
    env->ExceptionClear();
    jvm->DetachCurrentThread();
    driver->stop();
    this->error(driver, -1, "Java exception caught");
  }
}


void JNIScheduler::error(SchedulerDriver* driver, int code, const string& message)
{
  jvm->AttachCurrentThread((void **) &env, NULL);

  jclass clazz = env->GetObjectClass(jdriver);

  jfieldID sched = env->GetFieldID(clazz, "sched", "Lmesos/Scheduler;");
  jobject jsched = env->GetObjectField(jdriver, sched);

  clazz = env->GetObjectClass(jsched);

  // sched.error(driver, code, message);
  jmethodID error = env->GetMethodID(clazz, "error",
    "(Lmesos/SchedulerDriver;ILjava/lang/String;)V");

  jint jcode = code;
  jobject jmessage = convert<string>(env, message);

  env->ExceptionClear();

  env->CallVoidMethod(jsched, error, jdriver, jcode, jmessage);

  if (!env->ExceptionOccurred()) {
    jvm->DetachCurrentThread();
  } else {
    env->ExceptionDescribe();
    env->ExceptionClear();
    jvm->DetachCurrentThread();
    driver->stop();
    // Don't call error recursively here!
  }
}


#ifdef __cplusplus
extern "C" {
#endif

/*
 * Class:     mesos_MesosSchedulerDriver
 * Method:    initialize
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_mesos_MesosSchedulerDriver_initialize
  (JNIEnv *env, jobject thiz)
{
  jclass clazz = env->GetObjectClass(thiz);

  // Create a global reference to the MesosSchedulerDriver instance.
  jobject jdriver = env->NewWeakGlobalRef(thiz);

  // Create the C++ scheduler and initialize the __sched variable.
  JNIScheduler *sched = new JNIScheduler(env, jdriver);

  jfieldID __sched = env->GetFieldID(clazz, "__sched", "J");
  env->SetLongField(thiz, __sched, (jlong) sched);

  // Get out the url passed into the constructor.
  jfieldID url = env->GetFieldID(clazz, "url", "Ljava/lang/String;");
  jobject jstr = env->GetObjectField(thiz, url);
  
  // Create the C++ driver and initialize the __driver variable.
  NexusSchedulerDriver *driver =
    new NexusSchedulerDriver(sched, construct<string>(env, jstr));

  jfieldID __driver = env->GetFieldID(clazz, "__driver", "J");
  env->SetLongField(thiz, __driver, (jlong) driver);
}


/*
 * Class:     mesos_MesosSchedulerDriver
 * Method:    finalize
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_mesos_MesosSchedulerDriver_finalize
  (JNIEnv *env, jobject thiz)
{
  jclass clazz = env->GetObjectClass(thiz);

  jfieldID __driver = env->GetFieldID(clazz, "__driver", "J");
  NexusSchedulerDriver *driver = (NexusSchedulerDriver *)
    env->GetLongField(thiz, __driver);

  // Call stop just in case.
  driver->stop();
  driver->join();

  delete driver;

  jfieldID __sched = env->GetFieldID(clazz, "__sched", "J");
  JNIScheduler *sched = (JNIScheduler *)
    env->GetLongField(thiz, __sched);

  env->DeleteWeakGlobalRef(sched->jdriver);

  delete sched;
}


/*
 * Class:     mesos_MesosSchedulerDriver
 * Method:    start
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_mesos_MesosSchedulerDriver_start
  (JNIEnv *env, jobject thiz)
{
  jclass clazz = env->GetObjectClass(thiz);

  jfieldID __driver = env->GetFieldID(clazz, "__driver", "J");
  NexusSchedulerDriver *driver = (NexusSchedulerDriver *)
    env->GetLongField(thiz, __driver);

  return driver->start();
}


/*
 * Class:     mesos_MesosSchedulerDriver
 * Method:    stop
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_mesos_MesosSchedulerDriver_stop
  (JNIEnv *env, jobject thiz)
{
  jclass clazz = env->GetObjectClass(thiz);

  jfieldID __driver = env->GetFieldID(clazz, "__driver", "J");
  NexusSchedulerDriver *driver = (NexusSchedulerDriver *)
    env->GetLongField(thiz, __driver);

  return driver->stop();
}


/*
 * Class:     mesos_MesosSchedulerDriver
 * Method:    join
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_mesos_MesosSchedulerDriver_join
  (JNIEnv *env, jobject thiz)
{
  jclass clazz = env->GetObjectClass(thiz);

  jfieldID __driver = env->GetFieldID(clazz, "__driver", "J");
  NexusSchedulerDriver *driver = (NexusSchedulerDriver *)
    env->GetLongField(thiz, __driver);

  return driver->join();
}


/*
 * Class:     mesos_MesosSchedulerDriver
 * Method:    sendFrameworkMessage
 * Signature: (Lmesos/FrameworkMessage;)I
 */
JNIEXPORT jint JNICALL Java_mesos_MesosSchedulerDriver_sendFrameworkMessage
  (JNIEnv *env, jobject thiz, jobject jmessage)
{
  // Construct a C++ FrameworkMessage from the Java FrameworkMessage.
  FrameworkMessage message = construct<FrameworkMessage>(env, jmessage);

  // Now invoke the underlying driver.
  jclass clazz = env->GetObjectClass(thiz);

  jfieldID __driver = env->GetFieldID(clazz, "__driver", "J");
  NexusSchedulerDriver *driver = (NexusSchedulerDriver *)
    env->GetLongField(thiz, __driver);

  return driver->sendFrameworkMessage(message);
}


/*
 * Class:     mesos_MesosSchedulerDriver
 * Method:    killTask
 * Signature: (Lmesos/TaskID;)I
 */
JNIEXPORT jint JNICALL Java_mesos_MesosSchedulerDriver_killTask
  (JNIEnv *env, jobject thiz, jobject jtaskId)
{
  // Construct a C++ TaskID from the Java TaskId.
  TaskID taskId = construct<TaskID>(env, jtaskId);

  // Now invoke the underlying driver.
  jclass clazz = env->GetObjectClass(thiz);

  jfieldID __driver = env->GetFieldID(clazz, "__driver", "J");
  NexusSchedulerDriver *driver = (NexusSchedulerDriver *)
    env->GetLongField(thiz, __driver);

  return driver->killTask(taskId);
}


/*
 * Class:     mesos_MesosSchedulerDriver
 * Method:    replyToOffer
 * Signature: (Lmesos/OfferID;[Lmesos/TaskDescription;Ljava/util/Map;)I
 */
JNIEXPORT jint JNICALL Java_mesos_MesosSchedulerDriver_replyToOffer
  (JNIEnv *env, jobject thiz, jobject jofferId, jobject jtasks, jobject jparams)
{
  // Construct a C++ OfferID from the Java OfferID.
  OfferID offerId = construct<OfferID>(env, jofferId);

  // Construct a C++ TaskDescription from each Java TaskDescription.
  vector<TaskDescription> tasks;

  jclass clazz = env->GetObjectClass(jtasks);

  // Iterator iterator = tasks.iterator();
  jmethodID iterator = env->GetMethodID(clazz, "iterator", "()Ljava/util/Iterator;");
  jobject jiterator = env->CallObjectMethod(jtasks, iterator);

  clazz = env->GetObjectClass(jiterator);

  // while (iterator.hasNext()) {
  jmethodID hasNext = env->GetMethodID(clazz, "hasNext", "()Z");

  jmethodID next = env->GetMethodID(clazz, "next", "()Ljava/lang/Object;");

  while (env->CallBooleanMethod(jiterator, hasNext)) {
    // Object task = iterator.next();
    jobject jtask = env->CallObjectMethod(jiterator, next);
    TaskDescription task = construct<TaskDescription>(env, jtask);
    tasks.push_back(task);
  }

  // Construct a C++ map<string, string> from the Java Map<String, String>.
  map<string, string> params = construct< map<string, string> >(env, jparams);

  // Now invoke the underlying driver.
  clazz = env->GetObjectClass(thiz);

  jfieldID __driver = env->GetFieldID(clazz, "__driver", "J");
  NexusSchedulerDriver *driver = (NexusSchedulerDriver *)
    env->GetLongField(thiz, __driver);

  return driver->replyToOffer(offerId, tasks, params);
}


/*
 * Class:     mesos_MesosSchedulerDriver
 * Method:    reviveOffers
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_mesos_MesosSchedulerDriver_reviveOffers
  (JNIEnv *env, jobject thiz)
{
  jclass clazz = env->GetObjectClass(thiz);

  jfieldID __driver = env->GetFieldID(clazz, "__driver", "J");
  NexusSchedulerDriver *driver = (NexusSchedulerDriver *)
    env->GetLongField(thiz, __driver);

  return driver->reviveOffers();
}


/*
 * Class:     mesos_MesosSchedulerDriver
 * Method:    sendHints
 * Signature: (Ljava/util/Map;)I
 */
JNIEXPORT jint JNICALL Java_mesos_MesosSchedulerDriver_sendHints
  (JNIEnv *env, jobject thiz, jobject jhints)
{
  // Construct a C++ map<string, string> from the Java Map<String, String>.
  map<string, string> hints = construct< map<string, string> >(env, jhints);

  // Now invoke the underlying driver.
  jclass clazz = env->GetObjectClass(thiz);

  jfieldID __driver = env->GetFieldID(clazz, "__driver", "J");
  NexusSchedulerDriver *driver = (NexusSchedulerDriver *)
    env->GetLongField(thiz, __driver);

  return driver->sendHints(hints);
}

#ifdef __cplusplus
}
#endif
