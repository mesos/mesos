package mesos;

import java.util.Map;
import java.util.Collections;


public class ExecutorInfo {
  public ExecutorInfo(String uri, byte[] data, Map<String, String> params) {
    this.uri = uri;
    this.data = data;
    this.params = params;
  }

  public ExecutorInfo(String uri, byte[] data) {
    this(uri, data, Collections.<String, String>emptyMap());
  }

  // TODO(benh): Implement equals, hashCode, and toString.

  public final String uri;
  public final byte[] data;
  public final Map<String, String> params;
};
