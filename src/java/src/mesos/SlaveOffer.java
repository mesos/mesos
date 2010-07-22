package mesos;

import java.util.Map;


public class SlaveOffer {

  public SlaveOffer(SlaveID slaveId, String host, Map<String, String> params) {
    this.slaveId = slaveId;
    this.host = host;
    this.params = params;
  }

  // TODO(benh): Implement equals, hashCode, and toString.

  public final SlaveID slaveId;
  public final String host;
  public final Map<String, String> params;
};
