package org.kie.u212.election;

public interface LeadershipElection {

  void start() throws Exception;

  void stop() throws Exception;

  boolean amITheLeader();
}
