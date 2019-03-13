package org.kie.u212.core;

import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.kie.u212.election.KubernetesLockConfiguration;
import org.kie.u212.election.LeadershipElection;
import org.kie.u212.election.LeadershipElectionImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Core {

  private static final Logger logger = LoggerFactory.getLogger(Core.class);
  private static KubernetesClient kubernetesClient = new DefaultKubernetesClient();
  private static KubernetesLockConfiguration configuration = createKubeConfiguration();
  private static LeadershipElection leadership = new LeadershipElectionImpl(kubernetesClient, configuration);

  public static KubernetesClient getKubeClient() {
    return kubernetesClient;
  }

  public static KubernetesLockConfiguration getKubernetesLockConfiguration() {
    return configuration;
  }

  private static KubernetesLockConfiguration createKubeConfiguration() {
    String podName = System.getenv("HOSTNAME");
    KubernetesLockConfiguration configuration = new KubernetesLockConfiguration();
    configuration.setPodName(podName);
    return configuration;
  }

  public static LeadershipElection getLeadershipElection() {
    return leadership;
  }
}

