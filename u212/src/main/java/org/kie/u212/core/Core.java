/*
 * Copyright 2019 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kie.u212.core;

import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.kie.u212.election.KubernetesLockConfiguration;
import org.kie.u212.election.LeaderElection;
import org.kie.u212.election.LeaderElectionImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Core {

  private static final Logger logger = LoggerFactory.getLogger(Core.class);
  private static KubernetesClient kubernetesClient = new DefaultKubernetesClient();
  private static KubernetesLockConfiguration configuration = createKubeConfiguration();
  private static LeaderElection leadership = new LeaderElectionImpl(kubernetesClient, configuration);

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

  public static LeaderElection getLeadershipElection() {
    return leadership;
  }
}

