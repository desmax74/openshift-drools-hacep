package org.kie.hacep.core.infra.utils;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Assert;
import org.junit.Test;
import org.kie.hacep.EnvConfig;
import org.kie.hacep.core.infra.DefaultSessionSnapShooter;
import org.kie.hacep.core.infra.SessionSnapshooter;
import org.kie.hacep.core.infra.SnapshotInfos;

public class SnapshotOnDemandUtilsTest {

    @Test(expected = org.apache.kafka.common.KafkaException.class)
    public void askAKAfkaConsumerWithoutServerUpTest(){
        EnvConfig config = EnvConfig.getDefaultEnvConfig();
        config.local(false);
        config.underTest(false);
        KafkaConsumer consumer = SnapshotOnDemandUtils.getConfiguredSnapshotConsumer(config);
        Assert.assertNull(consumer);
    }

    @Test(expected = org.apache.kafka.common.KafkaException.class)
    public void askASnapshogtWithoutServerUTest(){
        EnvConfig config = EnvConfig.getDefaultEnvConfig();
        config.local(false);
        config.underTest(false);
        SessionSnapshooter sessionSnapshooter = new DefaultSessionSnapShooter(config);
        SnapshotInfos infos = SnapshotOnDemandUtils.askASnapshotOnDemand(config, sessionSnapshooter );
        Assert.assertNull(infos);
    }
}
