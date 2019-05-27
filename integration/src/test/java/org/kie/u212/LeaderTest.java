package org.kie.u212;

import java.util.ConcurrentModificationException;

import org.junit.After;
import org.junit.Before;
import org.kie.KafkaKieServerTest;
import org.kie.u212.core.Bootstrap;
import org.kie.u212.core.infra.election.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LeaderTest {
    private KafkaKieServerTest kafkaServerTest;
    private Logger kafkaLogger = LoggerFactory.getLogger("org.kie.u212.kafkaLogger");
    private final  String TEST_KAFKA_LOGGER_TOPIC = "testevents";
    private final  String TEST_TOPIC = "test";


    @Before
    public  void setUp() throws Exception{
        kafkaServerTest = new KafkaKieServerTest();
        kafkaServerTest.startServer();
        kafkaServerTest.createTopic(TEST_KAFKA_LOGGER_TOPIC);
        kafkaServerTest.createTopic(TEST_TOPIC);
        kafkaServerTest.createTopic(Config.EVENTS_TOPIC);
        kafkaServerTest.createTopic(Config.CONTROL_TOPIC);
        kafkaServerTest.createTopic(Config.SNAPSHOT_TOPIC);
        Bootstrap.startEngine();
        Bootstrap.getRestarter().getCallback().updateStatus(State.LEADER);
    }

    @After
    public  void tearDown(){
        System.out.println("tearDown");
        try {
            Bootstrap.stopEngine();
        }catch (ConcurrentModificationException ex){ }
        kafkaServerTest.deleteTopic(TEST_TOPIC);
        kafkaServerTest.deleteTopic(TEST_KAFKA_LOGGER_TOPIC);
        kafkaServerTest.deleteTopic(Config.EVENTS_TOPIC);
        kafkaServerTest.deleteTopic(Config.CONTROL_TOPIC);
        kafkaServerTest.deleteTopic(Config.SNAPSHOT_TOPIC);
        kafkaServerTest.shutdownServer();
    }


}
