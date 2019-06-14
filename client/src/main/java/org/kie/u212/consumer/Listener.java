package org.kie.u212.consumer;

import java.util.Properties;

import org.kie.remote.RemoteFactHandle;
import org.kie.u212.ClientUtils;
import org.kie.u212.EnvConfig;
import org.kie.u212.core.infra.utils.ConsumerUtils;
import org.kie.u212.model.FactCountMessage;
import org.kie.u212.producer.RemoteKieSessionImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Listener {

    private static Logger logger = LoggerFactory.getLogger(RemoteKieSessionImpl.class);
    private Properties configuration;
    private EnvConfig envConfig;

    public Listener(){
        configuration = ClientUtils.getConfiguration(ClientUtils.CONSUMER_CONF);
        envConfig = EnvConfig.getDefaultEnvConfig();
    }

    public FactCountMessage getFactCount(RemoteFactHandle factHandle){
        return ConsumerUtils.getFactCount(factHandle, envConfig, configuration);
    }

}
