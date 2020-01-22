package org.kie.hacep.util;

import java.util.Properties;

import org.kie.remote.message.ControlMessage;

public interface ConsumerUtilsCore {

    public ControlMessage getLastEvent(String topic, Integer pollTimeout);

    public ControlMessage getLastEvent(String topic, Properties properties, Integer pollTimeout) ;
}
