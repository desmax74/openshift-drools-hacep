package org.kie.u212.model;

import java.io.Serializable;

public class FactCountMessage implements Serializable {

    private String key;
    private long factCount;

    public FactCountMessage(){}

    public FactCountMessage(String key,
                            long factCount) {
        this.key = key;
        this.factCount = factCount;
    }

    public String getKey() {
        return key;
    }

    public long getFactCount() {
        return factCount;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FactCountMessage{");
        sb.append("key='").append(key).append('\'');
        sb.append(", factCount=").append(factCount);
        sb.append('}');
        return sb.toString();
    }
}
