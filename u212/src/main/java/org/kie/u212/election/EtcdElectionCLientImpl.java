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
package org.kie.u212.election;

import java.util.UUID;

import com.google.protobuf.ByteString;
import com.ibm.etcd.client.EtcdClient;
import com.ibm.etcd.client.utils.EtcdLeaderElection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.ibm.etcd.client.KeyUtils.bs;

public class EtcdElectionCLientImpl implements EtcdElectionCLient {

    private final Logger logger = LoggerFactory.getLogger(EtcdElectionCLientImpl.class);
    private String ETCD_ADDRESS = "localhost";
    public static final String ELECTION_KEY = "/election";
    private int ETCD_PORT = 2379;
    private EtcdClient client;
    private ByteString electionKey;
    private EtcdLeaderElection observer;
    private String myID = UUID.randomUUID().toString();//TODO replace with env vars from k8s
    private EtcdLeaderElection thisPOD;

    public static void init(){

    }

    public EtcdElectionCLientImpl(){
        client = EtcdClient.forEndpoint(ETCD_ADDRESS, ETCD_PORT).withPlainText().build();
        electionKey = bs(ELECTION_KEY);
        observer = new EtcdLeaderElection(client, electionKey);
        observer.start();
        thisPOD = new EtcdLeaderElection(client, electionKey,
                                         myID);
        thisPOD.start();
        logger.info("EtcdElectionClient started");
        if(amITheLeader()){
            logger.info("I'm the leader");
        }else{
            logger.info("I'm a slave");
        }
    }

    @Override
    public boolean amITheLeader(){
        return thisPOD.isLeader();
    }

    @Override
    public String getID(){
        return myID;
    }

    @Override
    public String getLeaderID(){
        return  thisPOD.getLeaderId();
    }

    @Override
    public EtcdLeaderElection getObserver(){
        return observer;
    }

    @Override
    public void close(){
        observer.close();
        thisPOD.close();
        logger.info("EtcdElectionClient stopped");
    }

}
