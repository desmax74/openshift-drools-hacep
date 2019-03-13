package org.kie.u212.bootstrap;

public interface MasterElectionService {


    void init();

    void refresh();

    boolean amIMaster();
}
