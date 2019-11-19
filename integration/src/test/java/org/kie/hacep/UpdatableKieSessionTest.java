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
package org.kie.hacep;

import java.io.File;
import java.util.HashMap;
import java.util.Optional;

import org.apache.maven.repository.internal.MavenRepositorySystemUtils;
import org.apache.maven.wagon.Wagon;
import org.apache.maven.wagon.providers.http.HttpWagon;
import org.eclipse.aether.DefaultRepositorySystemSession;
import org.eclipse.aether.RepositorySystem;
import org.eclipse.aether.RepositorySystemSession;
import org.eclipse.aether.artifact.Artifact;
import org.eclipse.aether.artifact.DefaultArtifact;
import org.eclipse.aether.connector.basic.BasicRepositoryConnectorFactory;
import org.eclipse.aether.impl.DefaultServiceLocator;
import org.eclipse.aether.installation.InstallRequest;
import org.eclipse.aether.repository.LocalRepository;
import org.eclipse.aether.spi.connector.RepositoryConnectorFactory;
import org.eclipse.aether.spi.connector.transport.TransporterFactory;
import org.eclipse.aether.transport.file.FileTransporterFactory;
import org.eclipse.aether.transport.http.HttpTransporterFactory;
import org.eclipse.aether.transport.wagon.WagonProvider;
import org.eclipse.aether.util.artifact.SubArtifact;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.kie.api.KieServices;
import org.kie.api.builder.ReleaseId;
import org.kie.api.builder.Results;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.kie.hacep.consumer.KieContainerUtils;
import org.kie.hacep.core.KieSessionContext;
import org.kie.remote.util.GAVUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpdatableKieSessionTest {

    private Logger logger = LoggerFactory.getLogger(UpdatableKieSessionTest.class);
    private RepositorySystem system;
    private RepositorySystemSession session;
    private KieSessionContext ksCtx ;
    private KieSession kieSession;
    private KieContainer kieContainer;
    private final String gav = "org.kie:sample-hacep-project-kjar:7.29.0-SNAPSHOT";
    private final String updatedGav = "org.kie:sample-hacep-project-kjar:7.30.0-SNAPSHOT";
    private KieServices srv;

    @Before
    public void init() {
        srv = KieServices.get();
        installArtifact(gav, "target/test-classes/sample-hacep-project-kjar-7.29.0-SNAPSHOT.jar", "target/test-classes/sample-hacep-project-kjar-7.29.0-SNAPSHOT.pom");
        installArtifact(gav, "target/test-classes/sample-hacep-project-kjar-7.30.0-SNAPSHOT.jar", "target/test-classes/sample-hacep-project-kjar-7.30.0-SNAPSHOT.pom");
        ksCtx = new KieSessionContext();
    }

    private void initSessionContextFromEmbeddKjar() {
        EnvConfig envConfig = EnvConfig.getDefaultEnvConfig();
        kieContainer = KieContainerUtils.getKieContainer(envConfig, srv);
        kieSession = kieContainer.newKieSession();
        ksCtx.init(kieContainer, kieSession);
    }

    private void initSessionContextFromSpecificKjar() {
        EnvConfig envConfig = EnvConfig.getDefaultEnvConfig();
        envConfig.withUpdatableKJar("true");
        envConfig.withKJarGAV(gav);
        envConfig.skipOnDemandSnapshot("true");
        kieContainer = KieContainerUtils.getKieContainer(envConfig, srv);
        kieSession = kieContainer.newKieSession();
        ksCtx.init(kieContainer, kieSession);
    }

    @Test
    public void testEmbeddedKjar() {
        initSessionContextFromEmbeddKjar();
        Optional<String> gavUSed = ksCtx.getKjarGAVUsed();
        Assert.assertFalse(gavUSed.isPresent());
    }

    @Test
    public void testWithSpecificKjar() {
        initSessionContextFromSpecificKjar();;
        Optional<String> gavUSed = ksCtx.getKjarGAVUsed();
        Assert.assertTrue(gavUSed.isPresent());
        Assert.assertEquals(gavUSed.get(), gav);
    }

    @Test
    public void testUpdateWithSpecificKjar() {
        initSessionContextFromSpecificKjar();;
        Optional<String> gavUSed = ksCtx.getKjarGAVUsed();
        Assert.assertTrue(gavUSed.isPresent());
        Assert.assertEquals(gavUSed.get(), gav);
        ReleaseId releaseId=  GAVUtils.getReleaseID(updatedGav, srv);
        ksCtx.getKieContainer().updateToVersion(releaseId);
        gavUSed = ksCtx.getKjarGAVUsed();
        Assert.assertTrue(gavUSed.isPresent());
        Assert.assertEquals(updatedGav, gavUSed.get());
    }

    ///Aether methods

    private static RepositorySystemSession newSession(RepositorySystem system) {
        DefaultRepositorySystemSession session = MavenRepositorySystemUtils.newSession();
        LocalRepository localRepo = new LocalRepository(System.getenv("HOME")+"/.m2/repository");
        session.setLocalRepositoryManager(system.newLocalRepositoryManager(session, localRepo));
        return session;
    }

    private RepositorySystem newRepositorySystem() {
        DefaultServiceLocator locator = MavenRepositorySystemUtils.newServiceLocator();
        locator.addService(RepositoryConnectorFactory.class, BasicRepositoryConnectorFactory.class);
        locator.addService(TransporterFactory.class, FileTransporterFactory.class);
        locator.addService(TransporterFactory.class, HttpTransporterFactory.class);
        locator.setServices(WagonProvider.class, new ManualWagonProvider());
        return locator.getService(RepositorySystem.class);
    }

    private class ManualWagonProvider implements WagonProvider {

        public Wagon lookup(String roleHint) {
            if ("http".equals(roleHint) || "https".equals(roleHint)) {
                return new HttpWagon();
            }
            return null;
        }

        public void release(Wagon wagon) { }
    }

    private void installArtifact(String gav, String fileName, String pomName) {
        system = newRepositorySystem();
        session = newSession(system);
        InstallRequest req = new InstallRequest();
        String[] gavs = GAVUtils.getSplittedGav(gav);
        File jar = new File(fileName);
        if(!jar.exists()){
            throw new RuntimeException("File not found");
        }

        Artifact jarArtifact = new DefaultArtifact(gavs[0], gavs[1],null, "jar", gavs[2],new HashMap<>(), jar);
        Artifact pom = new SubArtifact(jarArtifact, null, "pom" );
        pom = pom.setFile( new File( pomName ) );
        req.addArtifact(jarArtifact).addArtifact( pom );;

        try {
            system.install(session, req);

        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage(), e);
        }
    }
}
