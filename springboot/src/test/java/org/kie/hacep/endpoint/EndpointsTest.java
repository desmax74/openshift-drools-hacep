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
package org.kie.hacep.endpoint;

import org.junit.Assert;
import org.junit.Test;
import org.kie.hacep.core.GlobalStatus;
import org.springframework.http.ResponseEntity;

public class EndpointsTest {

    @Test
    public void allTest() {
        Endpoints endpoints = new Endpoints();
        ResponseEntity<String> result = endpoints.all();
        Assert.assertNotNull(result);
    }

    @Test
    public void readinessTest() {
        GlobalStatus.setNodeReady(true);
        Endpoints endpoints = new Endpoints();
        ResponseEntity<Void> result = endpoints.getReadiness();
        Assert.assertNotNull(result);
        Assert.assertTrue(result.getStatusCode().is2xxSuccessful());
        GlobalStatus.setNodeReady(false);
        result = endpoints.getReadiness();
        Assert.assertNotNull(result);
        Assert.assertTrue(result.getStatusCode().is5xxServerError());
    }

    @Test
    public void livenessTest() {
        GlobalStatus.setNodeLive(true);
        Endpoints endpoints = new Endpoints();
        ResponseEntity<Void> result = endpoints.getLiveness();
        Assert.assertNotNull(result);
        Assert.assertTrue(result.getStatusCode().is2xxSuccessful());
        GlobalStatus.setNodeLive(false);
        result = endpoints.getLiveness();
        Assert.assertNotNull(result);
        Assert.assertTrue(result.getStatusCode().is5xxServerError());
    }
}
