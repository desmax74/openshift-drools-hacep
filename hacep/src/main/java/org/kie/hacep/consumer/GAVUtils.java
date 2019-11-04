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
package org.kie.hacep.consumer;

import org.kie.api.KieServices;
import org.kie.api.builder.ReleaseId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GAVUtils {

    private static final Logger logger = LoggerFactory.getLogger(GAVUtils.class);

    public static ReleaseId getReleaseID(String gav, KieServices srv){
        String parts[] = getSplittedGav(gav);
        return srv.newReleaseId(parts[0], parts[1], parts[2]);
    }

    public static String[] getSplittedGav(String gav){
        return gav.split(":");
    }


}
