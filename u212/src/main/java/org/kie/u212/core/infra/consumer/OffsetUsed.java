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
package org.kie.u212.core.infra.consumer;

public enum OffsetUsed {
    BEGIN, // start from the begin of the topic
    END,   // start from the end of the topic
    TIME,  // start from a specific interval before now
    INDEX, // start from a specific index
    WAIT,  // the offset was already set and we don't want to change
    CHANGE // we want to change in a next iteration
}
