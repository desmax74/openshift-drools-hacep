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
package org.kie.u212.producer;

import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

public class KafkaCallbackTranslator implements Callback {

    private CompletableFuture<Boolean> completableFuture;

    public KafkaCallbackTranslator(CompletableFuture completableFuture){
        this.completableFuture = completableFuture;
    }

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if(exception != null) {
            completableFuture.complete(Boolean.FALSE);
        }else{
            completableFuture.complete(Boolean.TRUE);
        }

    }
}
