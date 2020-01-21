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

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpPrincipal;
import org.junit.Test;
import org.kie.hacep.core.GlobalStatus;
import org.kie.hacep.endpoint.bootstrap.JdkHttpServer;

import static org.junit.Assert.*;

public class EndpointsTest {

    @Test
    public void allTest() throws IOException {
        HttpHandler handler = new JdkHttpServer.EnvHandler();
        HttpExchange exchange = getHttpExchange();
        handler.handle(exchange);
        assertNotNull(exchange);
    }

    @Test
    public void livenessTest() throws IOException {
        GlobalStatus.setNodeLive(true);
        HttpHandler handler = new JdkHttpServer.LivenessHandler();
        HttpExchange exchange = getHttpExchange();
        handler.handle(exchange);
        assertNotNull(exchange);
        assertTrue(200 == Integer.valueOf(exchange.getResponseHeaders().get("return-code").get(0)));

        GlobalStatus.setNodeLive(false);
        handler = new JdkHttpServer.LivenessHandler();
        exchange = getHttpExchange();
        handler.handle(exchange);
        assertNotNull(exchange);
        assertTrue(503 == Integer.valueOf(exchange.getResponseHeaders().get("return-code").get(0)));
    }

    @Test
    public void readynessTest() throws IOException {
        GlobalStatus.setNodeReady(true);
        HttpHandler handler = new JdkHttpServer.ReadinessHandler();
        HttpExchange exchange = getHttpExchange();
        handler.handle(exchange);
        assertNotNull(exchange);
        assertTrue(200 == Integer.valueOf(exchange.getResponseHeaders().get("return-code").get(0)));

        GlobalStatus.setNodeReady(false);
        handler = new JdkHttpServer.ReadinessHandler();
        exchange = getHttpExchange();
        handler.handle(exchange);
        assertNotNull(exchange);
        assertTrue(503 == Integer.valueOf(exchange.getResponseHeaders().get("return-code").get(0)));
    }


    private HttpExchange getHttpExchange() {

        HttpExchange exchange = new HttpExchange() {


            private Headers rsHeaders = new Headers();
            private Headers rqHeaders = new Headers();

            @Override
            public Headers getRequestHeaders() {
                return rqHeaders;
            }

            @Override
            public Headers getResponseHeaders() {
                return rsHeaders;
            }

            @Override
            public URI getRequestURI() {
                return null;
            }

            @Override
            public String getRequestMethod() {
                return null;
            }

            @Override
            public HttpContext getHttpContext() {
                return null;
            }

            @Override
            public void close() {

            }

            @Override
            public InputStream getRequestBody() {
                return null;
            }

            @Override
            public OutputStream getResponseBody() {
                FileOutputStream fos = null;
                ObjectOutputStream oos = null;
                try {
                    fos = new FileOutputStream("t.tmp");
                    oos = new ObjectOutputStream(fos);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return oos;
            }

            @Override
            public void sendResponseHeaders(int rCode, long responseLength) {
                rsHeaders.add("return-code", String.valueOf(rCode));
            }

            @Override
            public InetSocketAddress getRemoteAddress() {
                return null;
            }

            @Override
            public int getResponseCode() {
                return 0;
            }

            @Override
            public InetSocketAddress getLocalAddress() {
                return null;
            }

            @Override
            public String getProtocol() {
                return null;
            }

            @Override
            public Object getAttribute(String name) {
                return null;
            }

            @Override
            public void setAttribute(String name, Object value) { }

            @Override
            public void setStreams(InputStream i, OutputStream o) { }

            @Override
            public HttpPrincipal getPrincipal() {
                return null;
            }
        };
        return exchange;
    }


}
