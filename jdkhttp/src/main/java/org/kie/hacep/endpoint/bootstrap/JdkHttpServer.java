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
package org.kie.hacep.endpoint.bootstrap;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.Map;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.kie.hacep.EnvConfig;
import org.kie.hacep.core.Bootstrap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdkHttpServer {

    private final static String OK = "OK";
    private static Logger logger = LoggerFactory.getLogger(JdkHttpServer.class);

    public static void main(String[] args) throws Exception {

        HttpServer server = HttpServer.create(new InetSocketAddress(8080), 0);
        server.createContext("/health", new HealthHandler());
        server.createContext("/env/all", new EnvHandler());
        server.start();
        Bootstrap.startEngine(EnvConfig.getDefaultEnvConfig());
        logger.info("Core system started");
        Runtime.getRuntime().addShutdownHook(new Thread(() -> Bootstrap.stopEngine()));
    }

    static class HealthHandler implements HttpHandler {

        @Override
        public void handle(HttpExchange httpExchange) throws IOException {
            httpExchange.sendResponseHeaders(200, OK.length());
            OutputStream os = httpExchange.getResponseBody();
            os.write(OK.getBytes());
            os.close();
        }
    }

    private static class EnvHandler implements HttpHandler {

        @Override
        public void handle(HttpExchange httpExchange) throws IOException {
            StringBuilder sb = new StringBuilder();
            Map<String, String> env = System.getenv();
            for (Map.Entry<String, String> entry : env.entrySet()) {
                sb.append(entry.getKey()).append(":").append(entry.getValue()).append("\n");
            }
            String result = sb.toString();
            httpExchange.sendResponseHeaders(200, result.length());
            OutputStream os = httpExchange.getResponseBody();
            os.write(result.getBytes());
            os.close();
        }
    }
}
