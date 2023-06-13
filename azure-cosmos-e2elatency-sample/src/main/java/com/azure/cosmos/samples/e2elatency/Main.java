// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.samples.e2elatency;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.implementation.Database;
import com.azure.cosmos.implementation.guava25.collect.Lists;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.ThroughputProperties;
import com.beust.jcommander.JCommander;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class Main {
    private final static Logger LOGGER = LoggerFactory.getLogger(Main.class);
    private static ExecutorService pool;

    public static void main(String[] args) {

        LOGGER.debug("Parsing the command-line arguments ...");
        Configuration cfg = new Configuration();
        cfg.tryGetValuesFromSystem();

        JCommander jcommander = new JCommander(cfg, args);
        if (cfg.isHelp()) {
            // prints out the usage help
            jcommander.usage();
            return;
        }

        pool = Executors.newFixedThreadPool(cfg.getConcurrency());

        Workload workload = new Workload(cfg);
        workload.init();

        for (int i = 0; i < cfg.getConcurrency(); i++) {
            final int snapshot = i;
            pool.execute(() -> workload.execute(snapshot));
        }

        Scanner scanner = new Scanner(System.in);
        jcommander.getConsole().println("Press <ENTER> to quit.");
        scanner.nextLine();
        LOGGER.info("Closing...");
        workload.stop();
        try {
            pool.awaitTermination(5, TimeUnit.SECONDS);
            pool.shutdown();
        } catch (InterruptedException e) {
            LOGGER.info("Could not shutdown gracefully...");
        }
        LOGGER.info("Good bye...");
    }

    private static final class Workload {
        private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

        private static final int DOC_COUNT = 1000;
        private final CosmosClient client;
        private final Configuration cfg;

        private final String[] ids;

        private final AtomicBoolean stopped = new AtomicBoolean(false);

        public Workload(Configuration cfg) {
            this.cfg = cfg;
            CosmosClientBuilder clientBuilder = new CosmosClientBuilder()
                .endpoint(cfg.getServiceEndpoint())
                .key(cfg.getMasterKey())
                .preferredRegions(Lists.newArrayList(cfg.getPreferredRegions().split(",")));
            this.client = clientBuilder.buildClient();
            this.ids = new String[DOC_COUNT];

            for (int i = 0; i < DOC_COUNT; i++) {
                this.ids[i] = UUID.randomUUID().toString();
            }
        }

        public void init() {
            LOGGER.info("Initializing workload...");

            client.createDatabaseIfNotExists(cfg.getDatabaseId());
            CosmosDatabase db = client.getDatabase(cfg.getDatabaseId());
            db.createContainerIfNotExists(
                cfg.getContainerId(),
                "/id",
                ThroughputProperties.createAutoscaledThroughput(100_000));
            CosmosContainer container = db.getContainer(cfg.getContainerId());

            for (int i = 0; i < DOC_COUNT; i++) {
                container.createItem(
                    getDocumentDefinition(this.ids[i]),
                    new PartitionKey(this.ids[i]),
                    null
                );
            }

            LOGGER.info("Initialization finished.");
        }

        public void stop() {
            if (this.stopped.compareAndSet(false, true)) {
                LOGGER.info("Requested workload to stop...");
            }
        }

        public void execute(int sequence) {
            LOGGER.info("Starting Thread {}", sequence);
            CosmosDatabase db = client.getDatabase(cfg.getDatabaseId());
            CosmosContainer container = db.getContainer(cfg.getContainerId());

            while(!this.stopped.get()) {
                String id = this.ids[ThreadLocalRandom.current().nextInt(DOC_COUNT)];
                try {
                    CosmosItemResponse<ObjectNode> response = container.readItem(id, new PartitionKey(id), ObjectNode.class);
                    if (response.getStatusCode() != 200) {
                        LOGGER.error("Failure reading document {}: {}", id, response.getDiagnostics());
                    }
                } catch (CosmosException error) {
                    LOGGER.error("Exception reading document {}: {}", id, error);
                }
            }

            LOGGER.info("Completed Thread {}", sequence);
        }

        private static ObjectNode getDocumentDefinition(String id) {
            ObjectNode json = OBJECT_MAPPER.createObjectNode();
            json.put("id", id);
            for (int i = 1; i <= 20; i++) {
                json.put("Property" + String.valueOf(i), UUID.randomUUID().toString());
            }

            return json;
        }
    }
}