// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.samples.e2elatency;

import com.azure.cosmos.implementation.apachecommons.lang.StringUtils;
import com.beust.jcommander.Parameter;

public class Configuration {
    @Parameter(names = "-serviceEndpoint", description = "Service Endpoint")
    private String serviceEndpoint;

    @Parameter(names = "-masterKey", description = "Master Key")
    private String masterKey;

    @Parameter(names = "-databaseId", description = "Database ID")
    private String databaseId;

    @Parameter(names = "-containerId", description = "Container ID")
    private String containerId;

    @Parameter(names = "-preferredRegions", description = "Preferred regions")
    private String preferredRegions;

    @Parameter(names = "-concurrency", description = "Number of concurrent threads executing the workload")
    private int concurrency = 1;

    @Parameter(names = {"-h", "-help", "--help"}, description = "Help", help = true)
    private boolean help = false;

    public boolean isHelp() {
        return help;
    }

    public int getConcurrency() { return this.concurrency; }

    public String getServiceEndpoint() { return this.serviceEndpoint; }

    public String getMasterKey() { return this.masterKey; }

    public String getDatabaseId() { return this.databaseId; }

    public String getContainerId() { return this.containerId; }

    public String getPreferredRegions() { return this.preferredRegions; }

    public void tryGetValuesFromSystem() {
        this.serviceEndpoint = StringUtils.defaultString(emptyToNull(System.getenv().get("COSMOS_SERVICE_ENDPOINT")),
            serviceEndpoint);

        this.masterKey = StringUtils.defaultString(emptyToNull(System.getenv().get("COSMOS_KEY")),
            this.masterKey);

        this.databaseId = StringUtils.defaultString(emptyToNull(System.getenv().get("COSMOS_DATABASE_ID")),
            this.databaseId);

        this.containerId = StringUtils.defaultString(emptyToNull(System.getenv().get("COSMOS_CONTAINER_ID")),
            this.containerId);

        this.preferredRegions = StringUtils.defaultString(emptyToNull(System.getenv().get("COSMOS_PREFERRED_REGIONS")),
            this.preferredRegions);

        if (emptyToNull(System.getenv().get("CONCURRENCY")) != null) {
            this.concurrency = Integer.valueOf(System.getenv().get("CONCURRENCY"));
        }
    }

    private static String emptyToNull(String s) {
        if (s == null || s.isEmpty()) {
            return null;
        }

        return s;
    }
}