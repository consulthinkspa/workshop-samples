/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package com.dellemc.oe.util;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.pravega.client.stream.Stream;
import io.pravega.connectors.flink.PravegaConfig;

// All parameters will come from environment variables. This makes it easy
// to configure on Docker, Mesos, Kubernetes, etc.
public class AppConfiguration {
    private static Logger log = LoggerFactory.getLogger(AppConfiguration.class);

    private final PravegaConfig pravegaConfig;
    private final StreamConfig inputStreamConfig;
    private final StreamConfig outputStreamConfig;
    private int parallelism;
    private long checkpointInterval;
    private boolean enableCheckpoint;
    private boolean enableOperatorChaining;
    private boolean enableRebalance;
    private String routingKey;
    private String dataFile;
    private String message;
    private String csvDir;
    private Set<String> myIps = new HashSet<String>();
    private String influxdbUrl =  "http://host.docker.internal:8086";
    private String influxdbUsername = "admin";
    private String influxdbPassword = "password";
	private String influxdbDbName = "demo";    

    public AppConfiguration(String[] args) {
        ParameterTool params = ParameterTool.fromArgs(args);
        log.info("Parameter Tool: {}", params.toMap());

        pravegaConfig = PravegaConfig.fromParams(params).withDefaultScope("examples"); // TODO: make configurable
        inputStreamConfig = new StreamConfig(pravegaConfig, "input-", params);
        outputStreamConfig = new StreamConfig(pravegaConfig, "output-", params);
        parallelism = params.getInt("parallelism", 1);
        checkpointInterval = params.getLong("checkpointInterval", 10000);     // milliseconds
        enableCheckpoint = params.getBoolean("enableCheckpoint", true);
        enableOperatorChaining = params.getBoolean("enableOperatorChaining", true);
        enableRebalance = params.getBoolean("rebalance", false);
        routingKey = params.get("routing-key", "default");
        dataFile = params.get("dataFile", "earthquakes1970-2014.csv");
        message = params.get("message", "hello world");
        csvDir = params.get("csvDir", "/mnt/flink");
        myIps = new HashSet<String>(Arrays.asList(params.get("myIps",
                "213.61.202.114,213.61.202.115,213.61.202.116," +
                        "213.61.202.117,213.61.202.118,213.61.202.119," +
                        "213.61.202.120,213.61.202.121,213.61.202.122," +
                        "213.61.202.123,213.61.202.124,213.61.202.125,213.61.202.126").split(",")));
        influxdbUrl =  params.get("influxdbUrl", "http://host.docker.internal:8086");
        influxdbUsername = params.get("influxdbUsername", "root");
        influxdbPassword = params.get("influxdbPassword", "root");
        influxdbDbName = params.get("influxdbDbName", "demo");           
    }

    public String getCsvDir() {return csvDir;}

    public Set<String> getMyIps() {
		return myIps;
	}

    public String getInfluxdbUrl() {
		return influxdbUrl;
	}

	public String getInfluxdbUsername() {
		return influxdbUsername;
	}

	public String getInfluxdbPassword() {
		return influxdbPassword;
	}

	public String getInfluxdbDbName() {
		return influxdbDbName;
	}

	public String getMessage() {
        return message;
    }

    public String getDataFile() {
        return dataFile;
    }

    public String getRoutingKey() {
        return routingKey;
    }

    public PravegaConfig getPravegaConfig() {
        return pravegaConfig;
    }

    public StreamConfig getInputStreamConfig() {
        return inputStreamConfig;
    }

    public StreamConfig getOutputStreamConfig() {
        return outputStreamConfig;
    }

    public int getParallelism() {
        return parallelism;
    }

    public long getCheckpointInterval() {
        return checkpointInterval;
    }

    public boolean isEnableCheckpoint() {
        return enableCheckpoint;
    }

    public boolean isEnableOperatorChaining() {
        return enableOperatorChaining;
    }

    public boolean isEnableRebalance() {
        return enableRebalance;
    }

    public static class StreamConfig {
        protected Stream stream;
        protected int targetRate;
        protected int scaleFactor;
        protected int minNumSegments;

        public StreamConfig(PravegaConfig pravegaConfig, String argPrefix, ParameterTool params) {
            stream = pravegaConfig.resolve(params.get(argPrefix + "stream", "default"));
            targetRate = params.getInt(argPrefix + "targetRate", 100000);  // Data rate in KB/sec
            scaleFactor = params.getInt(argPrefix + "scaleFactor", 2);
            minNumSegments = params.getInt(argPrefix + "minNumSegments", 3);
        }
        

        public Stream getStream() {
            return stream;
        }

        public int getTargetRate() {
            return targetRate;
        }

        public int getScaleFactor() {
            return scaleFactor;
        }

        public int getMinNumSegments() {
            return minNumSegments;
        }
    }
}
