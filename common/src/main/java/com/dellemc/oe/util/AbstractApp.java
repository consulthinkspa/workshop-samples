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

import io.pravega.client.admin.StreamInfo;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

public abstract class AbstractApp implements Runnable {

    private static Logger LOG = LoggerFactory.getLogger( AbstractApp.class );

    protected AppConfiguration appConfiguration;

    public AbstractApp(AppConfiguration appConfiguration) {
        this.appConfiguration = appConfiguration;
        LOG.info("AppConfiguration: "+this.appConfiguration);
//        try {
//			initializeFlinkStreaming();
//		} catch (Exception e) {
//			LOG.error("Error on initializeFlinkStreaming",e);
//		}
//        try {
//			initializeFlinkBatch();
//		} catch (Exception e) {
//			LOG.error("Error on initializeFlinkBatch",e);
//		}
    }

    public boolean createStream(AppConfiguration.StreamConfig streamConfig) {
    	return AppConfiguration.createStream(this.appConfiguration, streamConfig);
    }
    

//    public StreamInfo getStreamInfo(Stream stream) {
//        try(StreamManager streamManager = StreamManager.create(appConfiguration.getPravegaConfig().getClientConfig())) {
//            return streamManager.getStreamInfo(stream.getScope(), stream.getStreamName());
//        }
//    }

    public StreamExecutionEnvironment initializeFlinkStreaming() throws Exception {
        // Configure the Flink job environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Set parallelism, etc.
        int parallelism = appConfiguration.getParallelism();
        if (parallelism > 0) {
            env.setParallelism(parallelism);
        }
        if (!appConfiguration.isEnableOperatorChaining()) {
            env.disableOperatorChaining();
        }
        if(appConfiguration.isEnableCheckpoint()) {
            long checkpointInterval = appConfiguration.getCheckpointInterval();
            env.enableCheckpointing(checkpointInterval, CheckpointingMode.EXACTLY_ONCE);
        }
        LOG.info("Parallelism={}, MaxParallelism={}", env.getParallelism(), env.getMaxParallelism());
        return env;
    }

    public ExecutionEnvironment initializeFlinkBatch() throws Exception {
        // Configure the Flink job environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // Set parallelism, etc.
        int parallelism = appConfiguration.getParallelism();
        if (parallelism > 0) {
            env.setParallelism(parallelism);
        }
        LOG.info("Parallelism={}", env.getParallelism());
        return env;
    }
}
