/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.dellemc.oe.ingest;

import com.dellemc.oe.serialization.JsonNodeSerializer;
import com.dellemc.oe.util.*;
import com.dellemc.oe.util.AppConfiguration.StreamConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.concurrent.CompletableFuture;

/**
 * A simple example app that uses a Pravega Writer to write to a given scope and stream.
 */
public class JSONWriter implements Runnable{
    // Logger initialization
    private static final Logger LOG = LoggerFactory.getLogger(JSONWriter.class);
	private AppConfiguration appConfiguration;

    public JSONWriter(AppConfiguration appConfiguration) {
    	this.appConfiguration = appConfiguration;
    	LOG.info(this.getClass().getName()+".appConfiguration = "+this.appConfiguration);
    }

    public void run() {

        ObjectNode message = null;
        try {
        	
            String scope = appConfiguration.getInputStreamConfig().getStream().getScope();
            String streamName = appConfiguration.getInputStreamConfig().getStream().getStreamName();
            ClientConfig config = appConfiguration.getPravegaConfig().getClientConfig();
            
			StreamConfig stream = appConfiguration.getInputStreamConfig();
	        
            boolean  streamok = AppConfiguration.createStream(appConfiguration, stream);
        	
            EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, config);
            // Create  Pravega event writer
            EventStreamWriter<JsonNode> writer = clientFactory.createEventWriter(
                    streamName,
                    new JsonNodeSerializer(),
                    EventWriterConfig.builder().build());

            //  Coverst CSV  data to JSON
            String data = DataGenerator.convertCsvToJson(Parameters.getDataFile());
            // Deserialize the JSON message.
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonArray = objectMapper.readTree(data);
            if (jsonArray.isArray()) {
                for (JsonNode node : jsonArray) {
                    message = (ObjectNode) node;
                    LOG.info("@@@@@@@@@@@@@ DATA  @@@@@@@@@@@@@  "+message.toString());
                    final CompletableFuture writeFuture = writer.writeEvent(Parameters.getRoutingKey(), message);
                    writeFuture.get();
                    Thread.sleep(10000);
                }

            }
        } catch (Exception e) {
            LOG.error("@@@@@@@@@@@@@ ERROR  @@@@@@@@@@@@@  " + e.getMessage());
        }
    }
    public static void main(String[] args) {
        // Get the Program parameters
        JSONWriter ew = new JSONWriter(new AppConfiguration(args));
        ew.run();
    }
}
