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

import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dellemc.oe.util.AppConfiguration;
import com.dellemc.oe.util.AppConfiguration.StreamConfig;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.impl.UTF8StringSerializer;

/**
 * A simple example app that uses a Pravega Writer to write to a given scope and stream.
 */
public class EventWriter implements Runnable{

    private static final Logger LOG = LoggerFactory.getLogger(EventWriter.class);
    private AppConfiguration appConfiguration;
    
    public EventWriter(AppConfiguration appConfiguration) {
    	this.appConfiguration = appConfiguration;
    	LOG.info(this.getClass().getName()+".appConfiguration = "+this.appConfiguration);
    }

    public void run() {
        try{
            String scope = appConfiguration.getInputStreamConfig().getStream().getScope();
            String streamName = appConfiguration.getInputStreamConfig().getStream().getStreamName();
            ClientConfig config = appConfiguration.getPravegaConfig().getClientConfig();
            
			StreamConfig stream = appConfiguration.getInputStreamConfig();
	        
            boolean  streamok = AppConfiguration.createStream(appConfiguration, stream);
            EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, config);
            // Create  Pravega event writer
            UTF8StringSerializer serializer = new UTF8StringSerializer();
			EventStreamWriter<String> writer = clientFactory.createEventWriter(
                    streamName,
                    serializer,
                    EventWriterConfig.builder().build());
            while(true) {
            	System.err.println("Sending  "+appConfiguration.getMessage()+ " >> " + new String(serializer.serialize(appConfiguration.getMessage()).array(),  "UTF-8"));
                final CompletableFuture writeFuture = writer.writeEvent( appConfiguration.getRoutingKey(), appConfiguration.getMessage());
                writeFuture.get();
                Thread.sleep(1000);
            }
        }
        catch(Exception e){
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) {
        EventWriter ew = new EventWriter(new AppConfiguration(args));
        ew.run();
    }
}
