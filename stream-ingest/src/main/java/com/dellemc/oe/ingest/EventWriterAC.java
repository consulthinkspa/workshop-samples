package com.dellemc.oe.ingest;

import com.dellemc.oe.serialization.JsonSerializer;
import com.dellemc.oe.util.AppConfiguration;
import com.dellemc.oe.util.AppConfiguration.StreamConfig;
import com.dellemc.oe.util.Parameters;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import it.consulthink.oe.ingest.NMAJSONDataGenerator;
import it.consulthink.oe.model.NMAJSONData;
import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

/**
 * A simple example app that uses a Pravega Writer to write to a given scope and stream.
 */
public class EventWriterAC implements Runnable{
    // Logger initialization
    private static final Logger LOG = LoggerFactory.getLogger(EventWriterAC.class);
    private Long limit = null;
    AppConfiguration appConfiguration = null;

    public EventWriterAC(AppConfiguration appConfiguration, Long limit) {
    	this.appConfiguration = appConfiguration;
    	this.limit = limit;
    }
    
    public boolean createStream(StreamConfig streamConfig) {
        boolean result = false;
        try(StreamManager streamManager = StreamManager.create(appConfiguration.getPravegaConfig().getClientConfig())) {

        	//TODO controllare modifiche per dell SDP
//        	if (!streamManager.checkScopeExists(streamConfig.getStream().getScope())) {
//        		streamManager.createScope(streamConfig.getStream().getScope());
//        	}
//
            StreamConfiguration streamConfiguration = StreamConfiguration.builder()
                    .scalingPolicy(ScalingPolicy.byDataRate(streamConfig.getTargetRate(), streamConfig.getScaleFactor(), streamConfig.getMinNumSegments()))
                    .build();

			//TODO controllare modifiche per dell SDP

//        	if (streamManager.checkStreamExists(streamConfig.getStream().getScope(), streamConfig.getStream().getStreamName())){
//                result = streamManager.updateStream(streamConfig.getStream().getScope(), streamConfig.getStream().getStreamName(), streamConfiguration);
//        	}else {
                result = streamManager.createStream(streamConfig.getStream().getScope(), streamConfig.getStream().getStreamName(), streamConfiguration);
//        	}

            LOG.info("Creating Pravega Stream: " +" "+ streamConfig.getStream().getStreamName() +" "+ streamConfig.getStream().getScope() + streamConfiguration);
        }
        return result;
    }

    public void run() {

        try {
            // Create client config
            String scope = appConfiguration.getInputStreamConfig().getStream().getScope();
            String streamName = appConfiguration.getInputStreamConfig().getStream().getStreamName();
            ClientConfig config = appConfiguration.getPravegaConfig().getClientConfig();
            
			StreamConfig stream = appConfiguration.getInputStreamConfig();
	        
            boolean  streamok = createStream(stream);

            
            EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, config);
            // Create  Pravega event writer

            EventStreamWriter<String> writer = clientFactory.createEventWriter(
                    streamName,
                    new UTF8StringSerializer(),
                    EventWriterConfig.builder().build());
            
                      

            while(true) {
            	final CompletableFuture writeFuture = writer.writeEvent( Parameters.getRoutingKey(), Parameters.getMessage());
				writeFuture.get();
				Thread.sleep(1000);
			}


            

        } catch (Exception e) {
            LOG.error("Error in run " + e.getMessage(), e);
        }
    }
    
    public static void main(String[] args) {
        // Get the Program parameters
    	Long limit = null;
    	if (args != null && args.length > 0) {
    		for (int i = 0; i < args.length; i++) {
				if (args[i].equals("--limit") && args.length >= i+1){
					limit = Long.valueOf(args[i+1]);
					break;
				}
			}
    	}
        EventWriterAC ew = new EventWriterAC(new AppConfiguration(args), limit);
        ew.run();
    }
}
