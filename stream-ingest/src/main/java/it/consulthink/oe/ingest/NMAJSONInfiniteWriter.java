package it.consulthink.oe.ingest;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import it.consulthink.oe.model.NMAJSONData;

/**
 * A simple example app that uses a Pravega Writer to write to a given scope and stream.
 */
public class NMAJSONInfiniteWriter {
    // Logger initialization
    private static final Logger LOG = LoggerFactory.getLogger(NMAJSONInfiniteWriter.class);
    AppConfiguration appConfiguration = null;

    public NMAJSONInfiniteWriter(AppConfiguration appConfiguration) {
    	this.appConfiguration = appConfiguration;
    }
    
    public boolean createStream(AppConfiguration.StreamConfig streamConfig) {
        boolean result = false;
        try(StreamManager streamManager = StreamManager.create(appConfiguration.getPravegaConfig().getClientConfig())) {
        	final boolean scopeIsNew = streamManager.createScope(streamConfig.getStream().getScope());
//            StreamConfiguration streamConfiguration = StreamConfiguration.builder()
//                    .scalingPolicy(ScalingPolicy.byDataRate(streamConfig.getTargetRate(), streamConfig.getScaleFactor(), streamConfig.getMinNumSegments()))
//                    .build();
            StreamConfiguration streamConfiguration = StreamConfiguration.builder()
                    .scalingPolicy(ScalingPolicy.fixed(1))
                    .build();     
            result = streamManager.createStream(streamConfig.getStream().getScope(), streamConfig.getStream().getStreamName(), streamConfiguration);
            LOG.info("Creating Pravega Stream: " +" "+ streamConfig.getStream().getStreamName() +" "+ streamConfig.getStream().getScope() +" ("+scopeIsNew+" )" + streamConfiguration);
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

            EventStreamWriter<NMAJSONData> writer = clientFactory.createEventWriter(
                    streamName,
                    new JsonSerializer<NMAJSONData>(NMAJSONData.class),
                    EventWriterConfig.builder().build());
            
                      

            
            Set<String> myIps = appConfiguration.getMyIps();
			List<String> myIpsList = new ArrayList<String>(myIps);
			NMAJSONDataGenerator.generateInfiniteStream(myIpsList)
//			.limit(100)
			.parallel()
			.map(data -> {
				CompletableFuture<Void> result = null;
				try {
					LOG.info("Sending  "+ data);
					result = writer.writeEvent(data);
				} catch (Throwable e) {
					LOG.error("Error Sending "+data+" " + e.getMessage());
				}
				return Tuple2.of(data, result);
			})
			.forEach(t -> {
				try {
					t.f1.get();
				} catch (Throwable e) {
					LOG.error("Error Sending on "+t.f0+"  " + e.getMessage());
				} 
			}
			);
        } catch (Exception e) {
            LOG.error("Error in run " + e.getMessage(), e);
        }
    }
    
    public static void main(String[] args) {
        // Get the Program parameters
        NMAJSONInfiniteWriter ew = new NMAJSONInfiniteWriter(new AppConfiguration(args));
        ew.run();
    }
}
