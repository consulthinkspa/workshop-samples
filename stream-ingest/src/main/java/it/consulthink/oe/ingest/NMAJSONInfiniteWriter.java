package it.consulthink.oe.ingest;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

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
public class NMAJSONInfiniteWriter implements Runnable{
    // Logger initialization
    private static final Logger LOG = LoggerFactory.getLogger(NMAJSONInfiniteWriter.class);
    private Long limit = null;
    AppConfiguration appConfiguration = null;

    public NMAJSONInfiniteWriter(AppConfiguration appConfiguration, Long limit) {
    	this.appConfiguration = appConfiguration;
    	this.limit = limit;
    }
    
    public boolean createStream(AppConfiguration.StreamConfig streamConfig) {
        boolean result = false;
        try(StreamManager streamManager = StreamManager.create(appConfiguration.getPravegaConfig().getClientConfig())) {
        	final boolean scopeIsNew = streamManager.createScope(streamConfig.getStream().getScope());
            StreamConfiguration streamConfiguration = StreamConfiguration.builder()
                    .scalingPolicy(ScalingPolicy.byDataRate(streamConfig.getTargetRate(), streamConfig.getScaleFactor(), streamConfig.getMinNumSegments()))
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
			Stream<NMAJSONData> generatorStream = NMAJSONDataGenerator.generateInfiniteStream(myIpsList);
			if (limit != null) {
				generatorStream = generatorStream.limit(limit);
			}
			generatorStream
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
    	Long limit = null;
    	if (args != null && args.length > 0) {
    		for (int i = 0; i < args.length; i++) {
				if (args[i].equals("--limit") && args.length >= i+1){
					limit = Long.valueOf(args[i+1]);
					break;
				}
			}
    	}
        NMAJSONInfiniteWriter ew = new NMAJSONInfiniteWriter(new AppConfiguration(args), limit);
        ew.run();
    }
}
