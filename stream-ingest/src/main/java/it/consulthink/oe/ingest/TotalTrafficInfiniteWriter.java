package it.consulthink.oe.ingest;

import java.io.UnsupportedEncodingException;
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

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import it.consulthink.oe.model.NMAJSONData;
import it.consulthink.oe.model.TotalTraffic;

/**
 * A simple example app that uses a Pravega Writer to write to a given scope and stream.
 */
public class TotalTrafficInfiniteWriter implements Runnable{
    // Logger initialization
    private static final Logger LOG = LoggerFactory.getLogger(TotalTrafficInfiniteWriter.class);
    private Long limit = null;
    AppConfiguration appConfiguration = null;

    public TotalTrafficInfiniteWriter(AppConfiguration appConfiguration, Long limit) {
    	this.appConfiguration = appConfiguration;
    	this.limit = limit;
    }
    
    public boolean createStream(AppConfiguration.StreamConfig streamConfig) {
        return AppConfiguration.createStream(this.appConfiguration, streamConfig);
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
            

           JsonSerializer<TotalTraffic> serializer = new JsonSerializer<TotalTraffic>(TotalTraffic.class);
		EventStreamWriter<TotalTraffic> writer = clientFactory.createEventWriter(
                    streamName,
                    serializer,
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
				TotalTraffic t = new TotalTraffic(data.getTime(), data.getBytesin() + data.getBytesout());
				try {
					System.err.println("Sending  "+t+ " >> " + new String(serializer.serializeToByteArray(t),  "UTF-8"));
					result = writer.writeEvent(t);
				} catch (Throwable e) {
					LOG.error("Error Sending "+t+" " + e.getMessage());
				}
				return Tuple2.of(t, result);
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
        TotalTrafficInfiniteWriter ew = new TotalTrafficInfiniteWriter(new AppConfiguration(args), limit);
        ew.run();
    }
}
