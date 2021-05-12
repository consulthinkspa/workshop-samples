package it.consulthink.oe.ingest;

import com.dellemc.oe.serialization.JsonSerializer;
import com.dellemc.oe.util.AppConfiguration;
import com.dellemc.oe.util.AppConfiguration.StreamConfig;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import it.consulthink.oe.model.NMAPacketData;
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
public class NMAPacketInfiniteWriter implements Runnable{
    // Logger initialization
    private static final Logger LOG = LoggerFactory.getLogger(NMAPacketInfiniteWriter.class);
    private Long limit = null;
    AppConfiguration appConfiguration = null;

    public NMAPacketInfiniteWriter(AppConfiguration appConfiguration, Long limit) {
    	this.appConfiguration = appConfiguration;
    	this.limit = limit;
    }
    
    public boolean createStream(StreamConfig streamConfig) {
        return AppConfiguration.createStream(this.appConfiguration, streamConfig);
    }

    public void run() {

        try {
            // Create client config
            String scope = appConfiguration.getInputStreamConfig().getStream().getScope();
            String streamName = appConfiguration.getInputStreamConfig().getStream().getStreamName();
            ClientConfig config = appConfiguration.getPravegaConfig().getClientConfig();
            
			StreamConfig stream = appConfiguration.getInputStreamConfig();
			StreamConfig streamAnomaly = stream.clone();
			String streamAnomalyName = streamAnomaly.getStream().getStreamName() + "-anomaly";
			streamAnomaly.updateStream(streamAnomaly.getStream().getScope(), streamAnomalyName);
			
	        
            boolean  streamok = createStream(stream);
            boolean  streamAnomalyOk = createStream(streamAnomaly);

            
            EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, config);
            // Create  Pravega event writer

            JsonSerializer<NMAPacketData> serializer = new JsonSerializer<NMAPacketData>(NMAPacketData.class);
			EventWriterConfig eventWriterConfig = EventWriterConfig.builder().build();
			
			EventStreamWriter<NMAPacketData> writer = clientFactory.createEventWriter(
                    streamName,
                    serializer,
                    eventWriterConfig);
			
			EventStreamWriter<NMAPacketData> writerAnomaly = clientFactory.createEventWriter(
					streamAnomalyName,
                    serializer,
                    eventWriterConfig);			
            
                      

            
            Set<String> myIps = appConfiguration.getMyIps();
            // even if empty it gets created when generating the data
            List<String> myMacsList = new ArrayList<>();

			List<String> myIpsList = new ArrayList<String>(myIps);
			Stream<NMAPacketData> generatorStream = NMAPacketDataGenerator.generateInfiniteStream(myIpsList, myMacsList);
			if (limit != null) {
				generatorStream = generatorStream.limit(limit);
			}
			generatorStream
			.parallel()
			.map(data -> {
				CompletableFuture<Void> result = null;
				try {
//					System.err.println("Sending  "+data+ " >> " + new String(serializer.serializeToByteArray(data),  "UTF-8"));
					result = writer.writeEvent(data);
					if (((NMAPacketData) data) instanceof NMAPacketDataGenerator.NMAPacketDataAnomaly) {
						NMAPacketDataGenerator.NMAPacketDataAnomaly anomaly = (NMAPacketDataGenerator.NMAPacketDataAnomaly) data;
						writerAnomaly.writeEvent(anomaly);
					}
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
        NMAPacketInfiniteWriter ew = new NMAPacketInfiniteWriter(new AppConfiguration(args), limit);
        ew.run();
    }
}
