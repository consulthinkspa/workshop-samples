package it.consulthink.oe.flink.packetcount;

import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dellemc.oe.serialization.JsonDeserializationSchema;
import com.dellemc.oe.serialization.JsonSerializationSchema;
import com.dellemc.oe.util.AbstractApp;
import com.dellemc.oe.util.AppConfiguration;
import com.dellemc.oe.util.AppConfiguration.StreamConfig;

import io.pravega.connectors.flink.FlinkPravegaReader;
import io.pravega.connectors.flink.FlinkPravegaWriter;
import io.pravega.connectors.flink.PravegaConfig;
import io.pravega.connectors.flink.PravegaEventRouter;
import it.consulthink.oe.model.NMAJSONData;
import it.consulthink.oe.model.TotalTraffic;

/*
 * At a high level, TotalTrafficReader reads from a Pravega stream, and prints
 * the packet count summary to the output. This class provides an example for
 * a simple Flink application that reads streaming data from Pravega.
 *
 * And  after flink transformation  output redirect to another pravega stream.
 *
 * This application has the following input parameters
 *     stream - Pravega stream name to read from
 *     controller - the Pravega controller URI, e.g., tcp://localhost:9090
 *                  Note that this parameter is automatically used by the PravegaConfig class
 */
public class NMADuplicator extends AbstractApp{

    // Logger initialization
    private static final Logger LOG = LoggerFactory.getLogger(NMADuplicator.class);


    // The application reads data from specified Pravega stream and once every 10 seconds
    // prints the distinct words and counts from the previous 10 seconds.
    public NMADuplicator(AppConfiguration appConfiguration){
        super(appConfiguration);
    }

    public void run(){
    	LOG.info("Run "+this.getClass().getName()+"...");
    	
	      try {
				initializeFlinkStreaming();
			} catch (Exception e) {
				LOG.error("Error on initializeFlinkStreaming",e);
				throw new RuntimeException("Error on initializeFlinkStreaming",e);
			}
    	
        	StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        	
            AppConfiguration.StreamConfig inputStreamConfig = appConfiguration.getInputStreamConfig();;
			String inputStreamName = inputStreamConfig.getStream().getStreamName();

			//TODO controllare modifiche dell SDP
			createStream(inputStreamConfig);
			LOG.info("============== input stream  =============== " + inputStreamName);
			
            
            List<AppConfiguration.StreamConfig> outputStreamConfigs = new ArrayList<AppConfiguration.StreamConfig>();
            Set<String> inputDuplicators = appConfiguration.getInputDuplicators();
            for (String outputStreamName : inputDuplicators) {
				try {
					StreamConfig outputStreamConfig = inputStreamConfig.clone();
					outputStreamConfig.updateStream(outputStreamConfig.getStream().getScope(), outputStreamConfig.getStream().getStreamName() + "-" + outputStreamName);
					outputStreamConfigs.add(outputStreamConfig);
				} catch (Throwable e) {
					LOG.error("Error generating duplicated output",e);
				}
			}
            if (outputStreamConfigs == null || outputStreamConfigs.isEmpty())
            	System.exit(-1);



            
            // Create EventStreamClientFactory
            PravegaConfig pravegaConfig = appConfiguration.getPravegaConfig();
            
           
			SourceFunction<NMAJSONData> sourceFunction = getSourceFunction(pravegaConfig, inputStreamName);
            
            DataStream<NMAJSONData> source = env.addSource(sourceFunction).name("Pravega."+inputStreamName);
            
			

			for (AppConfiguration.StreamConfig outputStreamConfig : outputStreamConfigs) {
				String outputStreamName = outputStreamConfig.getStream().getStreamName();
				createStream(outputStreamConfig);
				LOG.info("============== output stream  =============== " + outputStreamName);
				FlinkPravegaWriter<NMAJSONData> sink = getSinkFunction(pravegaConfig, outputStreamName);
				DataStreamSink<NMAJSONData> dsink = source.addSink(sink).name("Pravega."+outputStreamName);
			}
           

            // execute within the Flink environment
            try {
				env.execute(this.getClass().getName());
			} catch (Exception e) {
				LOG.error("Error executing "+this.getClass().getName()+"...",e);
				throw new RuntimeException("Error executing "+this.getClass().getName()+"...",e);
			}finally {
				LOG.info("Finally execute "+this.getClass().getName()+"...");	
			}

            

    }



	public static ProcessAllWindowFunction<NMAJSONData, Long, TimeWindow> getProcessAllWindowFunction() {
		ProcessAllWindowFunction<NMAJSONData, Long, TimeWindow> sumBytes = new ProcessAllWindowFunction<NMAJSONData, Long, TimeWindow>() {

			@Override
			public void process(ProcessAllWindowFunction<NMAJSONData, Long, TimeWindow>.Context ctx,
					Iterable<NMAJSONData> iterable, Collector<Long> collector) throws Exception {
				
				for (NMAJSONData element : iterable) {
					long totalTraffic = element.getBytesin() + element.getBytesout();
					LOG.info("Collecting: "+totalTraffic+" FROM "+element);
					collector.collect(totalTraffic);
				}
			}
			
		};
		return sumBytes;
	}
	
	public static ProcessWindowFunction<NMAJSONData, TotalTraffic, Date, TimeWindow> getProcessFunction() {
		ProcessWindowFunction<NMAJSONData, TotalTraffic, Date, TimeWindow> sumBytes = new ProcessWindowFunction<NMAJSONData, TotalTraffic, Date, TimeWindow>() {

			@Override
			public void process(Date key,ProcessWindowFunction<NMAJSONData, TotalTraffic, Date, TimeWindow>.Context ctx,Iterable<NMAJSONData> iterable, Collector<TotalTraffic> collector) throws Exception {
				for (NMAJSONData element : iterable) {
					long totalTraffic = element.getBytesin() + element.getBytesout();
					LOG.info("Collecting: "+totalTraffic+" FROM "+element);
					collector.collect(new TotalTraffic(key, totalTraffic));
				}
			}


			
		};
		return sumBytes;
	}



	@SuppressWarnings("unchecked")
	private FlinkPravegaReader<NMAJSONData> getSourceFunction(PravegaConfig pravegaConfig, String inputStreamName) {
		// create the Pravega source to read a stream of text
		FlinkPravegaReader<NMAJSONData> source = FlinkPravegaReader.builder()
				.withPravegaConfig(pravegaConfig)
		        .forStream(inputStreamName)
		        .withDeserializationSchema(new JsonDeserializationSchema(NMAJSONData.class))                    
		        .build();
		return source;
	}
	

	

	public static BoundedOutOfOrdernessTimestampExtractor<NMAJSONData> getTimestampAndWatermarkAssigner() {
		BoundedOutOfOrdernessTimestampExtractor<NMAJSONData> timestampAndWatermarkAssigner = new BoundedOutOfOrdernessTimestampExtractor<NMAJSONData>(Time.seconds(2)) {

		    @Override
		    public long extractTimestamp(NMAJSONData element) {
		        return element.getTime().getTime();
		    }
			
		};
		return timestampAndWatermarkAssigner;
	}


	public static FlinkPravegaWriter<NMAJSONData> getSinkFunction(PravegaConfig pravegaConfig, String outputStreamName) {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");

		
		JsonSerializationSchema<NMAJSONData> serializationSchema = new JsonSerializationSchema<NMAJSONData>();
		FlinkPravegaWriter<NMAJSONData> sink = FlinkPravegaWriter.<NMAJSONData>builder()
		        .withPravegaConfig(pravegaConfig)
		        .forStream(outputStreamName)
		        .withEventRouter(new PravegaEventRouter<NMAJSONData>() {

					@Override
					public String getRoutingKey(NMAJSONData event) {
						try {
							LOG.info(new String(serializationSchema.serialize(event),  "UTF-8"));
						} catch (UnsupportedEncodingException e) {
							//NOP
						}
						return NMADuplicator.class.getName();
					}

		        	
				})
                .withSerializationSchema(serializationSchema)
		        .build();
		return sink;
	}	

    public static void main(String[] args) throws Exception {
    	LOG.info("Main...");
        AppConfiguration appConfiguration = new AppConfiguration(args);
        NMADuplicator reader = new NMADuplicator(appConfiguration);
        reader.run();
    }

}
