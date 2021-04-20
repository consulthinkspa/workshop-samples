package it.consulthink.oe.flink.packetcount;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dellemc.oe.serialization.JsonDeserializationSchema;
import com.dellemc.oe.util.AbstractApp;
import com.dellemc.oe.util.AppConfiguration;

import io.pravega.connectors.flink.FlinkPravegaReader;
import io.pravega.connectors.flink.FlinkPravegaWriter;
import io.pravega.connectors.flink.PravegaConfig;
import io.pravega.connectors.flink.PravegaEventRouter;
import it.consulthink.oe.model.NMAJSONData;

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
public class TotalTrafficReader extends AbstractApp {

    // Logger initialization
    private static final Logger LOG = LoggerFactory.getLogger(TotalTrafficReader.class);


    // The application reads data from specified Pravega stream and once every 10 seconds
    // prints the distinct words and counts from the previous 10 seconds.
    public TotalTrafficReader(AppConfiguration appConfiguration){
        super(appConfiguration);
    }

    public void run(){
    		LOG.info("Starting NMA TotalTrafficReader...");
    	
        	StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        	
            AppConfiguration.StreamConfig inputStreamConfig = appConfiguration.getInputStreamConfig();;
			String inputStreamName = inputStreamConfig.getStream().getStreamName();
			LOG.info("============== input stream  =============== " + inputStreamName);
			
            AppConfiguration.StreamConfig outputStreamConfig= appConfiguration.getOutputStreamConfig();
			String outputStreamName = outputStreamConfig.getStream().getStreamName();
			LOG.info("============== output stream  =============== " + outputStreamName);
            
            
            // Create EventStreamClientFactory
            PravegaConfig pravegaConfig = appConfiguration.getPravegaConfig();
            LOG.info("============== Praevega  =============== " + pravegaConfig);
            
           
			SourceFunction<NMAJSONData> sourceFunction = getSourceFunction(pravegaConfig, inputStreamName);
            LOG.info("==============  SourceFunction  =============== " + sourceFunction);
            
            DataStream<NMAJSONData> source = env.addSource(sourceFunction).name("InputSource");
            LOG.info("==============  Source  =============== " + source);
            
			SingleOutputStreamOperator<Long> dataStream = processSource(env, source);

            dataStream.printToErr();
            LOG.info("==============  ProcessSource - PRINTED  ===============");
            
			
            
            FlinkPravegaWriter<Long> sink = getSinkFunction(pravegaConfig, outputStreamName);
            
            dataStream.addSink(sink).name("NMATotalTrafficStream");

            // create another output sink to print to stdout for verification
            dataStream.printToErr();
            LOG.info("==============  ProcessSink - PRINTED  ===============");
            
            
            
            // execute within the Flink environment
            try {
				env.execute("TotalTrafficReader");
			} catch (Exception e) {
				LOG.error("Error executing TotalTrafficReader...");	
			}finally {
				LOG.info("Ending NMA TotalTrafficReader...");	
			}

            

    }



	public static ProcessAllWindowFunction<NMAJSONData, Long, TimeWindow> getProcessFunction() {
		ProcessAllWindowFunction<NMAJSONData, Long, TimeWindow> sumBytes = new ProcessAllWindowFunction<NMAJSONData, Long, TimeWindow>() {

			@Override
			public void process(ProcessAllWindowFunction<NMAJSONData, Long, TimeWindow>.Context ctx,
					Iterable<NMAJSONData> iterable, Collector<Long> collector) throws Exception {
				
				for (NMAJSONData element : iterable) {
					collector.collect(element.getBytesin() + element.getBytesout());
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
	
	@SuppressWarnings("unchecked")
	public static SingleOutputStreamOperator<Long> processSource(StreamExecutionEnvironment env, DataStream<NMAJSONData> source) {
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		
		BoundedOutOfOrdernessTimestampExtractor<NMAJSONData> timestampAndWatermarkAssigner = getTimestampAndWatermarkAssigner();
		SingleOutputStreamOperator<Long> dataStream = source
				.assignTimestampsAndWatermarks(timestampAndWatermarkAssigner)
				.windowAll(TumblingEventTimeWindows.of(Time.seconds(1)))
				.process(getProcessFunction());
		return dataStream;
	}

	public static BoundedOutOfOrdernessTimestampExtractor<NMAJSONData> getTimestampAndWatermarkAssigner() {
		BoundedOutOfOrdernessTimestampExtractor<NMAJSONData> timestampAndWatermarkAssigner = new BoundedOutOfOrdernessTimestampExtractor<NMAJSONData>(Time.seconds(10)) {

		    @Override
		    public long extractTimestamp(NMAJSONData element) {
		        return element.getTime().getTime();
		    }
			
		};
		return timestampAndWatermarkAssigner;
	}
	
	private FlinkPravegaWriter<Long> getSinkFunction(PravegaConfig pravegaConfig, String outputStreamName) {
		FlinkPravegaWriter<Long> sink = FlinkPravegaWriter.<Long>builder()
		        .withPravegaConfig(pravegaConfig)
		        .forStream(outputStreamName)
		        .withEventRouter(new PravegaEventRouter<Long>() {

					@Override
					public String getRoutingKey(Long event) {
						return "TotalTraffic";
					}
		        	
				})
		        //TODO controllare la necessita dello scema
//                .withSerializationSchema(???)
		        .build();
		return sink;
	}	

    public static void main(String[] args) throws Exception {
        LOG.info("Starting TotalTrafficReader...");
        AppConfiguration appConfiguration = new AppConfiguration(args);
        TotalTrafficReader reader = new TotalTrafficReader(appConfiguration);
        reader.run();
    }

}
