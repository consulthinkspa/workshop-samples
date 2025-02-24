package it.consulthink.oe.flink.packetcount;

import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dellemc.oe.serialization.JsonDeserializationSchema;
import com.dellemc.oe.serialization.JsonSerializationSchema;
import com.dellemc.oe.util.AbstractApp;
import com.dellemc.oe.util.AppConfiguration;

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
public class TotalTrafficReader extends AbstractApp{

    // Logger initialization
    private static final Logger LOG = LoggerFactory.getLogger(TotalTrafficReader.class);


    // The application reads data from specified Pravega stream and once every 10 seconds
    // prints the distinct words and counts from the previous 10 seconds.
    public TotalTrafficReader(AppConfiguration appConfiguration){
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
			
            AppConfiguration.StreamConfig outputStreamConfig= appConfiguration.getOutputStreamConfig();
			String outputStreamName = outputStreamConfig.getStream().getStreamName();

			//TODO controllare modifiche dell SDP
			createStream(outputStreamConfig);
			LOG.info("============== output stream  =============== " + outputStreamName);
            
            
            // Create EventStreamClientFactory
            PravegaConfig pravegaConfig = appConfiguration.getPravegaConfig();
            
           
			SourceFunction<NMAJSONData> sourceFunction = getSourceFunction(pravegaConfig, inputStreamName);
            
            DataStream<NMAJSONData> source = env.addSource(sourceFunction).name("Pravega."+inputStreamName);
            
			SingleOutputStreamOperator<TotalTraffic> dataStream = processSource(env, source);

//            dataStream.printToErr();
            
			
            
            FlinkPravegaWriter<TotalTraffic> sink = getSinkFunction(pravegaConfig, outputStreamName);
//            dataStream.printToErr();
            DataStreamSink<TotalTraffic> dataStreamSink = dataStream.addSink(sink).name("Pravega."+outputStreamName);

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
	

	
	@SuppressWarnings("unchecked")
	public static SingleOutputStreamOperator<TotalTraffic> processSource(StreamExecutionEnvironment env, DataStream<NMAJSONData> source) {
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		
		BoundedOutOfOrdernessTimestampExtractor<NMAJSONData> timestampAndWatermarkAssigner = getTimestampAndWatermarkAssigner();

		
		
		SingleOutputStreamOperator<TotalTraffic> dataStream = source
				.assignTimestampsAndWatermarks(timestampAndWatermarkAssigner)           // extract timestamp and wm strategy
				.keyBy((NMAJSONData x) -> x.getTime())                                  // key by date
				.window(TumblingEventTimeWindows.of(Time.seconds(1)))                   // aggregate in 1 sec windows
				.process(getProcessFunction())                                          // process elements in window
				.keyBy((TotalTraffic x) -> x.getTime())                                   // key all processed elements by date (again)
				.window(TumblingEventTimeWindows.of(Time.seconds(1)))                   // aggregate them in 1 sec windows
				.reduce((TotalTraffic v1, TotalTraffic v2) -> {				// reduce by merging events with same timestamp
					if(v1.getTime().equals(v2.getTime()))
						return new TotalTraffic(v1.getTime(), v1.getValue() + v2.getValue());
					LOG.error(""+v1+" "+v2);
					throw new RuntimeException();
				});

		return dataStream;
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


	public static FlinkPravegaWriter<TotalTraffic> getSinkFunction(PravegaConfig pravegaConfig, String outputStreamName) {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");

		
		JsonSerializationSchema<TotalTraffic> serializationSchema = new JsonSerializationSchema<TotalTraffic>();
		FlinkPravegaWriter<TotalTraffic> sink = FlinkPravegaWriter.<TotalTraffic>builder()
		        .withPravegaConfig(pravegaConfig)
		        .forStream(outputStreamName)
		        .withEventRouter(new PravegaEventRouter<TotalTraffic>() {

					@Override
					public String getRoutingKey(TotalTraffic event) {
						try {
							LOG.info(new String(serializationSchema.serialize(event),  "UTF-8"));
						} catch (UnsupportedEncodingException e) {
							//NOP
						}
						return TotalTrafficReader.class.getName();
					}

		        	
				})
                .withSerializationSchema(serializationSchema)
		        .build();
		return sink;
	}	

    public static void main(String[] args) throws Exception {
    	LOG.info("Main...");
        AppConfiguration appConfiguration = new AppConfiguration(args);
        TotalTrafficReader reader = new TotalTrafficReader(appConfiguration);
        reader.run();
    }

}
