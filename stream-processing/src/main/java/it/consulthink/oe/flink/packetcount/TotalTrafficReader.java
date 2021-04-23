package it.consulthink.oe.flink.packetcount;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
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
			createStream(inputStreamConfig);
			LOG.info("============== input stream  =============== " + inputStreamName);
			
            AppConfiguration.StreamConfig outputStreamConfig= appConfiguration.getOutputStreamConfig();
			String outputStreamName = outputStreamConfig.getStream().getStreamName();
			createStream(outputStreamConfig);
			LOG.info("============== output stream  =============== " + outputStreamName);
            
            
            // Create EventStreamClientFactory
            PravegaConfig pravegaConfig = appConfiguration.getPravegaConfig();
            LOG.info("============== Praevega  =============== " + pravegaConfig);
            
           
			SourceFunction<NMAJSONData> sourceFunction = getSourceFunction(pravegaConfig, inputStreamName);
            LOG.info("==============  SourceFunction  =============== " + sourceFunction);
            
            DataStream<NMAJSONData> source = env.addSource(sourceFunction).name("InputSource");
            LOG.info("==============  Source  =============== " + source);
            
			SingleOutputStreamOperator<Tuple2<Date, Long>> dataStream = processSource(env, source);

            dataStream.printToErr();
            LOG.info("==============  ProcessSource - PRINTED  ===============");
            
			
            
            FlinkPravegaWriter<Tuple2<Date, Long>> sink = getSinkFunction(pravegaConfig, outputStreamName);
            
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
	
	public static ProcessWindowFunction<NMAJSONData, Tuple2<Date, Long>, Date, TimeWindow> getProcessFunction() {
		ProcessWindowFunction<NMAJSONData, Tuple2<Date, Long>, Date, TimeWindow> sumBytes = new ProcessWindowFunction<NMAJSONData, Tuple2<Date, Long>, Date, TimeWindow>() {

			@Override
			public void process(Date key,ProcessWindowFunction<NMAJSONData, Tuple2<Date, Long>, Date, TimeWindow>.Context ctx,Iterable<NMAJSONData> iterable, Collector<Tuple2<Date, Long>> collector) throws Exception {
				for (NMAJSONData element : iterable) {
					long totalTraffic = element.getBytesin() + element.getBytesout();
					LOG.info("Collecting: "+totalTraffic+" FROM "+element);
					collector.collect(Tuple2.of(key, totalTraffic));
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
	public static SingleOutputStreamOperator<Tuple2<Date, Long>> processSource(StreamExecutionEnvironment env, DataStream<NMAJSONData> source) {
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		
		BoundedOutOfOrdernessTimestampExtractor<NMAJSONData> timestampAndWatermarkAssigner = getTimestampAndWatermarkAssigner();

		
		
		SingleOutputStreamOperator<Tuple2<Date, Long>> dataStream = source
				.assignTimestampsAndWatermarks(timestampAndWatermarkAssigner)           // extract timestamp and wm strategy
				.keyBy((NMAJSONData x) -> x.getTime())                                  // key by date
				.window(TumblingEventTimeWindows.of(Time.seconds(1)))                   // aggregate in 1 sec windows
				.process(getProcessFunction())                                          // process elements in window
				.keyBy((Tuple2<Date,Long> x) -> x.f0)                                   // key all processed elements by date (again)
				.window(TumblingEventTimeWindows.of(Time.seconds(1)))                   // aggregate them in 1 sec windows
				.reduce((Tuple2<Date,Long> v1, Tuple2<Date,Long> v2) -> {				// reduce by merging events with same timestamp
					if(v1.f0.equals(v2.f0))
						return Tuple2.of(v1.f0,v1.f1 + v2.f1);
					LOG.error(""+v1+" "+v2);
					throw new RuntimeException();
				});

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
	
	private FlinkPravegaWriter<Tuple2<Date, Long>> getSinkFunction(PravegaConfig pravegaConfig, String outputStreamName) {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		
		
		FlinkPravegaWriter<Tuple2<Date, Long>> sink = FlinkPravegaWriter.<Tuple2<Date, Long>>builder()
		        .withPravegaConfig(pravegaConfig)
		        .forStream(outputStreamName)
		        .withEventRouter(new PravegaEventRouter<Tuple2<Date, Long>>() {

					@Override
					public String getRoutingKey(Tuple2<Date, Long> event) {
						return df.format(event.f0);
					}

		        	
				})
                .withSerializationSchema(new JsonSerializationSchema<Tuple2<Date, Long>>())
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
