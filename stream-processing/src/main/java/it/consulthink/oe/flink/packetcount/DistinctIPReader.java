package it.consulthink.oe.flink.packetcount;

import java.io.Serializable;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Set;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
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
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import io.pravega.connectors.flink.FlinkPravegaReader;
import io.pravega.connectors.flink.FlinkPravegaWriter;
import io.pravega.connectors.flink.PravegaConfig;
import it.consulthink.oe.model.NMAJSONData;

public class DistinctIPReader extends AbstractApp {


    // Logger initialization
    private static final Logger LOG = LoggerFactory.getLogger(DistinctIPReader.class);

    Set<String> ipList = appConfiguration.getMyIps();

    // The application reads data from specified Pravega stream and once every 10 seconds
    // prints the distinct words and counts from the previous 10 seconds.
    public DistinctIPReader(AppConfiguration appConfiguration){
        super(appConfiguration);
    }

    public void run(){



        LOG.info("Starting NMA DistinctIPReader...");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        AppConfiguration.StreamConfig inputStreamConfig = appConfiguration.getInputStreamConfig();
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

        SingleOutputStreamOperator<Tuple3<Date, Long, Long>> dataStream = processSource(env, source, ipList);

        dataStream.printToErr();
        LOG.info("==============  ProcessSource - PRINTED  ===============");


        FlinkPravegaWriter<Tuple3<Date, Long, Long>> sink = getSinkFunction(pravegaConfig, outputStreamName);

        dataStream.addSink(sink).name("NMADistinctIPReaderStream");

        // create another output sink to print to stdout for verification
        dataStream.printToErr();
        LOG.info("==============  ProcessSink - PRINTED  ===============");


        try {
            env.execute("DistinctIPReader");
        } catch (Exception e) {
            LOG.error("Error executing DistinctIPReader...");
        } finally {
            LOG.info("Ending NMA DistinctIPReader");
        }
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


    private FlinkPravegaWriter<Tuple3<Date, Long, Long>> getSinkFunction(PravegaConfig pravegaConfig, String outputStreamName) {

        FlinkPravegaWriter<Tuple3<Date, Long, Long>> sink = FlinkPravegaWriter.<Tuple3<Date, Long, Long>>builder()
                .withPravegaConfig(pravegaConfig)
                .forStream(outputStreamName)
                .withEventRouter((x) -> "DistinctIP")
                .withSerializationSchema(new JsonSerializationSchema<Tuple3<Date, Long, Long>>())
                .build();
        return sink;
    }



    public static SingleOutputStreamOperator<Tuple3<Date, Long, Long>> processSource(StreamExecutionEnvironment env, DataStream<NMAJSONData> source, Set<String> ipList){

        //setting EventTime Characteristic
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //setting the Time and Wm Extractor
		BoundedOutOfOrdernessTimestampExtractor<NMAJSONData> timestampAndWatermarkAssigner = getTimestampAndWatermarkAssigner();

		
		
		SingleOutputStreamOperator<Tuple3<Date, Long, Long>> dataStream = source
               .assignTimestampsAndWatermarks(timestampAndWatermarkAssigner)
               .keyBy((NMAJSONData x) -> x.getTime())
               .window(TumblingEventTimeWindows.of(Time.seconds(1)))
               .process(getProcessFunction(ipList))
               .windowAll(TumblingEventTimeWindows.of(Time.seconds(1)))
               .reduce((Tuple3<Date, Hashtable<String,Long>, Hashtable<String,Long>> v1, Tuple3<Date, Hashtable<String,Long>, Hashtable<String,Long>> v2) -> {				// reduce by merging events with same timestamp

                   Hashtable<String,Long> ipsLocal = new Hashtable<String,Long>();
                   ipsLocal.putAll(v2.f1);
                   Hashtable<String,Long> ipsExternal = new Hashtable<String,Long>();
                   ipsExternal.putAll(v2.f2);



                   Enumeration<String> keysV1F1 = v1.f1.keys();
                   while (keysV1F1.hasMoreElements()) {
                       String ip = (String) keysV1F1.nextElement();
                       if (ipsLocal.contains(ip)) {
                           ipsLocal.put(ip, ipsLocal.get(ip) + v1.f1.get(ip));
                       }else {
                           ipsLocal.put(ip, v1.f1.get(ip));
                       }
                   }

                   Enumeration<String> keysV1F2 = v1.f2.keys();
                   while (keysV1F1.hasMoreElements()) {
                       String ip = (String) keysV1F1.nextElement();
                       if (ipsExternal.contains(ip)) {
                           ipsExternal.put(ip, ipsLocal.get(ip) + v1.f1.get(ip));
                       }else {
                           ipsExternal.put(ip, v1.f1.get(ip));
                       }
                   }

                   return Tuple3.of(v1.f0, ipsLocal, ipsExternal);
               }).map(new MapFunction<Tuple3<Date,Hashtable<String,Long>,Hashtable<String,Long>>, Tuple3<Date, Long, Long>>() {

                    @Override
                    public Tuple3<Date, Long, Long> map(Tuple3<Date, Hashtable<String, Long>, Hashtable<String, Long>> t) throws Exception {

                        return Tuple3.of(t.f0, (long) t.f1.size(), (long) t.f2.size());
                    }

                });

        return dataStream;
    }


    public static BoundedOutOfOrdernessTimestampExtractor<NMAJSONData> getTimestampAndWatermarkAssigner() {
        BoundedOutOfOrdernessTimestampExtractor<NMAJSONData> timestampAndWatermarkAssigner = new BoundedOutOfOrdernessTimestampExtractor<NMAJSONData>(Time.seconds(3)) {
            @Override
            public long extractTimestamp(NMAJSONData element) {
                return element.getTime().getTime();
            }
        };
        return timestampAndWatermarkAssigner;
    }


    public static ProcessWindowFunction<NMAJSONData, Tuple3<Date, Hashtable<String,Long>, Hashtable<String,Long>>, Date, TimeWindow> getProcessFunction(final Set<String> myIpList) {
    	
        ProcessWindowFunction<NMAJSONData, Tuple3<Date, Hashtable<String,Long>, Hashtable<String,Long>>, Date, TimeWindow> countByDirection = new
                ProcessWindowFunction<NMAJSONData, Tuple3<Date, Hashtable<String,Long>, Hashtable<String,Long>>, Date, TimeWindow>() {
        	
        	Hashtable<String,Long> ipsLocal = new Hashtable<String,Long>();
        	Hashtable<String,Long> ipsExternal = new Hashtable<String,Long>();
        	
        	
            @Override
            public void process(Date key, Context context, Iterable<NMAJSONData> elements, Collector<Tuple3<Date, Hashtable<String,Long>, Hashtable<String,Long>>> out) throws Exception {
                for (NMAJSONData data : elements) {
                    insertIP(data.getSrc_ip());
                    insertIP(data.getDst_ip());
                }
                
                out.collect(Tuple3.of(key, ipsLocal, ipsExternal));
            }
            
            public Long insertIP(String ip) {
            	if (myIpList.contains(ip)) {
            		return insertIPLocal(ip);
            	}else {
            		return insertIPExternal(ip);
            	}
            }
            
            public Long insertIPExternal(String ip) {
            	if (ipsExternal.contains(ip)) {
            		ipsExternal.put(ip, ipsExternal.get(ip) + 1L);
            	}else {
            		ipsExternal.put(ip, 1L);
            	}
            	return ipsExternal.get(ip);
            }            
            
            public Long insertIPLocal(String ip) {
            	if (ipsLocal.contains(ip)) {
            		ipsLocal.put(ip, ipsLocal.get(ip) + 1L);
            	}else {
            		ipsLocal.put(ip, 1L);
            	}
            	return ipsLocal.get(ip);
            }            
        };

        return countByDirection;
    }



    public static void main(String[] args) throws Exception {
        LOG.info("Starting DistinctIPReader...");
        AppConfiguration appConfiguration = new AppConfiguration(args);
        DistinctIPReader reader = new DistinctIPReader(appConfiguration);
        reader.run();
    }



}
