package it.consulthink.oe.flink.packetcount;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.crypto.KeySelector;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.MultisetTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
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

import io.pravega.connectors.flink.FlinkPravegaReader;
import io.pravega.connectors.flink.FlinkPravegaWriter;
import io.pravega.connectors.flink.PravegaConfig;
import it.consulthink.oe.model.NMAJSONData;

public class DistinctIPReader2 extends AbstractApp {


    // Logger initialization
    private static final Logger LOG = LoggerFactory.getLogger(DistinctIPReader2.class);

    Set<String> ipList = appConfiguration.getMyIps();

    // The application reads data from specified Pravega stream and once every 10 seconds
    // prints the distinct words and counts from the previous 10 seconds.
    public DistinctIPReader2(AppConfiguration appConfiguration){
        super(appConfiguration);
    }

    public void run(){



        LOG.info("Starting NMA DistinctIPReader2...");

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

        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> dataStream = processSource(env, source, ipList);

        dataStream.printToErr();
        LOG.info("==============  ProcessSource - PRINTED  ===============");


        FlinkPravegaWriter<Tuple3<Date, Integer, Integer>> sink = getSinkFunction(pravegaConfig, outputStreamName);

        dataStream.flatMap(new FlatMapFunction<Tuple3<String, Integer, Integer>,Tuple3<Date, Integer, Integer>>() {
        	final SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			@Override
			public void flatMap(Tuple3<String, Integer, Integer> value, Collector<Tuple3<Date, Integer, Integer>> out)
					throws Exception {
				out.collect(Tuple3.of(df.parse(value.f0), value.f1, value.f2));
				
			}
		})
        .addSink(sink).name("NMADistinctIPReader2Stream");

        // create another output sink to print to stdout for verification
        dataStream.printToErr();
        LOG.info("==============  ProcessSink - PRINTED  ===============");


        try {
            env.execute("DistinctIPReader2");
        } catch (Exception e) {
            LOG.error("Error executing DistinctIPReader2...");
        } finally {
            LOG.info("Ending NMA DistinctIPReader2");
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


    private FlinkPravegaWriter<Tuple3<Date, Integer, Integer>> getSinkFunction(PravegaConfig pravegaConfig, String outputStreamName) {
    	final SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm");
        FlinkPravegaWriter<Tuple3<Date, Integer, Integer>> sink = FlinkPravegaWriter.<Tuple3<Date, Integer, Integer>>builder()
                .withPravegaConfig(pravegaConfig)
                .forStream(outputStreamName)
                .withEventRouter((x) -> "DistinctIP")
                .withSerializationSchema(new JsonSerializationSchema<Tuple3<Date, Integer, Integer>>())
                .build();
        return sink;
    }



    public static SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> processSource(StreamExecutionEnvironment env, DataStream<NMAJSONData> source, Set<String> ipList){
    	SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> result = null;
    	
		Map<String, Integer> ipsLocal = new HashMap<String, Integer>();
		Map<String, Integer> ipsExternal = new HashMap<String, Integer>();
		BroadcastStream<Map<String, Integer>> bcDistinctIpStream = env.fromElements(ipsLocal, ipsExternal).broadcast(distinctIpMapStateDesc);

    			
        //setting EventTime Characteristic
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //setting the Time and Wm Extractor
		BoundedOutOfOrdernessTimestampExtractor<NMAJSONData> timestampAndWatermarkAssigner = getTimestampAndWatermarkAssigner();
		final SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		
		
		

			KeyedBroadcastProcessFunction<String, NMAJSONData, Map<String, Integer>, Tuple3<String, Integer, Integer>> pf = getProcessFunctionWithMapState(ipList);
			BroadcastConnectedStream<NMAJSONData, Map<String, Integer>> connectedStream = source
					.assignTimestampsAndWatermarks(timestampAndWatermarkAssigner)
					.keyBy((line) -> {
	                    return df.format(line.getTime());
	                })
					.connect(bcDistinctIpStream);
			result = connectedStream
					.process(pf)
					.keyBy(t -> t.f0)
					.reduce(new ReduceFunction<Tuple3<String,Integer,Integer>>() {
						
						@Override
						public Tuple3<String, Integer, Integer> reduce(Tuple3<String, Integer, Integer> value1,	Tuple3<String, Integer, Integer> value2) throws Exception {
							if (!value1.f0.equals(value2.f0))
								throw new RuntimeException("Not Keyed");
								
							return Tuple3.of(value1.f0, Math.max(value1.f1, value2.f1), Math.max(value1.f2, value2.f2));
						}
					});
		

				
		return result;
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

    static final MapStateDescriptor<String, Map<String, Integer>> distinctIpMapStateDesc = new MapStateDescriptor<String, Map<String,Integer>>("DistinctIp",BasicTypeInfo.STRING_TYPE_INFO,new MultisetTypeInfo<String>(String.class));
    
    public static KeyedBroadcastProcessFunction<String, NMAJSONData, Map<String,Integer>, Tuple3<String, Integer, Integer> > getProcessFunctionWithMapState(final Set<String> myIpList) {


		    	
    	KeyedBroadcastProcessFunction<String, NMAJSONData, Map<String,Integer>, Tuple3<String, Integer, Integer> >  result = new KeyedBroadcastProcessFunction<String, NMAJSONData, Map<String,Integer>, Tuple3<String, Integer, Integer> >(){

        	

        	final SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

			@Override
			public void processElement(NMAJSONData data,
					KeyedBroadcastProcessFunction<String, NMAJSONData, Map<String,Integer>, Tuple3<String, Integer, Integer> >.ReadOnlyContext ctx,
					Collector<Tuple3<String, Integer, Integer>> out) throws Exception {

				MapState<String, Map<String, Integer>> ipsState = getRuntimeContext().getMapState(distinctIpMapStateDesc);
				Map<String, Integer> ipsLocal = ipsState.get("local");
				if (ipsLocal == null || ipsLocal.isEmpty()) {
					ipsLocal = new HashMap<String, Integer>();
				}

				Map<String, Integer> ipsExternal = ipsState.get("external");
				if (ipsExternal == null || ipsExternal.isEmpty()) {
					ipsExternal = new HashMap<String, Integer>();
				}
				
                insertIP(data.getSrc_ip(), ipsLocal, ipsExternal);
                insertIP(data.getDst_ip(), ipsLocal, ipsExternal);
                
                ipsState.put("local", ipsLocal);
                ipsState.put("external", ipsExternal);
                
                out.collect(Tuple3.of(df.format(data.getTime()), ipsLocal.size(), ipsExternal.size()));	
				
			}


            public Integer insertIP(String ip, Map<String, Integer> ipsLocal, Map<String, Integer> ipsExternal) {
            	if (myIpList.contains(ip)) {
            		return insertIPLocal(ip, ipsLocal);
            	}else {
            		return insertIPExternal(ip, ipsExternal);
            	}
            }
            
            public Integer insertIPExternal(String ip, Map<String, Integer> ipsExternal) {
            	if (ipsExternal.containsKey(ip)) {
            		ipsExternal.put(ip, ipsExternal.get(ip) + 1);
            	}else {
            		ipsExternal.put(ip, 1);
            	}
            	return ipsExternal.get(ip);
            }            
            
            public Integer insertIPLocal(String ip, Map<String, Integer> ipsLocal) {
            	if (ipsLocal.containsKey(ip)) {
            		ipsLocal.put(ip, ipsLocal.get(ip) + 1);
            	}else {
            		ipsLocal.put(ip, 1);
            	}
            	return ipsLocal.get(ip);
            }


			@Override
			public void processBroadcastElement(Map<String, Integer> value,
					KeyedBroadcastProcessFunction<String, NMAJSONData, Map<String, Integer>, Tuple3<String, Integer, Integer>>.Context ctx,
					Collector<Tuple3<String, Integer, Integer>> out) throws Exception {
				
				if (value != null && !value.isEmpty()) {
					String label = null;
					Set<String> keySet = value.keySet();
					for (String string : myIpList) {
						label = myIpList.contains(string) ? "local" : "external";
						break;
					}
					if (label != null) {
						LOG.info(label+" "+value);
						ctx.getBroadcastState(distinctIpMapStateDesc).put(label, value);
					}
				}

			}  
    		

    		
    	};

        return result;
    }
    
    public static ProcessWindowFunction<NMAJSONData, Tuple3<String, Integer, Integer>, String, TimeWindow> getProcessFunction(final Set<String> myIpList) {
    	
        ProcessWindowFunction<NMAJSONData,Tuple3<String, Integer, Integer>, String, TimeWindow> countByDirection = new
                ProcessWindowFunction<NMAJSONData, Tuple3<String, Integer, Integer>, String, TimeWindow>() {
        	
        	Hashtable<String,Long> ipsLocal = new Hashtable<String,Long>();
        	Hashtable<String,Long> ipsExternal = new Hashtable<String,Long>();
        	
            @Override
            public void process(String key, Context context, Iterable<NMAJSONData> elements, Collector< Tuple3<String, Integer, Integer> > out) throws Exception {
				for (NMAJSONData data : elements) {
                    insertIP(data.getSrc_ip());
                    insertIP(data.getDst_ip());
                }
                
                out.collect(Tuple3.of(key, ipsLocal.size(), ipsExternal.size()));
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
        LOG.info("Starting DistinctIPReader2...");
        AppConfiguration appConfiguration = new AppConfiguration(args);
        DistinctIPReader2 reader = new DistinctIPReader2(appConfiguration);
        reader.run();
    }



}
