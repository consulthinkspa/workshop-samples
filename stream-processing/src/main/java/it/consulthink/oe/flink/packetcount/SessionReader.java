package it.consulthink.oe.flink.packetcount;

import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.MultisetTypeInfo;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
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

public class SessionReader extends AbstractApp {


    // Logger initialization
    private static final Logger LOG = LoggerFactory.getLogger(SessionReader.class);

    Set<String> ipList = appConfiguration.getMyIps();

    // The application reads data from specified Pravega stream and once every 10 seconds
    // prints the distinct words and counts from the previous 10 seconds.
    public SessionReader(AppConfiguration appConfiguration){
        super(appConfiguration);
    }

    public void run(){



        LOG.info("Starting NMA SessionReader2...");

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

        SingleOutputStreamOperator<Tuple4<String, Integer, Integer, Integer>> dataStream = processSource(env, source);

        dataStream.printToErr();
        LOG.info("==============  ProcessSource - PRINTED  ===============");


        FlinkPravegaWriter<Tuple4<Date, Integer, Integer, Integer>> sink = getSinkFunction(pravegaConfig, outputStreamName);

        dataStream.flatMap(new FlatMapFunction<Tuple4<String, Integer, Integer, Integer>,Tuple4<Date, Integer, Integer, Integer>>() {
        	final SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			@Override
			public void flatMap(Tuple4<String, Integer, Integer, Integer> value, Collector<Tuple4<Date, Integer, Integer, Integer>> out)
					throws Exception {
				out.collect(Tuple4.of(df.parse(value.f0), value.f1, value.f2, value.f3));
				
			}
		})
        .addSink(sink).name("NMASessionReader2Stream");

        // create another output sink to print to stdout for verification
        dataStream.printToErr();
        LOG.info("==============  ProcessSink - PRINTED  ===============");


        try {
            env.execute("SessionReader2");
        } catch (Exception e) {
            LOG.error("Error executing SessionReader2...");
        } finally {
            LOG.info("Ending NMA SessionReader2");
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


    private FlinkPravegaWriter<Tuple4<Date, Integer, Integer, Integer>> getSinkFunction(PravegaConfig pravegaConfig, String outputStreamName) {
    	final SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm");
        FlinkPravegaWriter<Tuple4<Date, Integer, Integer, Integer>> sink = FlinkPravegaWriter.<Tuple4<Date, Integer, Integer, Integer>>builder()
                .withPravegaConfig(pravegaConfig)
                .forStream(outputStreamName)
                .withEventRouter((x) -> "Sessions")
                .withSerializationSchema(new JsonSerializationSchema<Tuple4<Date, Integer, Integer, Integer>>())
                .build();
        return sink;
    }



    public static SingleOutputStreamOperator<Tuple4<String, Integer, Integer, Integer>> processSource(StreamExecutionEnvironment env, DataStream<NMAJSONData> source){
    	SingleOutputStreamOperator<Tuple4<String, Integer, Integer, Integer>> result = null;
    	
		Map<String, NMAJSONData> opening = new HashMap<String, NMAJSONData>();
		Map<String, NMAJSONData> active = new HashMap<String, NMAJSONData>();
		Map<String, NMAJSONData> closing = new HashMap<String, NMAJSONData>();
		BroadcastStream<Map<String, NMAJSONData>> bcSessionsStream = env.fromElements(opening,active,closing).broadcast(sessionsMapStateDesc);

    			
        //setting EventTime Characteristic
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //setting the Time and Wm Extractor
		BoundedOutOfOrdernessTimestampExtractor<NMAJSONData> timestampAndWatermarkAssigner = getTimestampAndWatermarkAssigner();
		final SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		
		
		

			KeyedBroadcastProcessFunction<String, NMAJSONData, Map<String, NMAJSONData>, Tuple4<String, Integer, Integer, Integer>> pf = getProcessFunctionWithMapState();
			BroadcastConnectedStream<NMAJSONData, Map<String, NMAJSONData>> connectedStream = source
					.assignTimestampsAndWatermarks(timestampAndWatermarkAssigner)
					.keyBy((line) -> {
	                    return line.getSessionHash();
	                }).window(TumblingEventTimeWindows.of(Time.seconds(1)))
					.reduce(new ReduceFunction<NMAJSONData>() {

						@Override
						public NMAJSONData reduce(NMAJSONData value1, NMAJSONData value2) throws Exception {

	            			
							NMAJSONData result =				new NMAJSONData(
	            					new Date(Math.min(value1.getTime().getTime(), value2.getTime().getTime())), 
	            					value1.getSrc_ip(), 
	            					value1.getDst_ip(), 
	            					value1.getDport(), 
	            					value1.getSport(), 
	            					value1.getBytesin() + value2.getBytesin(), 
	            					value1.getBytesout() + value2.getBytesout(), 
	            					value1.getPkts() + value2.getPkts(), 
	            					value1.getPktsin(), value2.getPktsin(), 
	            					value1.getSynin() + value2.getSynin(), 
	            					value1.getSynackout() + value2.getSynackout(), 
	            					value1.getRstin() + value2.getRstin(), 
	            					value1.getRstout() + value2.getRstout(), 
	            					value1.getFin() + value2.getFin(),
	            					value1.getGet() + value2.getGet(),
	            					value1.getPost() +value2.getPost()
	            				);
							return result;
							
						}
						
					})
					
					
					.keyBy((line) -> {
	                    return df.format(line.getTime());
	                })
					.connect(bcSessionsStream);
			result = connectedStream
					.process(pf)
					.keyBy(t -> t.f0)
					.windowAll(TumblingEventTimeWindows.of(Time.seconds(1)))
					.reduce(new ReduceFunction<Tuple4<String,Integer,Integer,Integer>>() {
						
						@Override
						public Tuple4<String, Integer, Integer, Integer> reduce(Tuple4<String,Integer,Integer,Integer> value1,	Tuple4<String,Integer,Integer,Integer> value2) throws Exception {
							if (!value1.f0.equals(value2.f0))
								throw new RuntimeException("Not Keyed");
								
							return Tuple4.of(value1.f0, Math.max(value1.f1, value2.f1), Math.max(value1.f2, value2.f2), Math.max(value1.f3, value2.f3));
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

    static final MapStateDescriptor<String, Map<String, NMAJSONData>> sessionsMapStateDesc = new MapStateDescriptor<String, Map<String,NMAJSONData>>("Sessions",BasicTypeInfo.STRING_TYPE_INFO,new MapTypeInfo<String,NMAJSONData>(String.class, NMAJSONData.class));
    
    public static KeyedBroadcastProcessFunction<String, NMAJSONData, Map<String,NMAJSONData>, Tuple4<String, Integer, Integer, Integer> > getProcessFunctionWithMapState() {


		    	
    	KeyedBroadcastProcessFunction<String, NMAJSONData, Map<String,NMAJSONData>, Tuple4<String, Integer, Integer, Integer> >  result = new KeyedBroadcastProcessFunction<String, NMAJSONData, Map<String,NMAJSONData>, Tuple4<String, Integer, Integer, Integer> > (){

        	

        	final SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

			@Override
			public void processElement(NMAJSONData data,
					KeyedBroadcastProcessFunction<String, NMAJSONData, Map<String,NMAJSONData>, Tuple4<String, Integer, Integer, Integer> > .ReadOnlyContext ctx,
					Collector<Tuple4<String, Integer, Integer, Integer>> out) throws Exception {

				MapState<String, Map<String, NMAJSONData>> sessions = getRuntimeContext().getMapState(sessionsMapStateDesc);
				Map<String, NMAJSONData> openingSession = sessions.get(NMAJSONData.OPENING);
				if (openingSession == null || openingSession.isEmpty()) {
					openingSession = new HashMap<String, NMAJSONData>();
				}

				Map<String, NMAJSONData> activeSession = sessions.get(NMAJSONData.ACTIVE);
				if (activeSession == null || activeSession.isEmpty()) {
					activeSession = new HashMap<String, NMAJSONData>();
				}
				
				Map<String, NMAJSONData> closingSession = sessions.get(NMAJSONData.CLOSING);
				if (closingSession == null || closingSession.isEmpty()) {
					closingSession = new HashMap<String, NMAJSONData>();
				}				
				
                insertSession(data, openingSession, activeSession, closingSession);
                
                int currentClosingSessions = closingSession.size();
                resetClosingSession(data, closingSession);
                
                sessions.put(NMAJSONData.OPENING, openingSession);
                sessions.put(NMAJSONData.ACTIVE, activeSession);
                sessions.put(NMAJSONData.CLOSING, closingSession);
                
                
				out.collect(Tuple4.of(df.format(data.getTime()), openingSession.size(), activeSession.size(), currentClosingSessions));	
				
			}


            private void resetClosingSession(NMAJSONData data, Map<String, NMAJSONData> closingSession) {
            	
				if(data.getSessionState().equals(NMAJSONData.CLOSING) && closingSession.containsKey(data.getSessionHash())) {
					NMAJSONData old = closingSession.get(data.getSessionHash());
					closingSession.clear();
					closingSession.put(data.getSessionHash(), old);
				}else {
					closingSession.clear();
				}
				
			}


			public void insertSession(NMAJSONData data, Map<String, NMAJSONData> openingSession, Map<String, NMAJSONData> activeSession, Map<String, NMAJSONData> closingSession) {
            	if (data.getSessionState().equals(NMAJSONData.OPENING)) {
        
            		if(openingSession.containsKey(data.getSessionHash())) {
            			NMAJSONData old = openingSession.get(data.getSessionHash());
            			openingSession.put(data.getSessionHash(), 
            				new NMAJSONData(
            					old.getTime(), 
            					old.getSrc_ip(), 
            					old.getDst_ip(), 
            					old.getDport(), 
            					old.getSport(), 
            					old.getBytesin() + data.getBytesin(), 
            					old.getBytesout() + data.getBytesout(), 
            					old.getPkts() + data.getPkts(), 
            					old.getPktsin(), data.getPktsin(), 
            					old.getSynin() + data.getSynin(), 
            					old.getSynackout() + data.getSynackout(), 
            					old.getRstin() + data.getRstin(), 
            					old.getRstout() + data.getRstout(), 
            					old.getFin() + data.getFin(),
            					old.getGet() + data.getGet(),
            					old.getPost() +data.getPost())
            				);
            		}else {
            			openingSession.put(data.getSessionHash(), data);
            		}
            		
            		if(activeSession.containsKey(data.getSessionHash()))
            			activeSession.remove(data.getSessionHash());
            		
            		if(closingSession.containsKey(data.getSessionHash()))
            			closingSession.remove(data.getSessionHash());
            		

            		
            	}else if (data.getSessionState().equals(NMAJSONData.ACTIVE)){

            		if(openingSession.containsKey(data.getSessionHash())) {
            			NMAJSONData old = openingSession.get(data.getSessionHash());
            			openingSession.remove(data.getSessionHash());
                		if(!activeSession.containsKey(data.getSessionHash()) && old != null) {
                			activeSession.put(old.getSessionHash(), old);
                		}
            		}
            		

            		
            		if(activeSession.containsKey(data.getSessionHash())) {
            			NMAJSONData old = activeSession.get(data.getSessionHash());
            			activeSession.put(data.getSessionHash(), 
            				new NMAJSONData(
            					old.getTime(), 
            					old.getSrc_ip(), 
            					old.getDst_ip(), 
            					old.getDport(), 
            					old.getSport(), 
            					old.getBytesin() + data.getBytesin(), 
            					old.getBytesout() + data.getBytesout(), 
            					old.getPkts() + data.getPkts(), 
            					old.getPktsin(), data.getPktsin(), 
            					old.getSynin() + data.getSynin(), 
            					old.getSynackout() + data.getSynackout(), 
            					old.getRstin() + data.getRstin(), 
            					old.getRstout() + data.getRstout(), 
            					old.getFin() + data.getFin(),
            					old.getGet() + data.getGet(),
            					old.getPost() +data.getPost())
            				);
            		}else {
            			activeSession.put(data.getSessionHash(), data);
            		}            		
            			
            		
            		if(closingSession.containsKey(data.getSessionHash()))
            			closingSession.remove(data.getSessionHash());
            		
            	}else if (data.getSessionState().equals(NMAJSONData.CLOSING)){
            		
            		if(openingSession.containsKey(data.getSessionHash())) {
            			NMAJSONData old = openingSession.get(data.getSessionHash());
            			openingSession.remove(data.getSessionHash());
                		if(!closingSession.containsKey(data.getSessionHash()) && old != null) {
                			closingSession.put(old.getSessionHash(), old);
                		}
            		}
            		
            		if(activeSession.containsKey(data.getSessionHash())) {
            			NMAJSONData old = activeSession.get(data.getSessionHash());
            			activeSession.remove(data.getSessionHash());
                		if(!closingSession.containsKey(data.getSessionHash()) && old != null) {
                			closingSession.put(old.getSessionHash(), old);
                		}
            		}            		
            		

            		
            		if(closingSession.containsKey(data.getSessionHash())) {
            			NMAJSONData old = closingSession.get(data.getSessionHash());
            			closingSession.put(data.getSessionHash(), 
            				new NMAJSONData(
            					old.getTime(), 
            					old.getSrc_ip(), 
            					old.getDst_ip(), 
            					old.getDport(), 
            					old.getSport(), 
            					old.getBytesin() + data.getBytesin(), 
            					old.getBytesout() + data.getBytesout(), 
            					old.getPkts() + data.getPkts(), 
            					old.getPktsin(), data.getPktsin(), 
            					old.getSynin() + data.getSynin(), 
            					old.getSynackout() + data.getSynackout(), 
            					old.getRstin() + data.getRstin(), 
            					old.getRstout() + data.getRstout(), 
            					old.getFin() + data.getFin(),
            					old.getGet() + data.getGet(),
            					old.getPost() +data.getPost())
            				);
            		}else {
            			closingSession.put(data.getSessionHash(), data);
            		}            		
            		
            	}else {
            		throw new RuntimeException("NMAJSONDATA State not possible: "+data);
            	}
            }
            



			@Override
			public void processBroadcastElement(Map<String, NMAJSONData> value,
					KeyedBroadcastProcessFunction<String, NMAJSONData, Map<String, NMAJSONData>, Tuple4<String, Integer, Integer, Integer>>.Context ctx,
					Collector<Tuple4<String, Integer, Integer, Integer>> out) throws Exception {
				
				if (value != null && !value.isEmpty()) {
					String label = null;
					Collection<NMAJSONData> datas = value.values();
					for (NMAJSONData data : datas) {
						label = data.getSessionState();
						break;
					}
					
					if (label != null) {
						LOG.info(label+" "+value);
						ctx.getBroadcastState(sessionsMapStateDesc).put(label, value);
					}
				}

			}  
    		

    		
    	};

        return result;
    }
    



    public static void main(String[] args) throws Exception {
        LOG.info("Starting SessionReader...");
        AppConfiguration appConfiguration = new AppConfiguration(args);
        SessionReader reader = new SessionReader(appConfiguration);
        reader.run();
    }



}
