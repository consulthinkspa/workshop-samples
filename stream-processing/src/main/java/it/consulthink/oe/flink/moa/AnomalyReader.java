package it.consulthink.oe.flink.moa;

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
import com.yahoo.labs.samoa.instances.DenseInstance;
import com.yahoo.labs.samoa.instances.InstanceImpl;

import io.pravega.connectors.flink.FlinkPravegaReader;
import io.pravega.connectors.flink.FlinkPravegaWriter;
import io.pravega.connectors.flink.PravegaConfig;
import it.consulthink.oe.ingest.NMAJSONDataGenerator;
import it.consulthink.oe.model.NMAJSONData;
import moa.cluster.Cluster;
import moa.cluster.Clustering;
import moa.clusterers.streamkm.StreamKM;
import moa.core.AutoExpandVector;

public class AnomalyReader extends AbstractApp {


    private static final String STREAM_KM = "StreamKM";

	// Logger initialization
    private static final Logger LOG = LoggerFactory.getLogger(AnomalyReader.class);

    Set<String> ipList = appConfiguration.getMyIps();

    // The application reads data from specified Pravega stream and once every 10 seconds
    // prints the distinct words and counts from the previous 10 seconds.
    public AnomalyReader(AppConfiguration appConfiguration){
        super(appConfiguration);
    }

    public void run(){



        LOG.info("Starting NMA AnomalyReader...");

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

        
        trainStreamKM(appConfiguration, 500);
        
        SingleOutputStreamOperator<Tuple3<NMAJSONData, Double, Cluster>> dataStream = processSource(env, source);

        dataStream.printToErr();
        LOG.info("==============  ProcessSource - PRINTED  ===============");


        FlinkPravegaWriter<Tuple3<NMAJSONData, Double, Cluster>> sink = getSinkFunction(pravegaConfig, outputStreamName);

        dataStream
        .addSink(sink).name("NMAAnomalyReaderStream");

        // create another output sink to print to stdout for verification
        dataStream.printToErr();
        LOG.info("==============  ProcessSink - PRINTED  ===============");


        try {
            env.execute("AnomalyReader");
        } catch (Exception e) {
            LOG.error("Error executing AnomalyReader...");
        } finally {
            LOG.info("Ending NMA AnomalyReader");
        }
    }

	public static void trainStreamKM(AppConfiguration appConfiguration, long elements) {
		NMAJSONDataGenerator.generateInfiniteStreamNoAnomaly(appConfiguration.getMyIps())
		.limit(elements)
        .map(data ->{
	        //TODO
	        return toDenseInstance(data);
        })
        .forEach(inst ->{
        	
        	try {
				getDefaultStreamKM().trainOnInstance(inst);
			} catch (Throwable e) {
				LOG.error(e.getMessage());
			}
        });
	}

	public static DenseInstance toDenseInstance(NMAJSONData data) {
		DenseInstance instance = new DenseInstance(3);
		instance.setValue(0, Math.min((data.getPkts())/512d, 1d));
		instance.setValue(1, Math.min((data.getBytesin())/85000d, 1d) );
		instance.setValue(2, Math.min((data.getBytesout())/85000d, 1d));
		return instance;
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


    private FlinkPravegaWriter<Tuple3<NMAJSONData, Double, Cluster>> getSinkFunction(PravegaConfig pravegaConfig, String outputStreamName) {
    	final SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm");
        FlinkPravegaWriter<Tuple3<NMAJSONData, Double, Cluster>> sink = FlinkPravegaWriter.<Tuple3<NMAJSONData, Double, Cluster>>builder()
                .withPravegaConfig(pravegaConfig)
                .forStream(outputStreamName)
                .withEventRouter((x) -> "Anomalies")
                .withSerializationSchema(new JsonSerializationSchema<Tuple3<NMAJSONData, Double, Cluster>>())
                .build();
        return sink;
    }



    public static SingleOutputStreamOperator<Tuple3<NMAJSONData, Double, Cluster>> processSource(StreamExecutionEnvironment env, DataStream<NMAJSONData> source){
    	SingleOutputStreamOperator<Tuple3<NMAJSONData, Double, Cluster>> result = null;
    	
    	
        StreamKM streamKM = getDefaultStreamKM();
    	
        
        
        
		BroadcastStream<StreamKM> kmStream = env.fromElements(streamKM).broadcast(kmStateDesc);

    			
        //setting EventTime Characteristic
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //setting the Time and Wm Extractor
		BoundedOutOfOrdernessTimestampExtractor<NMAJSONData> timestampAndWatermarkAssigner = getTimestampAndWatermarkAssigner();
		final SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		
		
		

			KeyedBroadcastProcessFunction<String, NMAJSONData, StreamKM, Tuple3<NMAJSONData, Double, Cluster>> pf = getProcessFunctionWithMapState();
			BroadcastConnectedStream<NMAJSONData, StreamKM> connectedStream = source
					.assignTimestampsAndWatermarks(timestampAndWatermarkAssigner)
					.keyBy((line) -> {
	                    return df.format(line.getTime());
	                })
					.connect(kmStream);
			result = connectedStream
					.process(pf)
					.filter(t -> t.f1 <= 0.5d);
		

				
		return result;
    }

    private static StreamKM streamKM = null;
	public static synchronized StreamKM getDefaultStreamKM() {
		if (streamKM != null)
			return streamKM;
		streamKM = new StreamKM();
        streamKM.numClustersOption.setValue(5); 
        streamKM.lengthOption.setValue(100000); 
        streamKM.resetLearning(); // UPDATED CODE LINE !!!
		return streamKM;
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

    static final MapStateDescriptor<String, StreamKM> kmStateDesc                          = new MapStateDescriptor<String, StreamKM>               (STREAM_KM, BasicTypeInfo.STRING_TYPE_INFO,TypeInformation.of(StreamKM.class));
    static final MapStateDescriptor<String, Map<String, NMAJSONData>> sessionsMapStateDesc = new MapStateDescriptor<String, Map<String,NMAJSONData>>("Sessions",BasicTypeInfo.STRING_TYPE_INFO,new MapTypeInfo<String,NMAJSONData>(String.class, NMAJSONData.class));
    
    public static KeyedBroadcastProcessFunction<String, NMAJSONData, StreamKM, Tuple3<NMAJSONData, Double, Cluster> > getProcessFunctionWithMapState() {


		    	
    	KeyedBroadcastProcessFunction<String, NMAJSONData, StreamKM, Tuple3<NMAJSONData, Double, Cluster> >  result = new KeyedBroadcastProcessFunction<String, NMAJSONData, StreamKM, Tuple3<NMAJSONData, Double, Cluster> > (){

        	

        	final SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

			@Override
			public void processElement(NMAJSONData data,
					KeyedBroadcastProcessFunction<String, NMAJSONData, StreamKM, Tuple3<NMAJSONData, Double, Cluster> > .ReadOnlyContext ctx,
					Collector<Tuple3<NMAJSONData, Double, Cluster>> out) throws Exception {
				
				StreamKM streamKM = null;
				StreamKM streamKM1 = ctx.getBroadcastState(kmStateDesc).get(STREAM_KM);
				StreamKM streamKM2 = getRuntimeContext().getMapState(kmStateDesc).get(STREAM_KM);
				
				double maxProbability = 0;
				Cluster assignedCluster = null;
				if (streamKM == null && streamKM1 == null && streamKM2 == null) {
			        streamKM = getDefaultStreamKM();
				}else if (streamKM1 != null){
					streamKM = streamKM1;
				}else {
					streamKM = streamKM2;
				}
				if (streamKM != null) {
					
			        //TODO
			        final DenseInstance instance = toDenseInstance(data);

			        
					Clustering result = null;
					try {
						result = streamKM.getClusteringResult();
					} catch (Throwable e) {
					}		
					if (result != null) {
				        AutoExpandVector<Cluster> clusteringCopy = result.getClusteringCopy();

				        
				        
				        
				        int i = 0;
				        for (Cluster cluster : clusteringCopy) {
				        	double inclusionProbability = cluster.getInclusionProbability(instance);
				        	if (inclusionProbability > maxProbability) {
				        		maxProbability = inclusionProbability; 
				        		assignedCluster = cluster;
				        	}
				        	i++;
						}
					}

					streamKM.trainOnInstanceImpl(instance);
					
				}
				

				out.collect(Tuple3.of(data, maxProbability, assignedCluster));	
				
			}



         



			@Override
			public void processBroadcastElement(StreamKM value,
					KeyedBroadcastProcessFunction<String, NMAJSONData, StreamKM, Tuple3<NMAJSONData, Double, Cluster>>.Context ctx,
					Collector<Tuple3<NMAJSONData, Double, Cluster>> out) throws Exception {
				
				ctx.getBroadcastState(kmStateDesc).put(STREAM_KM, value);
				
				LOG.info(STREAM_KM+" "+value.getClass());
				LOG.info("Context"+" "+ctx.getClass());
				LOG.info("Collector"+" "+out.getClass());
				
				Clustering clusteringResult = value.getClusteringResult();
				if (clusteringResult != null) {
					LOG.info("StreamKM clusteringResult"+" "+clusteringResult);
//					LOG.info("StreamKM clusteringResult size"+" "+clusteringResult.size());
//					LOG.info("StreamKM clusteringResult dimension"+" "+clusteringResult.dimension());					
				}

				
				

			}  
    		

    		
    	};

        return result;
    }
    



    public static void main(String[] args) throws Exception {
        LOG.info("Starting AnomalyReader...");
        AppConfiguration appConfiguration = new AppConfiguration(args);
        AnomalyReader reader = new AnomalyReader(appConfiguration);
        reader.run();
    }



}
