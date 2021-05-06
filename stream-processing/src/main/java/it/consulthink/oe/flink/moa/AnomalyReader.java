package it.consulthink.oe.flink.moa;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dellemc.oe.serialization.JsonDeserializationSchema;
import com.dellemc.oe.serialization.JsonSerializationSchema;
import com.dellemc.oe.util.AbstractApp;
import com.dellemc.oe.util.AppConfiguration;
import com.yahoo.labs.samoa.instances.Attribute;
import com.yahoo.labs.samoa.instances.DenseInstance;
import com.yahoo.labs.samoa.instances.Instances;
import com.yahoo.labs.samoa.instances.InstancesHeader;

import io.pravega.connectors.flink.FlinkPravegaReader;
import io.pravega.connectors.flink.FlinkPravegaWriter;
import io.pravega.connectors.flink.PravegaConfig;
import it.consulthink.oe.ingest.NMAJSONDataGenerator;
import it.consulthink.oe.model.Anomaly;
import it.consulthink.oe.model.NMAJSONData;
import moa.cluster.Cluster;
import moa.cluster.Clustering;
import moa.clusterers.streamkm.StreamKM;
import moa.core.AutoExpandVector;

public class AnomalyReader extends AbstractApp {


    private static final String STREAM_KM = "StreamKM";
    public static double TOLLERANCE = 0.5d;

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

        DataStream<NMAJSONData> source = env.addSource(sourceFunction).name("Pravega."+inputStreamName);
        LOG.info("==============  Source  =============== " + source);

        
        trainStreamKM(appConfiguration, 500);
        
        SingleOutputStreamOperator<Anomaly> dataStream = processSource(env, source);

        dataStream.printToErr();
        LOG.info("==============  ProcessSource - PRINTED  ===============");


        FlinkPravegaWriter<Anomaly> sink = getSinkFunction(pravegaConfig, outputStreamName);

        dataStream
        .addSink(sink).name("Pravega."+outputStreamName);

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
		Stream<NMAJSONData> stream = NMAJSONDataGenerator.generateInfiniteStreamNoAnomaly(appConfiguration.getMyIps())
		.limit(elements);
        trainStreamKM(stream);
	}
	
	public static void trainStreamKM(Stream<NMAJSONData> stream) {
		getDefaultStreamKM().resetLearning();
		stream
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
		
		// generates the name of the features which is called as InstanceHeader
		ArrayList<Attribute> attributes = new ArrayList<Attribute>();
		attributes.add(new Attribute("packets"));
		attributes.add(new Attribute("bytesin"));
		attributes.add(new Attribute("bytesout"));
		InstancesHeader streamHeader = new InstancesHeader(new Instances(data.getClass().getName(),attributes, attributes.size()));


		double packets = Math.min((data.getPkts())/((double)NMAJSONDataGenerator.MAX_PACKETS ), 1d);
		double bytesin = Math.min((data.getBytesin())/((double)NMAJSONDataGenerator.MAX_BYTESIN ), 1d);
		double bytesout = Math.min((data.getBytesout())/((double)NMAJSONDataGenerator.MAX_BYTESOUT ), 1d);
		
//		DenseInstance instance = new DenseInstance(3);		
//		instance.setValue(0, packets);
//		instance.setValue(1, bytesin );
//		instance.setValue(2, bytesout);
		
		double[] d = new double[attributes.size()];
		d[0] = packets;
		d[1] = bytesin;
		d[2] = bytesout;
		DenseInstance instance = new DenseInstance(1.0, d);
		instance.setDataset(streamHeader);		
		
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


    private FlinkPravegaWriter<Anomaly> getSinkFunction(PravegaConfig pravegaConfig, String outputStreamName) {
    	final SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm");
        FlinkPravegaWriter<Anomaly> sink = FlinkPravegaWriter.<Anomaly>builder()
                .withPravegaConfig(pravegaConfig)
                .forStream(outputStreamName)
                .withEventRouter((x) -> "Anomalies")
                .withSerializationSchema(new JsonSerializationSchema<Anomaly>())
                .build();
        return sink;
    }



    public static SingleOutputStreamOperator<Anomaly> processSource(StreamExecutionEnvironment env, DataStream<NMAJSONData> source){
    	SingleOutputStreamOperator<Anomaly> result = null;
    	
    	
        StreamKM streamKM = getDefaultStreamKM();
    	
        
        
        
		BroadcastStream<StreamKM> kmStream = env.fromElements(streamKM).broadcast(kmStateDesc);

    			
        //setting EventTime Characteristic
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //setting the Time and Wm Extractor
		BoundedOutOfOrdernessTimestampExtractor<NMAJSONData> timestampAndWatermarkAssigner = getTimestampAndWatermarkAssigner();
		final SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		
		
		

			KeyedBroadcastProcessFunction<String, NMAJSONData, StreamKM, Anomaly> pf = getProcessFunctionWithMapState();
			BroadcastConnectedStream<NMAJSONData, StreamKM> connectedStream = source
					.assignTimestampsAndWatermarks(timestampAndWatermarkAssigner)
					.keyBy((line) -> {
	                    return df.format(line.getTime());
	                })
					.connect(kmStream);
			result = connectedStream
					.process(pf)
					.filter(new FilterFunction<Anomaly>() {
						@Override
						public boolean filter(Anomaly t) throws Exception {
							return (t.getAssignedClusterCenter() == null || t.getAssignedClusterCenter().length == 0 || t.getMaxProbability() == 0d);
						}
					});
		

				
		return result;
    }

    private static StreamKM streamKM = null;
	public static synchronized StreamKM getDefaultStreamKM() {
		if (streamKM != null)
			return streamKM;
		streamKM = generateStreamKM(100000,5);
		return streamKM;
	}

	public static synchronized StreamKM generateStreamKM(int lengthOption, int numClustersOption) {
		streamKM = new StreamKM();
        streamKM.numClustersOption.setValue(numClustersOption); 
        streamKM.lengthOption.setValue(lengthOption); 
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
    
    public static KeyedBroadcastProcessFunction<String, NMAJSONData, StreamKM, Anomaly > getProcessFunctionWithMapState() {


		    	
    	KeyedBroadcastProcessFunction<String, NMAJSONData, StreamKM, Anomaly >  result = new KeyedBroadcastProcessFunction<String, NMAJSONData, StreamKM, Anomaly > (){

        	

        	final SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

			@Override
			public void processElement(NMAJSONData data,
					KeyedBroadcastProcessFunction<String, NMAJSONData, StreamKM, Anomaly > .ReadOnlyContext ctx,
					Collector<Anomaly> out) throws Exception {
				
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
					if (result == null || (result.getMaxInclusionProbability(instance) > 0 && result.getMaxInclusionProbability(instance) < 1)){
						streamKM.trainOnInstanceImpl(instance);
					}
					
					
					
				}
				

				out.collect(new Anomaly(Tuple3.of(data, maxProbability, assignedCluster)));	
				
			}



         



			@Override
			public void processBroadcastElement(StreamKM value,
					KeyedBroadcastProcessFunction<String, NMAJSONData, StreamKM, Anomaly>.Context ctx,
					Collector<Anomaly> out) throws Exception {
				
				ctx.getBroadcastState(kmStateDesc).put(STREAM_KM, value);
				
				LOG.info(STREAM_KM+" "+value.getClass().getName());
				LOG.info("Context"+" "+ctx.getClass().getName());
				LOG.info("Collector"+" "+out.getClass().getName());
				
				Clustering clusteringResult = value.getClusteringResult();
				if (clusteringResult != null) {
//					LOG.info("StreamKM clusteringResult"+" "+clusteringResult);
					LOG.info("StreamKM clusteringResult size"+" "+clusteringResult.size());
					LOG.info("StreamKM clusteringResult dimension"+" "+clusteringResult.dimension());					
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
