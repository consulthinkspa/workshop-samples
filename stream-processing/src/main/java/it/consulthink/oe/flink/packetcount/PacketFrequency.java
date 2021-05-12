package it.consulthink.oe.flink.packetcount;

import com.dellemc.oe.serialization.JsonDeserializationSchema;
import com.dellemc.oe.serialization.JsonSerializationSchema;
import com.dellemc.oe.util.AbstractApp;
import com.dellemc.oe.util.AppConfiguration;
import io.pravega.connectors.flink.FlinkPravegaReader;
import io.pravega.connectors.flink.FlinkPravegaWriter;
import io.pravega.connectors.flink.PravegaConfig;
import io.pravega.connectors.flink.PravegaEventRouter;
import it.consulthink.oe.model.*;
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

import java.io.UnsupportedEncodingException;
import java.net.Inet4Address;
import java.text.SimpleDateFormat;
import java.util.Date;

/*
 * At a high level, PacketFrequency reads from a Pravega stream, and prints
 * a packet frequency analysis by session to the output.
 *
 */
public class PacketFrequency extends AbstractApp{

    // Logger initialization
    private static final Logger LOG = LoggerFactory.getLogger(PacketFrequency.class);


    // The application reads data from specified Pravega stream and once every 10 seconds

    public PacketFrequency(AppConfiguration appConfiguration){
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
            
           
			SourceFunction<NMAPacketData> sourceFunction = getSourceFunction(pravegaConfig, inputStreamName);
            
            DataStream<NMAPacketData> source = env.addSource(sourceFunction).name("Pravega."+inputStreamName);
            
			SingleOutputStreamOperator<SessionPacketFrequency> dataStream = processSource(env, source);

//            dataStream.printToErr();
            
			
            
            FlinkPravegaWriter<SessionPacketFrequency> sink = getSinkFunction(pravegaConfig, outputStreamName);
//            dataStream.printToErr();
            DataStreamSink<SessionPacketFrequency> dataStreamSink = dataStream.addSink(sink).name("Pravega."+outputStreamName);

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



	public static ProcessWindowFunction<NMAPacketData, SessionPacketFrequency, Integer, TimeWindow>
	getFrequencyPerSessionProcessFunction() {

		ProcessWindowFunction<NMAPacketData, SessionPacketFrequency, Integer, TimeWindow> sessionsPerSecond =
				new ProcessWindowFunction<NMAPacketData, SessionPacketFrequency, Integer, TimeWindow>() {

			@Override
			public void process(Integer key,
								ProcessWindowFunction<NMAPacketData,SessionPacketFrequency,Integer,TimeWindow>.Context ctx,
								Iterable<NMAPacketData> iterable,
								Collector<SessionPacketFrequency> collector) throws Exception {

				Date time = null;
				Inet4Address client = null;
				Inet4Address server = null;
				int source_port = 0;
				int dest_port = 0;

				int c2sPkts = 0;
				int s2cPkts = 0;

				int totalSessionPkts = 0;

				//todo capire come tenere le informazioni sulla
				// direzione come uno stato globale

				//ctx.globalState().getState();

				for (NMAPacketData element : iterable) {

					if( element.getSessionState() == "OPENING"){
						client = element.getSrc_ip();
						server = element.getDst_ip();
						source_port = element.sport;
						dest_port = element.dport;

						//esco dal for perché ho scoperto definitivamente la direzione
						break;

					} else if(element.sport > element.dport) {
						//guessing direction
						client = element.getSrc_ip();
						server = element.getDst_ip();
						source_port = element.sport;
						dest_port = element.dport;

					} else {
						//guessing direction
						server = element.getSrc_ip();
						client = element.getDst_ip();
						dest_port= element.sport;
						source_port = element.dport;
					}

				}

				// extract info
				for(NMAPacketData element : iterable){
					time = element.getTime();
					totalSessionPkts++;
					if(client.equals(element.src_ip))
						c2sPkts++;
					else
						s2cPkts++;
				}


				// faccio il collect di un oggetto che modella la sessione
				// e modella la frequenza dei pacchetti totali, per client, per server
				collector.collect( new SessionPacketFrequency(time, client, server, source_port, dest_port,
						c2sPkts,s2cPkts, totalSessionPkts));

			}

		};

		return sessionsPerSecond;
	}



	@SuppressWarnings("unchecked")
	private FlinkPravegaReader<NMAPacketData> getSourceFunction(PravegaConfig pravegaConfig, String inputStreamName) {
		// create the Pravega source to read a stream of text
		FlinkPravegaReader<NMAPacketData> source = FlinkPravegaReader.builder()
				.withPravegaConfig(pravegaConfig)
		        .forStream(inputStreamName)
		        .withDeserializationSchema(new JsonDeserializationSchema(NMAPacketData.class))
		        .build();
		return source;
	}



	//todo
	@SuppressWarnings("unchecked")
	public static SingleOutputStreamOperator<SessionPacketFrequency> processSource(StreamExecutionEnvironment env, DataStream<NMAPacketData> source) {
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		
		BoundedOutOfOrdernessTimestampExtractor<NMAPacketData> timestampAndWatermarkAssigner = getTimestampAndWatermarkAssigner();


		SingleOutputStreamOperator<SessionPacketFrequency> dataStream = source
				.assignTimestampsAndWatermarks(timestampAndWatermarkAssigner)
				.keyBy((NMAPacketData x) -> x.getServiceHash())
				.window(TumblingEventTimeWindows.of(Time.seconds(1)))
				.process(getFrequencyPerSessionProcessFunction());




		return dataStream;
	}


	public static BoundedOutOfOrdernessTimestampExtractor<NMAPacketData> getTimestampAndWatermarkAssigner() {
		BoundedOutOfOrdernessTimestampExtractor<NMAPacketData> timestampAndWatermarkAssigner = new BoundedOutOfOrdernessTimestampExtractor<NMAPacketData>(Time.seconds(2)) {

		    @Override
		    public long extractTimestamp(NMAPacketData element) {
		        return element.getTime().getTime();
		    }
			
		};
		return timestampAndWatermarkAssigner;
	}

	//todo
	public static FlinkPravegaWriter<SessionPacketFrequency> getSinkFunction(PravegaConfig pravegaConfig, String outputStreamName) {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");

		
		JsonSerializationSchema<SessionPacketFrequency> serializationSchema = new JsonSerializationSchema<SessionPacketFrequency>();
		FlinkPravegaWriter<SessionPacketFrequency> sink = FlinkPravegaWriter.<SessionPacketFrequency>builder()
		        .withPravegaConfig(pravegaConfig)
		        .forStream(outputStreamName)
		        .withEventRouter(new PravegaEventRouter<SessionPacketFrequency>() {

					@Override
					public String getRoutingKey(SessionPacketFrequency event) {
						try {
							LOG.info(new String(serializationSchema.serialize(event),  "UTF-8"));
						} catch (UnsupportedEncodingException e) {
							//NOP
						}
						return PacketFrequency.class.getName();
					}

		        	
				})
                .withSerializationSchema(serializationSchema)
		        .build();
		return sink;
	}	

    public static void main(String[] args) throws Exception {
    	LOG.info("Main...");
        AppConfiguration appConfiguration = new AppConfiguration(args);
        PacketFrequency reader = new PacketFrequency(appConfiguration);
        reader.run();
    }

}
