package it.consulthink.oe.flink.packetcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.sources.wmstrategies.WatermarkStrategy;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dellemc.oe.serialization.JsonDeserializationSchema;
import com.dellemc.oe.serialization.JsonSerializationSchema;
import com.dellemc.oe.util.AbstractApp;
import com.dellemc.oe.util.AppConfiguration;

import io.pravega.client.ClientConfig;
import io.pravega.connectors.flink.FlinkPravegaReader;
import io.pravega.connectors.flink.FlinkPravegaWriter;
import io.pravega.connectors.flink.PravegaEventRouter;
import it.consulthink.oe.model.NMAJSONData;

import java.util.HashSet;
import java.util.Set;

/*
 * At a high level, PacketCountReader reads from a Pravega stream, and prints
 * the packet count summary to the output. This class provides an example for
 * a simple Flink application that reads streaming data from Pravega.
 *
 * And  after flink transformation output redirect to another pravega stream.
 *
 * This application has the following input parameters
 *     stream - Pravega stream name to read from
 *     controller - the Pravega controller URI, e.g., tcp://localhost:9090
 *                  Note that this parameter is automatically used by the PravegaConfig class
 */
public class PacketCountReader extends AbstractApp {

    // Logger initialization
    private static final Logger LOG = LoggerFactory.getLogger(PacketCountReader.class);


    // The application reads data from specified Pravega stream and once every 10 seconds
    // prints the distinct words and counts from the previous 10 seconds.
    public PacketCountReader(AppConfiguration appConfiguration){
        super(appConfiguration);
    }

    public void run(){
        try {
            AppConfiguration.StreamConfig streamConfig = appConfiguration.getInputStreamConfig();
            //  create stream
            createStream(appConfiguration.getInputStreamConfig());
            // Create EventStreamClientFactory
            ClientConfig clientConfig = appConfiguration.getPravegaConfig().getClientConfig();
            LOG.info("============== NMA PacketCountReader stream  =============== " + streamConfig.getStream().getStreamName());

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
            
            // create the Pravega source to read a stream of text
            FlinkPravegaReader<NMAJSONData> source = FlinkPravegaReader.builder()
            		.withPravegaConfig(appConfiguration.getPravegaConfig())
                    .forStream(streamConfig.getStream().getStreamName())
                    .withDeserializationSchema(new JsonDeserializationSchema(NMAJSONData.class))
                    .build();


            LOG.info("==============  NMA PacketCountReader SOURCE  =============== " + source);


            ProcessAllWindowFunction<NMAJSONData, Long, TimeWindow> sumPackets = new ProcessAllWindowFunction<NMAJSONData, Long, TimeWindow>() {

                @Override
                public void process(ProcessAllWindowFunction<NMAJSONData, Long, TimeWindow>.Context arg0,
                                    Iterable<NMAJSONData> arg1, Collector<Long> arg2) throws Exception {

                    for (NMAJSONData nmajsonData : arg1) {
                        arg2.collect(nmajsonData.getPkts());
                    }
                }

            };


            // count packets over a 10 second time period
            DataStream<Long> dataStream = env.addSource(source).name(streamConfig.getStream().getStreamName())
               .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<NMAJSONData>(Time.seconds(5)) {
                   @Override
                   public long extractTimestamp(NMAJSONData element) {
                       return element.getTime().getTime();
                   }
               })
               .windowAll(TumblingEventTimeWindows.of(Time.seconds(1)))
               .process(sumPackets);


            // create an output sink to print to stdout for verification
            dataStream.printToErr();

            LOG.info("==============  NMA PacketCountReader PRINTED  ===============");
            AppConfiguration.StreamConfig outputStreamConfig = appConfiguration.getOutputStreamConfig();
            //  create stream
            createStream(appConfiguration.getOutputStreamConfig());
            FlinkPravegaWriter<Long> writer = FlinkPravegaWriter.<Long>builder()
                    .withPravegaConfig(appConfiguration.getPravegaConfig())
                    .forStream(outputStreamConfig.getStream().getStreamName())
                    .withEventRouter((a) -> "TotalPackets" )
                    // TODO controllare la necessità dello schema
                    //.withSerializationSchema(new JsonSerializationSchema())
                    .build();

            dataStream.addSink(writer).name("NMATotalPacketStream");

            // create another output sink to print to stdout for verification

            LOG.info("============== NMA PacketCountReader Final output ===============");
            dataStream.printToErr();
            // execute within the Flink environment
            env.execute("PacketCountReader");

            LOG.info("Ending NMA PacketCountReader...");
        }catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws Exception {
        LOG.info("Starting PacketCountReader...");
        AppConfiguration appConfiguration = new AppConfiguration(args);
        PacketCountReader reader = new PacketCountReader(appConfiguration);
        reader.run();
    }



}
