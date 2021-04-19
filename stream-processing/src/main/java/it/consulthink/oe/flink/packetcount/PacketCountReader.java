package it.consulthink.oe.flink.packetcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
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

/*
 * At a high level, PacketCountReader reads from a Pravega stream, and prints
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
            
            // create the Pravega source to read a stream of text
            FlinkPravegaReader<NMAJSONData> source = FlinkPravegaReader.builder()
            		.withPravegaConfig(appConfiguration.getPravegaConfig())
                    .forStream(streamConfig.getStream().getStreamName())
                    .withDeserializationSchema(new JsonDeserializationSchema(NMAJSONData.class))
                    .build();
            LOG.info("==============  NMA PacketCountReader SOURCE  =============== " + source);
            
            
            // count packets over a 10 second time period
            DataStream<PacketCount> dataStream = env.addSource(source).name(streamConfig.getStream().getStreamName())
                    .flatMap(new PacketCountReader.NMASumPackets())
                    .keyBy("networkHash")
                    //TODO check window 
                    .timeWindow(Time.seconds(10))
                    .sum("count");

            // create an output sink to print to stdout for verification
            dataStream.printToErr();

            LOG.info("==============  NMA PacketCountReader PRINTED  ===============");
            AppConfiguration.StreamConfig outputStreamConfig = appConfiguration.getOutputStreamConfig();
            //  create stream
            createStream(appConfiguration.getOutputStreamConfig());
            FlinkPravegaWriter<PacketCount> writer = FlinkPravegaWriter.<PacketCount>builder()
                    .withPravegaConfig(appConfiguration.getPravegaConfig())
                    .forStream(outputStreamConfig.getStream().getStreamName())
                    .withEventRouter(new EventRouter())
                    .withSerializationSchema(new JsonSerializationSchema())
                    .build();
            dataStream.addSink(writer).name("NMAPacketCountReaderOutputStream");

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
        PacketCountReader wordCounter = new PacketCountReader(appConfiguration);
        wordCounter.run();
    }

    /*
     * Event Router class
     */
    public static class EventRouter implements PravegaEventRouter<PacketCount> {
        // Ordering - events with the same routing key will always be
        // read in the order they were written
        @Override
        public String getRoutingKey(PacketCount event) {
            return event.getNetworkHash();
        }
    }

    // count packets by network hash
    private static class NMASumPackets implements FlatMapFunction<NMAJSONData, PacketCount> {
        @Override
        public void flatMap(NMAJSONData line, Collector<PacketCount> out) throws Exception {
        	out.collect(new PacketCount(line));
        }
    }

}
