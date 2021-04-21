package it.consulthink.oe.flink.tcpstate;

import com.dellemc.oe.serialization.JsonDeserializationSchema;
import com.dellemc.oe.serialization.JsonSerializationSchema;
import com.dellemc.oe.util.AbstractApp;
import com.dellemc.oe.util.AppConfiguration;
import io.pravega.client.ClientConfig;
import io.pravega.connectors.flink.FlinkPravegaReader;
import io.pravega.connectors.flink.FlinkPravegaWriter;
import io.pravega.connectors.flink.PravegaEventRouter;
import it.consulthink.oe.flink.packetcount.PacketCountReader;
import it.consulthink.oe.model.NMAJSONData;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TCPStateReader extends AbstractApp {



    // Logger initialization
    private static final Logger LOG = LoggerFactory.getLogger(TCPStateReader.class);


    // The application reads data from specified Pravega stream and once every 10 seconds
    // prints the distinct words and counts from the previous 10 seconds.
    public TCPStateReader(AppConfiguration appConfiguration){
        super(appConfiguration);
    }

    public void run(){
        try {
            AppConfiguration.StreamConfig streamConfig = appConfiguration.getInputStreamConfig();
            //  create stream
            createStream(appConfiguration.getInputStreamConfig());
            // Create EventStreamClientFactory
            ClientConfig clientConfig = appConfiguration.getPravegaConfig().getClientConfig();
            LOG.info("============== NMA TCPStateReader stream  =============== " + streamConfig.getStream().getStreamName());

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            // create the Pravega source to read a stream of text
            FlinkPravegaReader<NMAJSONData> source = FlinkPravegaReader.builder()
                    .withPravegaConfig(appConfiguration.getPravegaConfig())
                    .forStream(streamConfig.getStream().getStreamName())
                    .withDeserializationSchema(new JsonDeserializationSchema(NMAJSONData.class))
                    .build();


            LOG.info("==============  NMA TCPStateReader SOURCE  =============== " + source);


            // count packets over a 10 second time period
            DataStream<TCPState> dataStream = env.addSource(source).name(streamConfig.getStream().getStreamName())
                    .filter((NMAJSONData d) -> (d.synin > 0 || d.synackout > 0))
                    .flatMap(new TCPStateMap())
                    .keyBy("networkHash")

                    //TODO check window
                    .timeWindow(Time.seconds(10))
                    .sum("count");


            // create an output sink to print to stdout for verification
            dataStream.printToErr();

            LOG.info("==============  NMA TCPStateReader PRINTED  ===============");
            AppConfiguration.StreamConfig outputStreamConfig = appConfiguration.getOutputStreamConfig();
            //  create stream
            createStream(appConfiguration.getOutputStreamConfig());
            FlinkPravegaWriter<TCPState> writer = FlinkPravegaWriter.<TCPState>builder()
                    .withPravegaConfig(appConfiguration.getPravegaConfig())
                    .forStream(outputStreamConfig.getStream().getStreamName())
                    .withEventRouter(new TCPStateReader.EventRouter())
                    .withSerializationSchema(new JsonSerializationSchema())
                    .build();

            dataStream.addSink(writer).name("NMATCPStateReaderOutputStream");

            // create another output sink to print to stdout for verification

            LOG.info("============== NMA TCPStateReader Final output ===============");
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
    public static class EventRouter implements PravegaEventRouter<TCPState> {
        // Ordering - events with the same routing key will always be
        // read in the order they were written
        @Override
        public String getRoutingKey(TCPState event) {
            return event.getNetworkHash();
        }
    }

    // aggregate packets by network hash
    private static class TCPStateMap implements FlatMapFunction<NMAJSONData, TCPState> {
        @Override
        public void flatMap(NMAJSONData line, Collector<TCPState> out) throws Exception {
            out.collect(new TCPState(line));
        }
    }



}
