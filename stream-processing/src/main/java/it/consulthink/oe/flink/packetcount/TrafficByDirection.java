package it.consulthink.oe.flink.packetcount;

import com.dellemc.oe.serialization.JsonDeserializationSchema;
import com.dellemc.oe.serialization.JsonSerializationSchema;
import com.dellemc.oe.util.AbstractApp;
import com.dellemc.oe.util.AppConfiguration;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.pravega.client.ClientConfig;
import io.pravega.connectors.flink.FlinkPravegaReader;
import io.pravega.connectors.flink.FlinkPravegaWriter;
import it.consulthink.oe.model.NMAJSONData;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

public class TrafficByDirection extends AbstractApp {


    // Logger initialization
    private static final Logger LOG = LoggerFactory.getLogger(TrafficByDirection.class);


    // The application reads data from specified Pravega stream and once every 10 seconds
    // prints the distinct words and counts from the previous 10 seconds.
    public TrafficByDirection(AppConfiguration appConfiguration){
        super(appConfiguration);
    }

    public void run(){
        try {
            AppConfiguration.StreamConfig streamConfig = appConfiguration.getInputStreamConfig();
            //  create stream
            createStream(appConfiguration.getInputStreamConfig());
            // Create EventStreamClientFactory
            ClientConfig clientConfig = appConfiguration.getPravegaConfig().getClientConfig();
            LOG.info("============== NMA TrafficByDirection stream  =============== " + streamConfig.getStream().getStreamName());

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

            // create the Pravega source to read a stream of text
            FlinkPravegaReader<NMAJSONData> source = FlinkPravegaReader.builder()
                    .withPravegaConfig(appConfiguration.getPravegaConfig())
                    .forStream(streamConfig.getStream().getStreamName())
                    .withDeserializationSchema(new JsonDeserializationSchema(NMAJSONData.class))
                    .build();


            LOG.info("==============  NMA TrafficByDirection SOURCE  =============== " + source);


            ProcessWindowFunction<NMAJSONData, Tuple3<Long, Long, Long>,String,TimeWindow> countByDirection = new ProcessWindowFunction<NMAJSONData, Tuple3<Long, Long, Long>, String, TimeWindow>() {
                @Override
                public void process(String key, Context context, Iterable<NMAJSONData> elements, Collector<Tuple3<Long, Long, Long>> out) throws Exception {

                    Long inbound = 0l;
                    Long outbound = 0l;
                    Long lateral = 0l;

                    if (key.equals("notLateral")) {

                        for (NMAJSONData data : elements) {
                            inbound += data.getBytesin();
                            outbound += data.getBytesout();
                        }

                    } else {

                        for (NMAJSONData data : elements) {
                            lateral += data.getBytesin() + data.getBytesout();
                        }
                    }

                    out.collect(Tuple3.of(inbound, outbound, lateral));

                }

            };

            Set<String> ipList = new HashSet<String>();


            ipList.add("213.61.202.114");
            ipList.add("213.61.202.115");
            ipList.add("213.61.202.116");

            // count packets over a 10 second time period
            DataStream<Traffic> dataStream = env.addSource(source).name(streamConfig.getStream().getStreamName())
                    .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<NMAJSONData>(Time.seconds(5)) {
                        @Override
                        public long extractTimestamp(NMAJSONData element) {
                            return element.getTime().getTime();
                        }
                    })
                    .keyBy((line) -> {
                        if (ipList.contains(line.getSrc_ip()) || ipList.contains(line.getDst_ip())) {
                            return "notLateral";
                        } else {
                            return "Lateral";
                        }
                    })
                    .window(TumblingEventTimeWindows.of(Time.seconds(1)))
                    .process(countByDirection)
                    .map((a) -> new Traffic(a.f0, a.f1, a.f2));


            // create an output sink to print to stdout for verification
            dataStream.printToErr();

            LOG.info("==============  NMA TrafficByDirection PRINTED  ===============");
            AppConfiguration.StreamConfig outputStreamConfig = appConfiguration.getOutputStreamConfig();
            //  create stream
            createStream(appConfiguration.getOutputStreamConfig());
            FlinkPravegaWriter<Traffic> writer = FlinkPravegaWriter.<Traffic>builder()
                    .withPravegaConfig(appConfiguration.getPravegaConfig())
                    .forStream(outputStreamConfig.getStream().getStreamName())
                    .withEventRouter((a) -> "TrafficByDirection" )
                    .withSerializationSchema(new JsonSerializationSchema())
                    .build();

            dataStream.addSink(writer).name("NMATrafficByDirectionStream");

            // create another output sink to print to stdout for verification

            LOG.info("============== NMA TrafficByDirection Final output ===============");
            dataStream.printToErr();
            // execute within the Flink environment
            env.execute("TrafficByDirection");

            LOG.info("Ending NMA PacketCountReader...");
        }catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    @JsonIgnoreProperties(ignoreUnknown = true)
    class Traffic implements Serializable {

        public Long inbound;
        public Long outbound;
        public Long lateral;

        public Traffic(Long inbound, Long outbound, Long lateral) {
            this.inbound = inbound;
            this.outbound = outbound;
            this.lateral = lateral;
        }

        @Override
        public String toString() {
            return "Traffic{" +
                    "inbound=" + inbound +
                    ", outbound=" + outbound +
                    ", lateral=" + lateral +
                    '}';
        }
    }


    public static void main(String[] args) throws Exception {
        LOG.info("Starting PacketCountReader...");
        AppConfiguration appConfiguration = new AppConfiguration(args);
        PacketCountReader reader = new PacketCountReader(appConfiguration);
        reader.run();
    }



}
