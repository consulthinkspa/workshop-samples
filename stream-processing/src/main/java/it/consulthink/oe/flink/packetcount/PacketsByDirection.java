package it.consulthink.oe.flink.packetcount;

import com.dellemc.oe.serialization.JsonDeserializationSchema;
import com.dellemc.oe.util.AbstractApp;
import com.dellemc.oe.util.AppConfiguration;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.pravega.connectors.flink.FlinkPravegaReader;
import io.pravega.connectors.flink.FlinkPravegaWriter;
import io.pravega.connectors.flink.PravegaConfig;
import it.consulthink.oe.model.NMAJSONData;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Date;
import java.util.Set;

public class PacketsByDirection extends AbstractApp {


    // Logger initialization
    private static final Logger LOG = LoggerFactory.getLogger(PacketsByDirection.class);

    Set<String> ipList = appConfiguration.getMyIps();

    // The application reads data from specified Pravega stream and once every 10 seconds
    // prints the distinct words and counts from the previous 10 seconds.
    public PacketsByDirection(AppConfiguration appConfiguration){
        super(appConfiguration);
    }

    public void run(){


        LOG.info("Starting NMA PacketsByDirection...");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        AppConfiguration.StreamConfig inputStreamConfig = appConfiguration.getInputStreamConfig();
        String inputStreamName = inputStreamConfig.getStream().getStreamName();
        LOG.info("============== input stream  =============== " + inputStreamName);

        AppConfiguration.StreamConfig outputStreamConfig= appConfiguration.getOutputStreamConfig();
        String outputStreamName = outputStreamConfig.getStream().getStreamName();
        LOG.info("============== output stream  =============== " + outputStreamName);


        // Create EventStreamClientFactory
        PravegaConfig pravegaConfig = appConfiguration.getPravegaConfig();
        LOG.info("============== Praevega  =============== " + pravegaConfig);

        SourceFunction<NMAJSONData> sourceFunction = getSourceFunction(pravegaConfig, inputStreamName);
        LOG.info("==============  SourceFunction  =============== " + sourceFunction);

        DataStream<NMAJSONData> source = env.addSource(sourceFunction).name("InputSource");
        LOG.info("==============  Source  =============== " + source);

        SingleOutputStreamOperator<Traffic> dataStream = processSource(env, source, ipList);

        dataStream.printToErr();
        LOG.info("==============  ProcessSource - PRINTED  ===============");


        FlinkPravegaWriter<Traffic> sink = getSinkFunction(pravegaConfig, outputStreamName);

        dataStream.addSink(sink).name("NMAPacketsByDirectionStream");

        // create another output sink to print to stdout for verification
        dataStream.printToErr();
        LOG.info("==============  ProcessSink - PRINTED  ===============");


        try {
            env.execute("PacketsByDirection");
        } catch (Exception e) {
            LOG.error("Error executing PacketsByDirection...");
        } finally {
            LOG.info("Ending NMA PacketsByDirection");
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


    private FlinkPravegaWriter<Traffic> getSinkFunction(PravegaConfig pravegaConfig, String outputStreamName) {

        //SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");


        FlinkPravegaWriter<Traffic> sink = FlinkPravegaWriter.<Traffic>builder()
                .withPravegaConfig(pravegaConfig)
                .forStream(outputStreamName)
                .withEventRouter((x) -> "PacketsByDirection")
                //TODO controllare la necessita dello scema
//                .withSerializationSchema(???)
                .build();
        return sink;
    }



    public static SingleOutputStreamOperator<Traffic> processSource(StreamExecutionEnvironment env, DataStream<NMAJSONData> source, Set<String> ipList){

        //setting EventTime Characteristic
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //setting the Time and Wm Extractor
        BoundedOutOfOrdernessTimestampExtractor<NMAJSONData> timestampAndWatermarkAssigner = getTimestampAndWatermarkAssigner();


        SingleOutputStreamOperator<Traffic> dataStream = source
                .assignTimestampsAndWatermarks(timestampAndWatermarkAssigner)
                .keyBy((NMAJSONData x) -> x.getTime())
                .keyBy((line) -> {
                    if (ipList.contains(line.getSrc_ip()) || ipList.contains(line.getDst_ip())) {
                        return "notLateral";
                    } else {
                        return "Lateral";
                    }
                }).window(TumblingEventTimeWindows.of(Time.seconds(1)))
                .process(getProcessFunction())
                .map((a) -> new Traffic(a.f0, a.f1, a.f2));

        return dataStream;
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


    public static ProcessWindowFunction<NMAJSONData, Tuple3<Long, Long, Long>, String, TimeWindow> getProcessFunction() {
        ProcessWindowFunction<NMAJSONData, Tuple3<Long, Long, Long>, String, TimeWindow> countByDirection = new
                ProcessWindowFunction<NMAJSONData, Tuple3<Long, Long, Long>, String, TimeWindow>() {
            @Override
            public void process(String key, Context context, Iterable<NMAJSONData> elements, Collector<Tuple3<Long, Long, Long>> out) throws Exception {
                Long inbound = 0l;
                Long outbound = 0l;
                Long lateral = 0l;

                if (key.equals("notLateral")) {

                    for (NMAJSONData data : elements) {
                        inbound += data.getPktsin();
                        outbound += data.getPktsout();
                    }

                } else {

                    for (NMAJSONData data : elements) {
                        lateral += data.getPkts();
                    }
                }

                out.collect(Tuple3.of(inbound, outbound, lateral));
            }
        };

        return countByDirection;
    }


    @JsonIgnoreProperties(ignoreUnknown = true)
    static
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
        PacketsByDirection reader = new PacketsByDirection(appConfiguration);
        reader.run();
    }



}
