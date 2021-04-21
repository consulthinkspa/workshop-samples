package it.consulthink.oe.flink.packetcount;

import com.dellemc.oe.serialization.JsonDeserializationSchema;
import com.dellemc.oe.serialization.JsonSerializationSchema;
import com.dellemc.oe.util.AbstractApp;
import com.dellemc.oe.util.AppConfiguration;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.pravega.client.ClientConfig;
import io.pravega.connectors.flink.FlinkPravegaReader;
import io.pravega.connectors.flink.FlinkPravegaWriter;
import io.pravega.connectors.flink.PravegaConfig;
import io.pravega.connectors.flink.PravegaEventRouter;
import it.consulthink.oe.model.NMAJSONData;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
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
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

public class TrafficByDirection extends AbstractApp {


    // Logger initialization
    private static final Logger LOG = LoggerFactory.getLogger(TrafficByDirection.class);

    Set<String> ipList = appConfiguration.getMyIps();

    // The application reads data from specified Pravega stream and once every 10 seconds
    // prints the distinct words and counts from the previous 10 seconds.
    public TrafficByDirection(AppConfiguration appConfiguration){
        super(appConfiguration);
    }

    public void run(){



        LOG.info("Starting NMA TrafficByDirection...");

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

        dataStream.addSink(sink).name("NMATrafficByDirectionStream");

        // create another output sink to print to stdout for verification
        dataStream.printToErr();
        LOG.info("==============  ProcessSink - PRINTED  ===============");


        try {
            env.execute("TrafficByDirection");
        } catch (Exception e) {
            LOG.error("Error executing TrafficByDirection...");
        } finally {
            LOG.info("Ending NMA TrafficByDirection");
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
                .withEventRouter((x) -> "")
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

        //TODO qui come gestire la doppia aggregazione?
        //TODO concatenare le due keyby oppure processare la prima finestra e poi procedere alla seconda?
        SingleOutputStreamOperator<Traffic> dataStream = source
                .assignTimestampsAndWatermarks(timestampAndWatermarkAssigner)
                .keyBy(new KeySelector<NMAJSONData, Date>(){

                    @Override
                    public Date getKey(NMAJSONData value) throws Exception {
                        return value.getTime();
                    }

                })
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
        TrafficByDirection reader = new TrafficByDirection(appConfiguration);
        reader.run();
    }



}
