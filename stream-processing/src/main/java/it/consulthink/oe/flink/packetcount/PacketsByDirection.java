package it.consulthink.oe.flink.packetcount;

import com.dellemc.oe.serialization.JsonDeserializationSchema;
import com.dellemc.oe.serialization.JsonSerializationSchema;
import com.dellemc.oe.util.AbstractApp;
import com.dellemc.oe.util.AppConfiguration;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.pravega.connectors.flink.FlinkPravegaReader;
import io.pravega.connectors.flink.FlinkPravegaWriter;
import io.pravega.connectors.flink.PravegaConfig;
import io.pravega.connectors.flink.PravegaEventRouter;
import it.consulthink.oe.model.NMAJSONData;
import it.consulthink.oe.model.Traffic;
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

        SingleOutputStreamOperator<Tuple2<Date,Traffic>> dataStream = processSource(env, source, ipList);

        dataStream.printToErr();
        LOG.info("==============  ProcessSource - PRINTED  ===============");


        FlinkPravegaWriter<Tuple2<Date,Traffic>> sink = getSinkFunction(pravegaConfig, outputStreamName);

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


    private FlinkPravegaWriter<Tuple2<Date, Traffic>> getSinkFunction(PravegaConfig pravegaConfig, String outputStreamName) {

        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");


        FlinkPravegaWriter<Tuple2<Date, Traffic>> sink = FlinkPravegaWriter.<Tuple2<Date, Traffic>>builder()
                .withPravegaConfig(pravegaConfig)
                .forStream(outputStreamName)
                .withEventRouter(new PravegaEventRouter<Tuple2<Date, Traffic>>() {
                    @Override
                    public String getRoutingKey(Tuple2<Date, Traffic> event) {
                        return df.format(event.f0);
                    }
                })
                .withSerializationSchema(new JsonSerializationSchema<Tuple2<Date, Traffic>>())
                .build();
        return sink;
    }



    public static SingleOutputStreamOperator<Tuple2<Date, Traffic>> processSource(StreamExecutionEnvironment env, DataStream<NMAJSONData> source, Set<String> ipList){

        //setting EventTime Characteristic
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //setting the Time and Wm Extractor
        BoundedOutOfOrdernessTimestampExtractor<NMAJSONData> timestampAndWatermarkAssigner = getTimestampAndWatermarkAssigner();

        //TODO qui come gestire la doppia aggregazione?
        //TODO concatenare le due keyby oppure processare la prima finestra e poi procedere alla seconda?
        SingleOutputStreamOperator<Tuple2<Date, Traffic>> dataStream = source
                .assignTimestampsAndWatermarks(timestampAndWatermarkAssigner)
                .keyBy((NMAJSONData x) -> x.getTime())
                .window(TumblingEventTimeWindows.of(Time.seconds(1)))
                .process(getProcessFunctionDate(ipList))
                .keyBy((Tuple2<Date, Traffic> x) -> x.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(1)))
                .reduce((Tuple2<Date, Traffic> v1, Tuple2<Date, Traffic> v2) -> {

                    if(v1.f0.equals(v2.f0))
                        return Tuple2.of(v1.f0, new Traffic(v1.f1,v2.f1));
                    LOG.error(""+v1+" "+v2);
                    throw new RuntimeException();
                });


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


    public static ProcessWindowFunction<NMAJSONData, Tuple2<Date, Traffic>, Date, TimeWindow> getProcessFunctionDate(Set<String> ipList) {

        ProcessWindowFunction<NMAJSONData, Tuple2<Date, Traffic>, Date, TimeWindow> trafficByDate = new ProcessWindowFunction<NMAJSONData, Tuple2<Date, Traffic>, Date, TimeWindow>() {

            @Override
            public void process(Date key, ProcessWindowFunction<NMAJSONData, Tuple2<Date, Traffic>, Date, TimeWindow>.Context ctx, Iterable<NMAJSONData> iterable, Collector<Tuple2<Date, Traffic>> collector) throws Exception {

                Long inbound = 0l;
                Long outbound = 0l;
                Long lateral = 0l;

                for (NMAJSONData element : iterable) {

                    if (ipList.contains(element.getSrc_ip()) || ipList.contains(element.getDst_ip())) {
                        inbound += element.getPktsin();
                        outbound += element.getPktsout();
                    } else {
                        lateral += element.getPkts();
                    }
                }

                collector.collect(Tuple2.of(key, new Traffic(inbound, outbound, lateral)));

            }
        };
        return trafficByDate;
    };


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



    public static void main(String[] args) throws Exception {
        LOG.info("Starting PacketCountReader...");
        AppConfiguration appConfiguration = new AppConfiguration(args);
        PacketsByDirection reader = new PacketsByDirection(appConfiguration);
        reader.run();
    }



}
