package it.consulthink.oe.flink.packetcount;

import it.consulthink.oe.model.NMAJSONData;
import junit.framework.Assert;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.Collector;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class PacketCountReaderTest {

    private static final Logger LOG = LoggerFactory.getLogger(TrafficByDirectionTest.class);

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster = new MiniClusterWithClientResource(
            new MiniClusterResourceConfiguration.Builder().setNumberSlotsPerTaskManager(6).setNumberTaskManagers(2)
                    .build());


    @Test
    public void testProcessAllWindowFunction() throws Exception {
        ProcessAllWindowFunction<NMAJSONData, Long, TimeWindow> sumPackets = PacketCountReader.getProcessAllWindowFunction();

        Collector<Long> collector = new Collector<Long>() {
            Long collected = 0l;

            @Override
            public void collect(Long record) {
                this.collected += record;
            }

            @Override
            public void close() {
                // TODO Auto-generated method stub
            }

            @Override
            public String toString() {
                return String.valueOf(collected);
            }
        };

        List<NMAJSONData> iterable = TestUtilities.readCSV("metrics_23-03-ordered.csv");

        sumPackets.process(null, iterable, collector);

        Assert.assertEquals("52873", collector.toString());

    }


    @Test
    public void testProcessFunction() throws Exception {
        ProcessWindowFunction<NMAJSONData, Tuple2<Date, Long>, Date, TimeWindow> sumBytes = TotalTrafficReader.getProcessFunction();

        Collector<Tuple2<Date, Long>> collector = new Collector<Tuple2<Date, Long>>() {
            Long collected = 0l;

            @Override
            public void collect(Tuple2<Date, Long> record) {
                this.collected += record.f1;
            }

            @Override
            public void close() {
                // TODO Auto-generated method stub
            }

            @Override
            public String toString() {
                return String.valueOf(collected);
            }
        };

        Date example = DateUtils.parseDate("2021-03-21  22:59:59", "yyyy-MM-dd HH:mm:ss");

        List<NMAJSONData> iterable = Arrays.asList(
                new NMAJSONData(example, "", "", "", "", 0l, 0l,0l,0l,0l,0l,0l,0l,0l,0l,0l),
                new NMAJSONData(example, "", "", "", "", 0l, 0l,0l,0l,0l,0l,0l,0l,0l,0l,0l),
                new NMAJSONData(example, "", "", "", "", 0l, 0l,0l,0l,0l,0l,0l,0l,0l,0l,0l),
                new NMAJSONData(example, "", "", "", "", 0l, 0l,0l,0l,0l,0l,0l,0l,0l,0l,0l)
        );

        ProcessWindowFunction<NMAJSONData, Tuple2<Date, Long>, Date, TimeWindow>.Context ctx = null;
        sumBytes.process(example, ctx, iterable, collector);

        Assert.assertEquals("0", collector.toString());


        iterable = Arrays.asList(
                new NMAJSONData(example, "", "", "", "", 0l, 0l,1l,0l,0l,0l,0l,0l,0l,0l,0l),
                new NMAJSONData(example, "", "", "", "", 0l, 0l,2l,0l,0l,0l,0l,0l,0l,0l,0l),
                new NMAJSONData(example, "", "", "", "", 0l, 0l,3l,0l,0l,0l,0l,0l,0l,0l,0l),
                new NMAJSONData(example, "", "", "", "", 0l, 0l,4l,0l,0l,0l,0l,0l,0l,0l,0l)
        );

        sumBytes.process(example, ctx, iterable, collector);

        Assert.assertEquals("10", collector.toString());

    }



    @Test
    public void testProcessSource() throws Exception {

        PacketCountReaderTest.CollectSink.values.clear();

        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(3);
        senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        ArrayList<NMAJSONData> iterable = TestUtilities.readCSV("metrics_23-03-ordered.csv");

        DataStream<NMAJSONData> source = senv.fromCollection(iterable).filter(TestUtilities.getFilterFunction());

        source.printToErr();
        LOG.info("==============  ProcessSource Source - PRINTED  ===============");

        SingleOutputStreamOperator<Tuple2<Date, Long>> datasource = PacketCountReader.processSource(senv, source);


//		datasource.printToErr();
        LOG.info("==============  ProcessSource Processed - PRINTED  ===============");
        datasource.addSink(new PacketCountReaderTest.CollectSink());
//		datasource.printToErr();
        LOG.info("==============  ProcessSource Sink - PRINTED  ===============");
        senv.execute();

        for (Tuple2<Date, Long> l : PacketCountReaderTest.CollectSink.values) {
            LOG.info(l.toString());
        }

        long total = 0l;
        for (Tuple2<Date, Long> l : PacketCountReaderTest.CollectSink.values) {
            total+=l.f1;
        }

        // verify your results
        Assert.assertEquals(97l, total);

    }

    private static class CollectSink implements SinkFunction<Tuple2<Date, Long>> {

        // must be static
        public static final List<Tuple2<Date, Long>> values = Collections.synchronizedList(new ArrayList<Tuple2<Date, Long>>());

        @Override
        public void invoke(Tuple2<Date, Long> value) throws Exception {
            values.add(value);
        }
    }


}
