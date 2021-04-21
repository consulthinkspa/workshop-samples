package it.consulthink.oe.flink.packetcount;


import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import com.dellemc.oe.util.AppConfiguration;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.Collector;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.consulthink.oe.model.NMAJSONData;
import junit.framework.Assert;


public class TrafficByDirectionTest {


    private static final Logger LOG = LoggerFactory.getLogger(TrafficByDirectionTest.class);

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster = new MiniClusterWithClientResource(
            new MiniClusterResourceConfiguration.Builder().setNumberSlotsPerTaskManager(6).setNumberTaskManagers(2)
                    .build());


    @Test
    public void testProcessFunction() throws Exception {


        ProcessWindowFunction<NMAJSONData, Tuple3<Long, Long, Long>, String, TimeWindow>
                sumDirectedTraffic = TrafficByDirection.getProcessFunction();

        Collector<Tuple3<Long, Long, Long>> collector = new Collector<Tuple3<Long, Long, Long>>() {

            Long inbound = 0l;
            Long outbound = 0l;
            Long lateral = 0l;

            @Override
            public void collect(Tuple3<Long, Long, Long> record) {
                this.inbound += record.f0;
                this.outbound += record.f1;
                this.lateral += record.f2;
            }

            @Override
            public void close() {
                inbound = outbound = lateral = 0l;
            }

            @Override
            public String toString() {
                return "in: " + String.valueOf(inbound) + ", out: "+ String.valueOf(outbound) + ", lat: "+ String.valueOf(lateral);
            }

              };

        Date example = DateUtils.parseDate("2021-03-21  22:59:59", "yyyy-MM-dd HH:mm:ss");


        List<NMAJSONData> iterable = Arrays.asList(
                new NMAJSONData(example, "", "", "", "", 1l, 0l,0l,0l,0l,0l,0l,0l,0l,0l,0l),
                new NMAJSONData(example, "", "", "", "", 0l, 3l,0l,0l,0l,0l,0l,0l,0l,0l,0l),
                new NMAJSONData(example, "", "", "", "", 3l, 3l,0l,0l,0l,0l,0l,0l,0l,0l,0l),
                new NMAJSONData(example, "", "", "", "", 0l, 4l,0l,0l,0l,0l,0l,0l,0l,0l,0l)
        );

        //test key = Lateral
        String key1 = "Lateral";

        sumDirectedTraffic.process(key1, null, iterable, collector);
        Assert.assertEquals("in: 0, out: 0, lat: 14", collector.toString());

        //flushing old collected values
        collector.close();


        //test key = notLateral
        String key2 = "notLateral";
        sumDirectedTraffic.process(key2, null, iterable, collector);
        Assert.assertEquals("in: 4, out: 10, lat: 0", collector.toString() );


    }


    @Test
    public void testProcessSource() throws Exception {

        //TODO non so chiamare appConfiguration da sezioni statiche di codice
        Set<String> ipList = new HashSet<String>(Arrays.asList((
                "213.61.202.114,213.61.202.115,213.61.202.116," +
                        "213.61.202.117,213.61.202.118,213.61.202.119," +
                        "213.61.202.120,213.61.202.121,213.61.202.122," +
                        ",213.61.202.123,213.61.202.124,213.61.202.125,213.61.202.126").split(",")));


        CollectSink.values.clear();

        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(3);
        senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        ArrayList<NMAJSONData> iterable = readCSV("metrics_23-03-ordered_noq.csv");


        DataStream<NMAJSONData> source = senv.fromCollection(iterable).filter(getFilterFunction());

        source.printToErr();
        LOG.info("==============  ProcessSource Source - PRINTED  ===============");

        SingleOutputStreamOperator<TrafficByDirection.Traffic> datasource =
                TrafficByDirection.processSource(senv, source, ipList);

		datasource.printToErr();
        LOG.info("==============  ProcessSource Processed - PRINTED  ===============");
        datasource.addSink(new CollectSink());
//		datasource.printToErr();
        LOG.info("==============  ProcessSource Sink - PRINTED  ===============");
        senv.execute();

        for (TrafficByDirection.Traffic l : CollectSink.values) {
            LOG.info(l.toString());
        }

        long inbound = 0l;
        long outbound = 0l;
        long lateral = 0l;
        for (TrafficByDirection.Traffic l : CollectSink.values) {
            inbound += l.inbound;
            outbound += l.outbound;
            lateral += l.lateral;
        }

        // verify your results
        Assert.assertEquals(6715l, inbound);
        Assert.assertEquals(21288l, outbound);
        Assert.assertEquals(0l, lateral);


    }



    public static FilterFunction<NMAJSONData> getFilterFunction() {

        FilterFunction<NMAJSONData> filter = new FilterFunction<NMAJSONData>() {

            @Override
            public boolean filter(NMAJSONData value) throws Exception {
                Date min = DateUtils.parseDate("2021-03-21  23:00:00", "yyyy-MM-dd HH:mm:ss");
                Date max = DateUtils.parseDate("2021-03-21  23:00:02", "yyyy-MM-dd HH:mm:ss");
                return value.getTime().after(min) && value.getTime().before(max);
            }

        };
        return filter;
    }

    public static ArrayList<NMAJSONData> readCSV(String csvFileName) throws FileNotFoundException, ParseException {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        Path path = FileSystems.getDefault().getPath("../common/src/main/resources/" + csvFileName).toAbsolutePath();
        File csv = path.toFile();
        Assert.assertTrue(csv.exists() && csv.isFile());

        ArrayList<NMAJSONData> iterable = new ArrayList<NMAJSONData>();
        Scanner myReader = new Scanner(csv);
        String line = myReader.nextLine();
        while (myReader.hasNextLine()) {
            String[] splitted = myReader.nextLine().split(",");
            NMAJSONData tmp = new NMAJSONData(df.parse(splitted[0]),
                    splitted[1], splitted[2], splitted[3], splitted[4],
                    Long.valueOf(splitted[5]), Long.valueOf(splitted[6]), Long.valueOf(splitted[7]),
                    Long.valueOf(splitted[8]), Long.valueOf(splitted[9]), Long.valueOf(splitted[10]),
                    Long.valueOf(splitted[11]), Long.valueOf(splitted[5]), Long.valueOf(splitted[5]),
                    Long.valueOf(splitted[12]), Long.valueOf(splitted[13]));
            iterable.add(tmp);
        }
        myReader.close();
        return iterable;
    }

    private static class CollectSink implements SinkFunction<TrafficByDirection.Traffic> {

        // must be static
        public static final List<TrafficByDirection.Traffic> values = Collections.synchronizedList(new ArrayList<TrafficByDirection.Traffic>());

        @Override
        public void invoke(TrafficByDirection.Traffic value) throws Exception {
            values.add(value);
        }
    }



}
