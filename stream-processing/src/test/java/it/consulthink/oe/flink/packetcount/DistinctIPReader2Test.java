package it.consulthink.oe.flink.packetcount;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.Collector;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dellemc.oe.util.AppConfiguration;

import it.consulthink.oe.model.NMAJSONData;
import junit.framework.Assert;

public class DistinctIPReader2Test {
    private static final Logger LOG = LoggerFactory.getLogger(TrafficByDirectionTest.class);

//    @ClassRule
//    public static MiniClusterWithClientResource flinkCluster = new MiniClusterWithClientResource(
//            new MiniClusterResourceConfiguration.Builder().setNumberSlotsPerTaskManager(6).setNumberTaskManagers(2)
//                    .build());
	@Test
	public void testProcessFunction() throws Exception {
		
		AppConfiguration ac = new AppConfiguration(new String[0]);
		Set<String> myIps = ac.getMyIps();
		
		ProcessWindowFunction<NMAJSONData, Tuple3<String, Integer, Integer>, String, TimeWindow> countDistinct = DistinctIPReader2.getProcessFunction(myIps);

		Date example = DateUtils.parseDate("2021-03-21  22:59:58", "yyyy-MM-dd HH:mm:ss");

		List<NMAJSONData> iterable = Arrays.asList(
					new NMAJSONData(example, "10.10.10.1", "10.10.10.2", "", "", 0l, 0l,0l,0l,0l,0l,0l,0l,0l,0l,0l),
					new NMAJSONData(example, "10.10.10.1", "10.10.10.2", "", "", 0l, 0l,0l,0l,0l,0l,0l,0l,0l,0l,0l),
					new NMAJSONData(example, "10.10.10.1", "10.10.10.2", "", "", 0l, 0l,0l,0l,0l,0l,0l,0l,0l,0l,0l),
					new NMAJSONData(example, "10.10.10.2", "10.10.10.1", "", "", 0l, 0l,0l,0l,0l,0l,0l,0l,0l,0l,0l),
					new NMAJSONData(example, "213.61.202.114", "10.10.10.3", "", "", 0l, 0l,0l,0l,0l,0l,0l,0l,0l,0l,0l)
					
				);

		ProcessWindowFunction<NMAJSONData, Tuple2<Date, Long>, Date, TimeWindow>.Context ctx = null;
		Collector<Tuple3<String, Integer, Integer>> collector = new Collector<Tuple3<String, Integer, Integer>>(){
			Long local = 0l;
			Long external = 0l;

			@Override
			public void collect(Tuple3<String, Integer, Integer> record) {
				LOG.info("B Collect Record, "+record+" : "+this.toString());
				
				local += record.f1;
				external += record.f2;
				
				LOG.info("A Collect Record, "+record+" : "+this.toString());
			}

			@Override
			public void close() {
				LOG.info("On Close: "+this.toString());
				
			}
			
			@Override
			public String toString() {
				return local+","+external;
			}
			
		};
		
		countDistinct.process("2021-03-21  22:59:58", null, iterable, collector);
		Assert.assertEquals("1,3", collector.toString());
		

	}
	
    @Test
    public void testProcessSource() throws Exception {
		AppConfiguration ac = new AppConfiguration(new String[0]);
		Set<String> myIps = ac.getMyIps();

        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(3);
        senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		Date example = DateUtils.parseDate("2021-03-21  22:59:58", "yyyy-MM-dd HH:mm:ss");
		Date example1 = DateUtils.parseDate("2021-03-21  22:59:59", "yyyy-MM-dd HH:mm:ss");

		List<NMAJSONData> iterable = Arrays.asList(
					new NMAJSONData(example, "10.10.10.1", "10.10.10.2", "", "", 0l, 0l,0l,0l,0l,0l,0l,0l,0l,0l,0l),
					new NMAJSONData(example, "10.10.10.1", "10.10.10.2", "", "", 0l, 0l,0l,0l,0l,0l,0l,0l,0l,0l,0l),
					new NMAJSONData(example, "10.10.10.1", "10.10.10.2", "", "", 0l, 0l,0l,0l,0l,0l,0l,0l,0l,0l,0l),
					new NMAJSONData(example, "10.10.10.2", "10.10.10.1", "", "", 0l, 0l,0l,0l,0l,0l,0l,0l,0l,0l,0l),
					new NMAJSONData(example, "213.61.202.114", "10.10.10.3", "", "", 0l, 0l,0l,0l,0l,0l,0l,0l,0l,0l,0l),
					new NMAJSONData(example1, "213.61.202.114", "10.10.10.3", "", "", 0l, 0l,0l,0l,0l,0l,0l,0l,0l,0l,0l)
					
				);
		
		iterable = TestUtilities.readCSV("metrics_23-03-ordered.csv");

        DataStream<NMAJSONData> source = senv.fromCollection(iterable);

        source.printToErr();
        LOG.info("==============  ProcessSource Source - PRINTED  ===============");

        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> datasource = DistinctIPReader2.processSource(senv, source, myIps);


//		datasource.printToErr();
        LOG.info("==============  ProcessSource Processed - PRINTED  ===============");
        datasource.addSink(new CollectSink());
//		datasource.printToErr();
        LOG.info("==============  ProcessSource Sink - PRINTED  ===============");
        senv.execute();

//        for (Tuple2<Date, Long> l : PacketCountReaderTest.CollectSink.values) {
//            LOG.info(l.toString());
//        }
//
//        long total = 0l;
//        for (Tuple2<Date, Long> l : PacketCountReaderTest.CollectSink.values) {
//            total+=l.f1;
//        }

        // verify your results
//        Assert.assertEquals(97l, total);

    }

    private static class CollectSink implements SinkFunction<Tuple3<String, Integer, Integer>> {

        // must be static
        public static final List<Tuple3<String, Integer, Integer>> values = Collections.synchronizedList(new ArrayList<Tuple3<String, Integer, Integer>>());

        @Override
        public void invoke(Tuple3<String, Integer, Integer> value) throws Exception {
        	LOG.info("sink invoke: " +value);
            values.add(value);
        }
    }
}
