package it.consulthink.oe.flink.packetcount;

import static org.junit.Assert.*;

import java.util.*;

import org.apache.commons.lang3.time.DateUtils;
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
import org.apache.flink.streaming.experimental.CollectSink;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.Collector;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dellemc.oe.util.AppConfiguration;

import it.consulthink.oe.model.NMAJSONData;
import junit.framework.Assert;

public class DistinctIPReaderTest {
    private static final Logger LOG = LoggerFactory.getLogger(TrafficByDirectionTest.class);

//    @ClassRule
//    public static MiniClusterWithClientResource flinkCluster = new MiniClusterWithClientResource(
//            new MiniClusterResourceConfiguration.Builder().setNumberSlotsPerTaskManager(6).setNumberTaskManagers(2)
//                    .build());
	@Test
	public void testProcessFunction() throws Exception {
		
		AppConfiguration ac = new AppConfiguration(new String[0]);
		Set<String> myIps = ac.getMyIps();
		
		ProcessWindowFunction<NMAJSONData, Tuple3<Date, Hashtable<String, Long>, Hashtable<String, Long>>, Date, TimeWindow> countDistinct = DistinctIPReader.getProcessFunction(myIps);

		Date example = DateUtils.parseDate("2021-03-21  22:59:59", "yyyy-MM-dd HH:mm:ss");

		List<NMAJSONData> iterable = Arrays.asList(
					new NMAJSONData(example, "10.10.10.1", "10.10.10.2", "", "", 0l, 0l,0l,0l,0l,0l,0l,0l,0l,0l,0l),
					new NMAJSONData(example, "10.10.10.1", "10.10.10.2", "", "", 0l, 0l,0l,0l,0l,0l,0l,0l,0l,0l,0l),
					new NMAJSONData(example, "10.10.10.1", "10.10.10.2", "", "", 0l, 0l,0l,0l,0l,0l,0l,0l,0l,0l,0l),
					new NMAJSONData(example, "10.10.10.2", "10.10.10.1", "", "", 0l, 0l,0l,0l,0l,0l,0l,0l,0l,0l,0l),
					new NMAJSONData(example, "213.61.202.114", "10.10.10.3", "", "", 0l, 0l,0l,0l,0l,0l,0l,0l,0l,0l,0l)
					
				);

		ProcessWindowFunction<NMAJSONData, Tuple2<Date, Long>, Date, TimeWindow>.Context ctx = null;
		Collector<Tuple3<Date, Hashtable<String, Long>, Hashtable<String, Long>>> collector = new Collector<Tuple3<Date, Hashtable<String, Long>, Hashtable<String, Long>>>(){
			Long local = 0l;
			Long external = 0l;

			@Override
			public void collect(Tuple3<Date, Hashtable<String, Long>, Hashtable<String, Long>> record) {
				local += record.f1.size();
				external += record.f2.size();
				
			}

			@Override
			public void close() {
				// TODO Auto-generated method stub
				
			}
			
			@Override
			public String toString() {
				return local+","+external;
			}
			
		};
		
		countDistinct.process(example, null, iterable, collector);
		Assert.assertEquals("1,3", collector.toString());
		

	}


	@Test
	public void testProcessSource() throws Exception{

		CollectSink.values.clear();

		StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
		senv.setParallelism(3);
		senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


		AppConfiguration ac = new AppConfiguration(new String[0]);
		Set<String> myIps = ac.getMyIps();

		Date example1 = DateUtils.parseDate("2021-03-21  22:59:59", "yyyy-MM-dd HH:mm:ss");
		Date example2 = DateUtils.parseDate("2021-03-21  23:00:00", "yyyy-MM-dd HH:mm:ss");
		Date example3 = DateUtils.parseDate("2021-03-21  23:00:01", "yyyy-MM-dd HH:mm:ss");

		List<NMAJSONData> iterable = Arrays.asList(
				new NMAJSONData(example1, "10.10.10.1", "10.10.10.2", "", "", 0l, 0l,0l,0l,0l,0l,0l,0l,0l,0l,0l),
				new NMAJSONData(example1, "10.10.10.1", "10.10.10.2", "", "", 0l, 0l,0l,0l,0l,0l,0l,0l,0l,0l,0l),
				new NMAJSONData(example2, "10.10.10.1", "10.10.10.2", "", "", 0l, 0l,0l,0l,0l,0l,0l,0l,0l,0l,0l),
				new NMAJSONData(example2, "10.10.10.2", "10.10.10.1", "", "", 0l, 0l,0l,0l,0l,0l,0l,0l,0l,0l,0l),
				new NMAJSONData(example3, "213.61.202.114", "10.10.10.3", "", "", 0l, 0l,0l,0l,0l,0l,0l,0l,0l,0l,0l),
				new NMAJSONData(example3, "213.61.202.118", "10.10.10.4", "", "", 0l, 0l,0l,0l,0l,0l,0l,0l,0l,0l,0l)
		);

		DataStream<NMAJSONData> source = senv.fromCollection(iterable);

		//source.printToErr();
		LOG.info("==============  ProcessSource Source - PRINTED  ===============");

		SingleOutputStreamOperator<Tuple3<Date, Long, Long>> datasource = DistinctIPReader.processSource(senv, source,myIps);

		//		datasource.printToErr();
		LOG.info("==============  ProcessSource Processed - PRINTED  ===============");
		datasource.addSink(new CollectSink());
//		datasource.printToErr();
		LOG.info("==============  ProcessSource Sink - PRINTED  ===============");
		senv.execute();

		for (Tuple3<Date, Long, Long> l : CollectSink.values) {
			LOG.info("==============  COLLECTED VALUES  ===============");
			LOG.info("Date: "+l.f0.toString() + ", f1: "+l.f1.toString() +", f2: "+ l.f2.toString());
		}


		Assert.assertEquals(3, CollectSink.values.size());



	}



	private static class CollectSink implements SinkFunction<Tuple3<Date, Long, Long>> {

		// must be static
		public static final List<Tuple3<Date, Long, Long>> values = Collections.synchronizedList(new ArrayList<Tuple3<Date, Long, Long>>());

		@Override
		public void invoke(Tuple3<Date, Long, Long> value) throws Exception {
			values.add(value);
		}


	}


}
