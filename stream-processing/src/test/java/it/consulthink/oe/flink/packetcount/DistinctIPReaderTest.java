package it.consulthink.oe.flink.packetcount;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Date;
import java.util.Hashtable;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
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
		
		ProcessWindowFunction<NMAJSONData, Tuple3<Date, Hashtable<String, Long>, Hashtable<String, Long>>, String, TimeWindow> countDistinct = DistinctIPReader.getProcessFunction(myIps);

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
		
		countDistinct.process("", null, iterable, collector);
		Assert.assertEquals("1,3", collector.toString());
		

	}

}
