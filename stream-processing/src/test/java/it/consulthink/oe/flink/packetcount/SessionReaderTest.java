package it.consulthink.oe.flink.packetcount;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dellemc.oe.util.AppConfiguration;

import it.consulthink.oe.model.NMAJSONData;
import junit.framework.Assert;

public class SessionReaderTest {
    private static final Logger LOG = LoggerFactory.getLogger(SessionReaderTest.class);

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster = new MiniClusterWithClientResource(
            new MiniClusterResourceConfiguration.Builder().setNumberSlotsPerTaskManager(6).setNumberTaskManagers(2)
                    .build());

	
    @Test
    public void testProcessSource() throws Exception {
    	CollectSink.values.clear();
    	
		AppConfiguration ac = new AppConfiguration(new String[0]);
		Set<String> myIps = ac.getMyIps();

        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(3);
        senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		Date example = DateUtils.parseDate("2021-03-21  22:59:58", "yyyy-MM-dd HH:mm:ss");
		Date example1 = DateUtils.parseDate("2021-03-21  22:59:59", "yyyy-MM-dd HH:mm:ss");

		List<NMAJSONData> iterable = Arrays.asList(
					new NMAJSONData(example, "10.10.10.1",     "10.10.10.2", "443", "12345", 0l, 0l,0l,0l,0l,1l,1l,0l,0l,0l,0l,0l),
					new NMAJSONData(example, "10.10.10.1",     "10.10.10.2", "443", "12345", 0l, 0l,0l,0l,0l,0l,2l,0l,0l,0l,0l,0l),
					new NMAJSONData(example, "10.10.10.1",     "10.10.10.2", "443", "12345", 0l, 0l,0l,0l,0l,1l,3l,0l,0l,0l,0l,0l),
					new NMAJSONData(example, "10.10.10.2",     "10.10.10.1", "12345", "443", 0l, 0l,0l,0l,0l,0l,4l,0l,0l,1l,0l,0l),
					new NMAJSONData(example, "213.61.202.114", "10.10.10.3", "8083", "12347", 0l, 0l,0l,0l,0l,0l,5l,0l,0l,0l,0l,0l),
					new NMAJSONData(example1,"213.61.202.114", "10.10.10.3", "8083", "12347", 0l, 0l,0l,0l,0l,0l,6l,0l,0l,0l,0l,0l)
					
				);
		
//		iterable = TestUtilities.readCSV("metrics_23-03-ordered.csv");

        DataStream<NMAJSONData> source = senv.fromCollection(iterable);

        source.printToErr();
        LOG.info("==============  ProcessSource Source - PRINTED  ===============");

       SingleOutputStreamOperator<Tuple4<String, Integer, Integer, Integer>> datasource = SessionReader.processSource(senv, source);


//		datasource.printToErr();
        LOG.info("==============  ProcessSource Processed - PRINTED  ===============");
        CollectSink sink = new CollectSink();
		datasource.addSink(sink);
//		datasource.printToErr();
        LOG.info("==============  ProcessSource Sink - PRINTED  ===============");
        senv.execute();



        // verify your results
        Assert.assertEquals(2, sink.values.size());
        
        Assert.assertEquals("2021-03-21 22:59:58", CollectSink.values.get(0).f0);
        Assert.assertEquals(Integer.valueOf(0), CollectSink.values.get(0).f1);
        Assert.assertEquals(Integer.valueOf(1), CollectSink.values.get(0).f2);
        Assert.assertEquals(Integer.valueOf(1), CollectSink.values.get(0).f3);
//        
        Assert.assertEquals("2021-03-21 22:59:59", CollectSink.values.get(1).f0);
        Assert.assertEquals(Integer.valueOf(0), CollectSink.values.get(1).f1);
        Assert.assertEquals(Integer.valueOf(1), CollectSink.values.get(1).f2);
        Assert.assertEquals(Integer.valueOf(0), CollectSink.values.get(1).f3);

    }
    
    @Test
    public void testProcessSourceFromXLSX() throws Exception {
    	CollectSink.values.clear();
    	
		AppConfiguration ac = new AppConfiguration(new String[0]);
		Set<String> myIps = ac.getMyIps();

        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(3);
        senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		Date example = DateUtils.parseDate("2021-03-21  22:59:58", "yyyy-MM-dd HH:mm:ss");
		Date example1 = DateUtils.parseDate("2021-03-21  22:59:59", "yyyy-MM-dd HH:mm:ss");

		List<NMAJSONData> iterable = TestUtilities.readCSV("metrics_23-03-ordered.csv");

        DataStream<NMAJSONData> source = senv.fromCollection(iterable).filter(TestUtilities.getFilterFunction());

        source.printToErr();
        LOG.info("==============  ProcessSource Source - PRINTED  ===============");

        SingleOutputStreamOperator<Tuple4<String, Integer, Integer, Integer>> datasource = SessionReader.processSource(senv, source);


//		datasource.printToErr();
        LOG.info("==============  ProcessSource Processed - PRINTED  ===============");
        CollectSink sink = new CollectSink();
		datasource.addSink(sink);
//		datasource.printToErr();
        LOG.info("==============  ProcessSource Sink - PRINTED  ===============");
        senv.execute();


        List<Tuple4<String, Integer, Integer, Integer>> values = CollectSink.values;
        for (Tuple4<String, Integer, Integer, Integer> t : values) {
			LOG.info("Sessions: "+t);
		}
        
        
        // verify your results
        Assert.assertEquals(3, CollectSink.values.size());
        
    }    

    private static class CollectSink implements SinkFunction<Tuple4<String, Integer, Integer, Integer>> {

        // must be static
        public static final List<Tuple4<String, Integer, Integer, Integer>> values = Collections.synchronizedList(new ArrayList<Tuple4<String, Integer, Integer, Integer>>());

        @Override
        public void invoke(Tuple4<String, Integer, Integer, Integer> value) throws Exception {
        	LOG.info("sink invoke: " +value);
            values.add(value);
        }
    }
}
