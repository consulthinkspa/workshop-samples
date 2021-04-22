package it.consulthink.oe.flink.packetcount;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Scanner;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.common.functions.FilterFunction;
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

import it.consulthink.oe.model.NMAJSONData;
import junit.framework.Assert;

public class TotalTrafficReaderTest {
	
	private static final Logger LOG = LoggerFactory.getLogger(TotalTrafficReaderTest.class);
	
	@ClassRule
	public static MiniClusterWithClientResource flinkCluster = new MiniClusterWithClientResource(
			new MiniClusterResourceConfiguration.Builder().setNumberSlotsPerTaskManager(6).setNumberTaskManagers(2)
					.build());

	@Test
	public void testProcessAllWindowFunction() throws Exception {
		ProcessAllWindowFunction<NMAJSONData, Long, TimeWindow> sumBytes = TotalTrafficReader.getProcessAllWindowFunction();

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

		sumBytes.process(null, iterable, collector);

		Assert.assertEquals("10311545", collector.toString());

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
				new NMAJSONData(example, "", "", "", "", 1l, 0l,0l,0l,0l,0l,0l,0l,0l,0l,0l),
				new NMAJSONData(example, "", "", "", "", 0l, 3l,0l,0l,0l,0l,0l,0l,0l,0l,0l),
				new NMAJSONData(example, "", "", "", "", 3l, 3l,0l,0l,0l,0l,0l,0l,0l,0l,0l),
				new NMAJSONData(example, "", "", "", "", 0l, 4l,0l,0l,0l,0l,0l,0l,0l,0l,0l)
			);

		sumBytes.process(example, ctx, iterable, collector);
	
		Assert.assertEquals("14", collector.toString());		

	}
	





	@Test
	public void testProcessSource() throws Exception {
		
		CollectSink.values.clear();
		
		StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
		senv.setParallelism(3);
		senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		ArrayList<NMAJSONData> iterable = TestUtilities.readCSV("metrics_23-03-ordered.csv");

		DataStream<NMAJSONData> source = senv.fromCollection(iterable).filter(TestUtilities.getFilterFunction());
		
		source.printToErr();
		LOG.info("==============  ProcessSource Source - PRINTED  ===============");
		
		SingleOutputStreamOperator<Tuple2<Date, Long>> datasource = TotalTrafficReader.processSource(senv, source);
		
		
//		datasource.printToErr();
		LOG.info("==============  ProcessSource Processed - PRINTED  ===============");
		datasource.addSink(new CollectSink());
//		datasource.printToErr();
		LOG.info("==============  ProcessSource Sink - PRINTED  ===============");
		senv.execute();
		
		for (Tuple2<Date, Long> l : CollectSink.values) {
			LOG.info(l.toString());
		}
		
		long total = 0l;
		for (Tuple2<Date, Long> l : CollectSink.values) {
			total+=l.f1;
		}
		
        // verify your results
        Assert.assertEquals(29233l, total);	

	}
	
	@Test
	public void testProcessSourceUnordered() throws Exception {
		
		CollectSink.values.clear();
		
		StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
		senv.setParallelism(3);
		senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		ArrayList<NMAJSONData> iterable = TestUtilities.readCSV("metrics_23-03.csv");

		DataStream<NMAJSONData> source = senv.fromCollection(iterable);

        source.printToErr();
		LOG.info("==============  ProcessSource Source - PRINTED  ===============");
		
		SingleOutputStreamOperator<Tuple2<Date, Long>> datasource = TotalTrafficReader.processSource(senv, source);
		
		
//		datasource.printToErr();
		LOG.info("==============  ProcessSource Processed - PRINTED  ===============");
		datasource.addSink(new CollectSink());
//		datasource.printToErr();
		LOG.info("==============  ProcessSource Sink - PRINTED  ===============");
		senv.execute();
		
		for (Tuple2<Date, Long> l : CollectSink.values) {
			LOG.info(l.toString());
		}
		
		long total = 0l;
		for (Tuple2<Date, Long> l : CollectSink.values) {
			total+=l.f1;
		}
		
        // verify your results
        Assert.assertFalse(new Long(total).equals(10311545l));	

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
