package it.consulthink.oe.flink.packetcount;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Scanner;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
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
	public void testProcessFunction() throws Exception {
		ProcessAllWindowFunction<NMAJSONData, Long, TimeWindow> sumBytes = TotalTrafficReader.getProcessFunction();

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

		List<NMAJSONData> iterable = readCSV();

		sumBytes.process(null, iterable, collector);

		Assert.assertEquals("10311545", collector.toString());

	}





	@Test
	public void testProcessSource() throws Exception {
		
		CollectSink.values.clear();
		
		StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
		senv.setParallelism(3);
		senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		ArrayList<NMAJSONData> iterable = readCSV();

		DataStream<NMAJSONData> source = senv.fromCollection(iterable).filter(getFilterFunction());
		
		
		SingleOutputStreamOperator<Long> datasource = TotalTrafficReader.processSource(senv, source);
		
		datasource.printToErr();
		LOG.info("==============  ProcessSource - PRINTED  ===============");
		datasource.addSink(new CollectSink());
		senv.execute();
		
		for (Long l : CollectSink.values) {
			LOG.info(l.toString());
		}
		
		long total = 0l;
		for (Long l : CollectSink.values) {
			total+=l;
		}
		
        // verify your results
        Assert.assertEquals(426l, total);	

	}





	public static FilterFunction<NMAJSONData> getFilterFunction() {
		FilterFunction<NMAJSONData> filter = new FilterFunction<NMAJSONData>() {

			@Override
			public boolean filter(NMAJSONData value) throws Exception {
				Date min = DateUtils.parseDate("2021-03-21  22:59:59", "yyyy-MM-dd HH:mm:ss");
				Date max = DateUtils.parseDate("2021-03-21  23:00:01", "yyyy-MM-dd HH:mm:ss");
				return value.getTime().after(min) && value.getTime().before(max);
			}
			
		};
		return filter;
	}

	public static ArrayList<NMAJSONData> readCSV() throws FileNotFoundException, ParseException {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		Path path = FileSystems.getDefault().getPath("../common/src/main/resources/metrics_23-03-ordered.csv").toAbsolutePath();
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
	
	private static class CollectSink implements SinkFunction<Long> {

        // must be static
        public static final List<Long> values = Collections.synchronizedList(new ArrayList<Long>());

        @Override
        public void invoke(Long value) throws Exception {
            values.add(value);
        }
    }	

}
