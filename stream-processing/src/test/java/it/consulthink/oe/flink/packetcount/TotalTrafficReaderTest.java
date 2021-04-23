package it.consulthink.oe.flink.packetcount;

import static org.junit.Assert.assertThat;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
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
	
     
 
    /**
     * Database initialization for testing i.e.
     * <ul>
     * <li>Creating Table</li>
     * <li>Inserting record</li>
     * </ul>
     * 
     * @throws SQLException
     */
    private static void initDatabase() throws SQLException {
        try (Connection connection = getConnection(); Statement statement = connection.createStatement();) {
            statement.execute("CREATE TABLE total_traffic (event_time TIMESTAMP NOT NULL, traffic BIGINT  NOT NULL, PRIMARY KEY (event_time))");
            connection.commit();
//            statement.executeUpdate(
//                    "INSERT INTO employee VALUES (1001,'Vinod Kumar Kashyap', 'vinod@javacodegeeks.com')");
//            statement.executeUpdate("INSERT INTO employee VALUES (1002,'Dhwani Kashyap', 'dhwani@javacodegeeks.com')");
//            statement.executeUpdate("INSERT INTO employee VALUES (1003,'Asmi Kashyap', 'asmi@javacodegeeks.com')");
            connection.commit();
        }
    }
 
    /**
     * Create a connection
     * 
     * @return connection object
     * @throws SQLException
     */
    private static Connection getConnection() throws SQLException {
        return DriverManager.getConnection("jdbc:hsqldb:mem:nmadb", "nma", "nma");
    }	
	
	@Test
	public void testProcessSourceToJdbc() throws Exception {
		
        Class.forName("org.hsqldb.jdbc.JDBCDriver");
        
        // initialize database
        initDatabase();		
		

		
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
		datasource.addSink(JdbcSink.sink(
	            "insert into total_traffic (event_time, traffic) values (?,?)",
	            (ps, t) -> {
	                ps.setTimestamp(1, java.sql.Timestamp.from(t.f0.toInstant()));
	                ps.setLong(2, t.f1);
	            },
	            new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
	                    .withUrl("jdbc:hsqldb:mem:nmadb")
	                    .withUsername("nma")
	                    .withPassword("nma")
	                    .withDriverName("org.hsqldb.jdbc.JDBCDriver")
	                    .build()));
//		datasource.printToErr();
		LOG.info("==============  ProcessSource Sink - PRINTED  ===============");
		senv.execute();
		
        
        try (Connection connection = getConnection();
				Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
						ResultSet.CONCUR_READ_ONLY);) {

			ResultSet result = statement.executeQuery("SELECT SUM(traffic) FROM total_traffic");

			if (result.first()) {
				Assert.assertEquals(29233l, result.getLong(0));
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}
        
        
        try (Connection connection = getConnection(); Statement statement = connection.createStatement();) {
            statement.executeUpdate("DROP TABLE total_traffic");
            connection.commit();
        }        

	}	
	
	

	
	@Test
	public void testProcessSourceReduced() throws Exception {
		
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
		datasource
			.keyBy(new KeySelector<Tuple2<Date,Long>, Date>(){

				@Override
				public Date getKey(Tuple2<Date, Long> value) throws Exception {
					return value.f0;
				}
				
			})
			.window(TumblingEventTimeWindows.of(Time.seconds(1)))
			.reduce(new ReduceFunction<Tuple2<Date,Long>>() {
				
				@Override
				public Tuple2<Date, Long> reduce(Tuple2<Date, Long> value1, Tuple2<Date, Long> value2) throws Exception {
					
					
					if (value1.f0.equals(value2.f0))
						return Tuple2.of(value1.f0, value1.f1 + value2.f1);
					LOG.error(""+value1+" "+value2);
					throw new RuntimeException();
				}
			})
			.addSink(new CollectSink());
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
        
        Assert.assertEquals(3, CollectSink.values.size());
        

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
		private static final Logger LOG = LoggerFactory.getLogger(CollectSink.class);
        // must be static
        public static final List<Tuple2<Date, Long>> values = Collections.synchronizedList(new ArrayList<Tuple2<Date, Long>>());

        @Override
        public void invoke(Tuple2<Date, Long> value) throws Exception {
        	LOG.info("collect:" + value);
            values.add(value);
        }
    }

}
