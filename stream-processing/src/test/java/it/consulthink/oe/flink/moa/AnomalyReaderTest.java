package it.consulthink.oe.flink.moa;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.java.tuple.Tuple3;
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

import it.consulthink.oe.ingest.NMAJSONDataGenerator;
import it.consulthink.oe.model.NMAJSONData;
import junit.framework.Assert;
import moa.cluster.Cluster;

public class AnomalyReaderTest {
    private static final Logger LOG = LoggerFactory.getLogger(AnomalyReaderTest.class);

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
		final List<NMAJSONData> iterable = new ArrayList<NMAJSONData>();
		final List<NMAJSONData> anomalies = new ArrayList<NMAJSONData>();
		
//		iterable = Arrays.asList(
//					new NMAJSONData(example, "10.10.10.1",     "10.10.10.2", "443", "12345", 0l, 0l,0l,0l,0l,1l,1l,0l,0l,0l,0l,0l),
//					new NMAJSONData(example, "10.10.10.1",     "10.10.10.2", "443", "12345", 0l, 0l,0l,0l,0l,0l,2l,0l,0l,0l,0l,0l),
//					new NMAJSONData(example, "10.10.10.1",     "10.10.10.2", "443", "12345", 0l, 0l,0l,0l,0l,1l,3l,0l,0l,0l,0l,0l),
//					new NMAJSONData(example, "10.10.10.2",     "10.10.10.1", "12345", "443", 0l, 0l,0l,0l,0l,0l,4l,0l,0l,1l,0l,0l),
//					new NMAJSONData(example, "213.61.202.114", "10.10.10.3", "8083", "12347", 0l, 0l,0l,0l,0l,0l,5l,0l,0l,0l,0l,0l),
//					new NMAJSONData(example1,"213.61.202.114", "10.10.10.3", "8083", "12347", 0l, 0l,0l,0l,0l,0l,6l,0l,0l,0l,0l,0l)
//					
//				);
		
//		iterable = TestUtilities.readCSV("metrics_23-03-ordered.csv");
		
		
		iterable.clear();
		anomalies.clear();
		Stream<NMAJSONData> generateInfiniteStream = NMAJSONDataGenerator.generateInfiniteStream(myIps);
		generateInfiniteStream
			.limit(120)
			.forEach(data -> {
				if (data.getClass().equals(NMAJSONDataGenerator.NMAJSONDataAnomaly.class)) {
					anomalies.add(data);
				}
				iterable.add(data);
			});
		NMAJSONData anomaly = NMAJSONDataGenerator.generateAnomaly(myIps);
		anomalies.add(anomaly);
		iterable.add(anomaly);
		Assert.assertTrue(anomalies.size() >0);
        DataStream<NMAJSONData> source = senv.fromCollection(iterable);
        
        LOG.info("==============  ProcessSource Source - PRINTED  ===============");
        AnomalyReader.trainStreamKM(ac, 50);
       SingleOutputStreamOperator<Tuple3<NMAJSONData, Double, Cluster>> datasource = AnomalyReader.processSource(senv, source);


		
//		datasource.printToErr();
        LOG.info("==============  ProcessSource Processed - PRINTED  ===============");
        CollectSink sink = new CollectSink();
		datasource.filter(t -> {
			return (t.f2 == null || t.f1 <= 0.2d);
		}).addSink(sink);
//		datasource.printToErr();
        LOG.info("==============  ProcessSource Sink - PRINTED  ===============");
        senv.execute();

        Collection<NMAJSONData> finding = new ArrayList<NMAJSONData>();
        
        for (Tuple3<NMAJSONData, Double, Cluster> t : sink.values) {
        	finding.add(t.f0);
		}
        
        
        Assert.assertEquals(anomalies.size(), finding.size());
        
        ArrayList correctPrediction = new ArrayList(CollectionUtils.intersection(anomalies, finding));
        
        LOG.info("============== START "+correctPrediction.size()+"  correctPrediction ===============");
        for (Object prediction : correctPrediction) {
			LOG.info(""+prediction);
		}
        LOG.info("============== END   "+correctPrediction.size()+"  correctPrediction ===============");
        
        Assert.assertTrue((correctPrediction.size() / anomalies.size()) >= 0.1);
        Assert.assertTrue((correctPrediction.size() / anomalies.size()) >= 0.2);
        Assert.assertTrue((correctPrediction.size() / anomalies.size()) >= 0.3);
        Assert.assertTrue((correctPrediction.size() / anomalies.size()) >= 0.4);
        Assert.assertTrue((correctPrediction.size() / anomalies.size()) >= 0.5);
        Assert.assertTrue((correctPrediction.size() / anomalies.size()) >= 0.6);
        Assert.assertTrue((correctPrediction.size() / anomalies.size()) >= 0.7);
        Assert.assertTrue((correctPrediction.size() / anomalies.size()) >= 0.8);
        Assert.assertTrue((correctPrediction.size() / anomalies.size()) >= 0.9);
        // verify your results
        Assert.assertEquals(anomalies.size(), correctPrediction.size());
        


    }
    
    private static class CollectSink implements SinkFunction<Tuple3<NMAJSONData, Double, Cluster>> {

        // must be static
        public static final List<Tuple3<NMAJSONData, Double, Cluster>> values = Collections.synchronizedList(new ArrayList<Tuple3<NMAJSONData, Double, Cluster>>());

        @Override
        public void invoke(Tuple3<NMAJSONData, Double, Cluster> value) throws Exception {
        	LOG.info("sink invoke: " +value);
            values.add(value);
        }
    }
}
