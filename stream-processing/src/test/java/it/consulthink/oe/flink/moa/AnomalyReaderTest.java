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

import it.consulthink.oe.flink.packetcount.TestUtilities;
import it.consulthink.oe.ingest.NMAJSONDataGenerator;
import it.consulthink.oe.model.NMAJSONData;
import junit.framework.Assert;
import moa.cluster.Cluster;
import moa.clusterers.streamkm.StreamKM;

public class AnomalyReaderTest {
	private static final double TOLLERANCE = AnomalyReader.TOLLERANCE;

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

		iterable.clear();
		anomalies.clear();
		int streamLimit = 2600;
		Stream<NMAJSONData> generateInfiniteStream = NMAJSONDataGenerator.generateInfiniteStream(myIps);
		generateInfiniteStream.limit(500).forEach(data -> {
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

//		ArrayList<NMAJSONData> readCSV = TestUtilities.readCSV("metrics_23-03-ordered.csv");

		int lengthOption = streamLimit;
		int numClustersOption = 1;
		StreamKM defaultStreamKM = AnomalyReader.generateStreamKM(lengthOption, numClustersOption);
		Stream<NMAJSONData> stream = NMAJSONDataGenerator.generateInfiniteStreamNoAnomaly(myIps).limit(streamLimit);
		AnomalyReader.trainStreamKM(stream);
		SingleOutputStreamOperator<Tuple3<NMAJSONData, Double, Cluster>> datasource = AnomalyReader.processSource(senv,
				source);

		CollectSink sink = new CollectSink();
		datasource.addSink(sink);
		senv.execute();

		Collection<NMAJSONData> finding = new ArrayList<NMAJSONData>();

		for (Tuple3<NMAJSONData, Double, Cluster> t : sink.values) {
			finding.add(t.f0);
		}


		if (anomalies.size() > 0) {
			
//			Assert.assertTrue((anomalies.size() * 1.3) >= finding.size());
//			Assert.assertTrue((finding.size() * 1.3) >= anomalies.size());
			
			ArrayList correctPrediction = new ArrayList(CollectionUtils.intersection(anomalies, finding));

			for (Object prediction : correctPrediction) {
				LOG.info("*** PREDICTION *** " + prediction);
			}			
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


	}

	@Test
	public void testTrainingFromXlsx() throws Exception {

		AppConfiguration ac = new AppConfiguration(new String[0]);
		Set<String> myIps = ac.getMyIps();

		ArrayList<NMAJSONData> readCSV = TestUtilities.readCSV("metrics_23-03-ordered.csv");
		int streamLimit = readCSV.size();
		for (int lengthOption = streamLimit - 100; lengthOption < streamLimit + 200; lengthOption += 100) {
			for (int numClustersOption = 4; numClustersOption > 0; numClustersOption--) {
				StreamKM defaultStreamKM = AnomalyReader.generateStreamKM(lengthOption, numClustersOption);
				Stream<NMAJSONData> stream = readCSV.stream();
				AnomalyReader.trainStreamKM(stream);
				int size = defaultStreamKM.getClusteringResult().size();
				int dimension = defaultStreamKM.getClusteringResult().dimension();

				for (int a = 0; a < 10; a++) {
					NMAJSONData anomaly = NMAJSONDataGenerator.generateAnomaly(myIps);
					double maxInclusionProbability = defaultStreamKM.getClusteringResult()
							.getMaxInclusionProbability(AnomalyReader.toDenseInstance(anomaly));
					Assert.assertEquals(
							"result size: " + size + ", result dimension: " + dimension + ", lengthOption: "
									+ lengthOption + ", numClustersOption: " + numClustersOption + ", anomaly #" + a,
							0d, maxInclusionProbability, TOLLERANCE);
				}

			}
		}

	}

	@Test
	public void testTrainingFromGenerator() throws Exception {

		AppConfiguration ac = new AppConfiguration(new String[0]);
		Set<String> myIps = ac.getMyIps();

		for (int streamLimit = 3000; streamLimit > 2500; streamLimit -= 200) {
//			System.err.println("streamLimit "+streamLimit);
			for (int lengthOption = streamLimit - 50; lengthOption < streamLimit + 10000; lengthOption += 5000) {
//				System.err.println("lengthOption "+lengthOption);
				for (int numClustersOption = 3; numClustersOption > 2; numClustersOption--) {
//					System.err.println("numClustersOption "+numClustersOption);
					StreamKM defaultStreamKM = AnomalyReader.generateStreamKM(lengthOption, numClustersOption);

					Stream<NMAJSONData> stream = NMAJSONDataGenerator.generateInfiniteStreamNoAnomaly(myIps)
							.limit(streamLimit);
					AnomalyReader.trainStreamKM(stream);
					int size = defaultStreamKM.getClusteringResult().size();
					int dimension = defaultStreamKM.getClusteringResult().dimension();

					for (int a = 0; a < 10; a++) {
						NMAJSONData anomaly = NMAJSONDataGenerator.generateAnomaly(myIps);
						double maxInclusionProbability = defaultStreamKM.getClusteringResult()
								.getMaxInclusionProbability(AnomalyReader.toDenseInstance(anomaly));
						Assert.assertEquals("result size: " + size + ", result dimension: " + dimension
								+ ", lengthOption: " + lengthOption + ", numClustersOption: " + numClustersOption
								+ ", anomaly #" + a, 0d, maxInclusionProbability, TOLLERANCE);
					}

				}
			}
		}
	}
	
	@Test
	public void testAnomalyGenerator() throws Exception {

		AppConfiguration ac = new AppConfiguration(new String[0]);
		Set<String> myIps = ac.getMyIps();
		int streamLimit = 2600;
		int lengthOption = streamLimit;
		int numClustersOption = 3;
		
		StreamKM defaultStreamKM = AnomalyReader.generateStreamKM(lengthOption, numClustersOption);

		Stream<NMAJSONData> stream = NMAJSONDataGenerator.generateInfiniteStreamNoAnomaly(myIps)
				.limit(streamLimit);
		AnomalyReader.trainStreamKM(stream);
		int size = defaultStreamKM.getClusteringResult().size();
		int dimension = defaultStreamKM.getClusteringResult().dimension();

		for (int a = 0; a < 100; a++) {
			NMAJSONData anomaly = NMAJSONDataGenerator.generateStandard(myIps);
			double k = 0.8;
			if (NMAJSONDataGenerator.r.nextInt(5) != 0) {
				anomaly.setBytesin(Math.round(anomaly.getBytesin() + (NMAJSONDataGenerator.MAX_BYTESIN * k) ));
			}else {
				anomaly.setBytesin(Math.max(Math.round(anomaly.getBytesin() - (NMAJSONDataGenerator.MAX_BYTESIN * k) ),0));
			}
			if (NMAJSONDataGenerator.r.nextInt(5) != 0) {
				anomaly.setBytesout(Math.round(anomaly.getBytesout() + (NMAJSONDataGenerator.MAX_BYTESOUT * k)));
			}else {
				anomaly.setBytesout(Math.max(Math.round(anomaly.getBytesout() - (NMAJSONDataGenerator.MAX_BYTESOUT * k) ),0));
			}
			
			if (NMAJSONDataGenerator.r.nextInt(5) != 0) {
				anomaly.setPktsin(Math.round(anomaly.getPktsin() + (NMAJSONDataGenerator.MAX_PACKETS_IN * k) ));
			}else {
				anomaly.setPktsin(Math.max(Math.round(anomaly.getPktsin() - (NMAJSONDataGenerator.MAX_PACKETS_IN * k) ),0));
			}
			if (NMAJSONDataGenerator.r.nextInt(5) != 0) {
				anomaly.setPktsout(Math.round(anomaly.getPktsout() + (NMAJSONDataGenerator.MAX_PACKETS_OUT * k)));
			}else {
				anomaly.setPktsout(Math.max(Math.round(anomaly.getPktsout() - (NMAJSONDataGenerator.MAX_PACKETS_OUT * k) ),0));
			}				
			
			anomaly.setPkts(anomaly.getPktsin() + anomaly.getPktsout());

			
			
			
			double maxInclusionProbability = defaultStreamKM.getClusteringResult()
					.getMaxInclusionProbability(AnomalyReader.toDenseInstance(anomaly));
			Assert.assertEquals("result size: " + size + ", result dimension: " + dimension
					+ ", lengthOption: " + lengthOption + ", numClustersOption: " + numClustersOption
					+ ", anomaly #" + a, 0d, maxInclusionProbability, 0);
		}		
				
	}	

	private static class CollectSink implements SinkFunction<Tuple3<NMAJSONData, Double, Cluster>> {

		// must be static
		public static final List<Tuple3<NMAJSONData, Double, Cluster>> values = Collections
				.synchronizedList(new ArrayList<Tuple3<NMAJSONData, Double, Cluster>>());

		@Override
		public void invoke(Tuple3<NMAJSONData, Double, Cluster> value) throws Exception {
			LOG.info("sink invoke: " + value);
			values.add(value);
		}
	}
}
