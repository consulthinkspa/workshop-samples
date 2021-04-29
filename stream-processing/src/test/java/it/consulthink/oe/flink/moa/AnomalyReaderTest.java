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
import org.apache.commons.math3.stat.inference.TestUtils;
import org.apache.flink.api.java.tuple.Tuple2;
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
		CollectSink.clear();

		AppConfiguration ac = new AppConfiguration(new String[0]);
		Set<String> myIps = ac.getMyIps();

		StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
		senv.setParallelism(3);
		senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		final List<NMAJSONData> input = new ArrayList<NMAJSONData>();
		final List<NMAJSONData> anomalies = new ArrayList<NMAJSONData>();

		input.clear();
		anomalies.clear();
		int streamLimit = 5000;
		int inputElements = 100000;
		Stream<NMAJSONData> generateInfiniteStream = NMAJSONDataGenerator.generateInfiniteStream(myIps);
		generateInfiniteStream.limit(inputElements).forEach(data -> {
			if (data.getClass().equals(NMAJSONDataGenerator.NMAJSONDataAnomaly.class)) {
				anomalies.add(data);
			}
			input.add(data);
		});
		NMAJSONData anomaly = NMAJSONDataGenerator.generateAnomaly(myIps);
		anomalies.add(anomaly);
		input.add(anomaly);
		
		Assert.assertTrue(anomalies.size() >0);

		DataStream<NMAJSONData> source = senv.fromCollection(input);


		int lengthOption = streamLimit;
		int numClustersOption = 2;
		StreamKM defaultStreamKM = AnomalyReader.generateStreamKM(lengthOption, numClustersOption);
		Stream<NMAJSONData> stream = NMAJSONDataGenerator.generateInfiniteStreamNoAnomaly(myIps).limit(streamLimit);
		AnomalyReader.trainStreamKM(stream);
		
		SingleOutputStreamOperator<Tuple3<NMAJSONData, Double, Cluster>> datasource = AnomalyReader.processSource(senv,	source);

		CollectSink sink = new CollectSink(input, anomalies);
		datasource.addSink(sink);
		senv.execute();
		System.err.println(sink.toAnomalyString());
		System.err.println(sink.toFalseEvaluationString());
		System.err.println(sink.toSensibilityString());
		
		Assert.assertTrue(anomalies.size() >0);				
		
		Assert.assertEquals(input.size(), sink.getInputSize());
		Assert.assertEquals(anomalies.size(), sink.getAnomalySize());
		Assert.assertEquals(sink.values.size(), sink.getFindingSize());
		Assert.assertEquals(100 * anomalies.size()/input.size(), sink.getAnomalyPercentage(), 1);
		
		

		ArrayList correctPrediction = sink.getCorrectPrediction();

		for (Object prediction : correctPrediction) {
			LOG.info("*** CORRECT PREDICTION *** " + prediction);
		}			

		if (anomalies.size() > 0) {
			
			int correctPredictionSize = correctPrediction.size();
			int anomalySize = anomalies.size();
			
			double sensibility = sink.getSensibility();
			
			Assert.assertTrue(sensibility >= 0.1d);
			Assert.assertTrue(sensibility >= 0.2d);
			Assert.assertTrue(sensibility >= 0.3d);
			Assert.assertTrue(sensibility >= 0.4d);
			Assert.assertTrue(sensibility >= 0.5d);
			Assert.assertTrue(sensibility >= 0.6d);
			Assert.assertTrue(sensibility >= 0.7d);
			Assert.assertTrue(sensibility >= 0.8d);
			Assert.assertTrue(sensibility >= 0.9d);
			// verify your results
			Assert.assertEquals(anomalySize, correctPredictionSize);			
		}


	}
	
	@Test
	public void testNumberClusters() throws Exception {
		

		AppConfiguration ac = new AppConfiguration(new String[0]);
		Set<String> myIps = ac.getMyIps();

		StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
		senv.setParallelism(3);
		senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		final List<NMAJSONData> input = new ArrayList<NMAJSONData>();
		final List<NMAJSONData> anomalies = new ArrayList<NMAJSONData>();

		input.clear();
		anomalies.clear();
		int streamLimit = 2600;
		int inputElements = 1000;
		Stream<NMAJSONData> generateInfiniteStream = NMAJSONDataGenerator.generateInfiniteStream(myIps);
		generateInfiniteStream.limit(inputElements).forEach(data -> {
			if (data.getClass().equals(NMAJSONDataGenerator.NMAJSONDataAnomaly.class)) {
				anomalies.add(data);
			}
			input.add(data);
		});
		NMAJSONData anomaly = NMAJSONDataGenerator.generateAnomaly(myIps);
		anomalies.add(anomaly);
		input.add(anomaly);
		
		Assert.assertTrue(anomalies.size() >0);

		DataStream<NMAJSONData> source = senv.fromCollection(input);


		int lengthOption = streamLimit;
		Tuple2<Integer, Double> bestClusterSize = null;
		for (int numClustersOption = 1; numClustersOption < 6; numClustersOption++) {
			StreamKM defaultStreamKM = AnomalyReader.generateStreamKM(lengthOption, numClustersOption);
			Stream<NMAJSONData> stream = NMAJSONDataGenerator.generateInfiniteStreamNoAnomaly(myIps).limit(streamLimit);
			AnomalyReader.trainStreamKM(stream);
			SingleOutputStreamOperator<Tuple3<NMAJSONData, Double, Cluster>> datasource = AnomalyReader.processSource(senv,	source);

			CollectSink sink = new CollectSink(input, anomalies);
			CollectSink.clear();
			datasource.addSink(sink);
			senv.execute();
//			System.err.println(sink.toAnomalyString());
//			System.err.println(sink.toFalseEvaluationString());
//			System.err.println(sink.toSensibilityString());
			
			Assert.assertTrue(anomalies.size() >0);				
			
			Assert.assertEquals(input.size(), sink.getInputSize());
			Assert.assertEquals(anomalies.size(), sink.getAnomalySize());
			Assert.assertEquals(sink.values.size(), sink.getFindingSize());
			Assert.assertEquals(100 * anomalies.size()/input.size(), sink.getAnomalyPercentage(), 1);
			
			double sensibility = sink.getSensibility();
			LOG.info("ClusterSize: "+Tuple2.of(numClustersOption, sensibility));
			if (bestClusterSize == null || sensibility > bestClusterSize.f1) {
				bestClusterSize = Tuple2.of(numClustersOption, sensibility);
			}
			 
		}
		System.err.println("bestClusterSize: "+bestClusterSize);




	}	

	
	@Test
	public void testProcessSourceXLSX() throws Exception {
		CollectSink.clear();

		AppConfiguration ac = new AppConfiguration(new String[0]);
		Set<String> myIps = ac.getMyIps();

		StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
		senv.setParallelism(3);
		senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		final List<NMAJSONData> iterable = new ArrayList<NMAJSONData>();

		iterable.clear();
		int streamLimit = 2600;


		ArrayList<NMAJSONData> readCSV = TestUtilities.readCSV("metrics_23-03-ordered.csv");
		DataStream<NMAJSONData> source = senv.fromCollection(readCSV);


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
			LOG.warn("** ANOMALY ** :: " + t.f0.toString());
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
		
		private List<NMAJSONData> input = null;
		private List<NMAJSONData> anomalies = null;
		private static Collection<NMAJSONData> finding = Collections.synchronizedList(new ArrayList<NMAJSONData>());
		// must be static
		public static final List<Tuple3<NMAJSONData, Double, Cluster>> values = Collections.synchronizedList(new ArrayList<Tuple3<NMAJSONData, Double, Cluster>>());


		public static void clear() {
			values.clear();
			finding.clear();
		}
		public CollectSink(List<NMAJSONData> input, List<NMAJSONData> anomalies) {
			super();
			this.input = input;
			this.anomalies = anomalies;
		}
		



		public CollectSink() {
			super();
		}




		@Override
		public void invoke(Tuple3<NMAJSONData, Double, Cluster> value) throws Exception {
			finding.add(value.f0);
			values.add(value);
		}
		
		public String toAnomalyString() {
			return anomalies == null ? null : "%ANOMALY: "+getAnomalyPercentage() +" [ #INPUT: "+getInputSize() + " #ANOMALY: "+getAnomalySize()+" ]";
		}
		
		public String toSensibilityString() {
			return anomalies == null ? null : "SENSIBILITY: "+getSensibility() + " [ %FN: "+getFalseNegativePercentage()+", %FP: "+getFalsePositivePercentage()+", AN/IN: "+getAnomalyPercentage()+"% ]";
		}

		public String toFalseEvaluationString() {
			return anomalies == null ? null : "FALSE EVALUATION: "+getFalseEvaluation() + " [ FN: "+getFalseNegativeSize()+", FP: "+getFalsePositiveSize()+" ]";
		}
		
		public String toString() {
			return "#INPUT: "+getInputSize() + " #ANOMALY: "+getAnomalySize() + " #FINDING: "+getFindingSize()  ;
		}		




		public double getSensibility() {
			int falseEvaluation = getFalseEvaluation();
			if (falseEvaluation == 0)
				return 1d;
			double sensibility = (double) getAnomalySize() / (double)falseEvaluation;
			return sensibility;
		}




		private int getFalseEvaluation() {
			return getFalseNegativeSize() + getFalsePositiveSize();
		}




		public int getFalsePositivePercentage() {
			return getAnomalySize() == 0 ? 0 : Math.round(100 * getFalsePositiveSize() / getAnomalySize()) ;
		}




		public int getFalseNegativePercentage() {
			return getAnomalySize() == 0 ? 0 : Math.round(100 * getFalseNegativeSize() / getAnomalySize()) ;
		}




		public int getFalsePositiveSize() {
			return getAnomalySize() == 0 ? 0 : Math.max(0, getFindingSize() - getCorrectPredictionSize());
		}




		public int getFalseNegativeSize() {
			return getAnomalySize() == 0 ? 0 : Math.max(0,getAnomalySize() - getCorrectPredictionSize());
		}




		public int getAnomalyPercentage() {
			return getAnomalySize() == 0 ? 0 : 100 * getAnomalySize() / getInputSize();
		}


		public int getInputSize() {
			return input == null ? 0 : input.size();
		}


		public int getFindingSize() {
			return finding == null ? 0 : finding.size();
		}


		public int getAnomalySize() {
			return anomalies == null ? 0 : anomalies.size();
		}


		public int getCorrectPredictionSize() {
			if (anomalies == null || finding == null)
				return 0;
			ArrayList correctPrediction = getCorrectPrediction();
			int correctPredictionSize = correctPrediction.size();
			return correctPredictionSize;
		}




		public ArrayList getCorrectPrediction() {
			if (anomalies == null || finding == null)
				return new ArrayList();
			return new ArrayList(CollectionUtils.intersection(anomalies, finding));
		}
	}
}
