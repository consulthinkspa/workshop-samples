package it.consulthink.oe.flink.packetcount;

import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dellemc.oe.util.AppConfiguration;

public class TotalTrafficReaderIntegrationTest {
	private static final Logger LOG = LoggerFactory.getLogger(TotalTrafficReaderIntegrationTest.class);
	
	@ClassRule
	public static MiniClusterWithClientResource flinkCluster = new MiniClusterWithClientResource(
			new MiniClusterResourceConfiguration.Builder().setNumberSlotsPerTaskManager(6).setNumberTaskManagers(2)
					.build());	
	
	@Test
	public void testRun() {
		
        LOG.info("Starting TotalTrafficReaderIntegrationTest...");
        String scope = "integration-test";
		String myIp = "213.61.202.114,213.61.202.115,213.61.202.116,213.61.202.117,213.61.202.118,213.61.202.119,213.61.202.120,213.61.202.121,213.61.202.122,213.61.202.123,213.61.202.124,213.61.202.125,213.61.202.126";
		String inputStreamName = "nma-input";
		String outputStreamName = "total-traffic";
		String controllerUri = "tcp://host.docker.internal:9090";
		String[] args = {
          	  "--myIps", myIp,
      		  "--scope", scope,
      		  "--input-stream", inputStreamName,
      		  "--output-stream", outputStreamName,
      		  "--controller", controllerUri
          };
  		AppConfiguration appConfiguration = new AppConfiguration( args );        

        TotalTrafficReader reader = new TotalTrafficReader(appConfiguration);
        reader.run();
	}

}
