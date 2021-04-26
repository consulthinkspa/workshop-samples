/**
 * 
 */
package it.consulthink.oe.ingest;

import static org.junit.Assert.fail;

import java.util.Collections;
import java.util.Hashtable;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.consulthink.oe.model.NMAJSONData;
import junit.framework.Assert;
import scala.collection.mutable.HashTable;

/**
 * @author svetr
 *
 */
public class NMAJSONDataGeneratorTest {

	private static final Logger LOG = LoggerFactory.getLogger(NMAJSONDataGeneratorTest.class);
	/**
	 * Test method for {@link it.consulthink.oe.ingest.NMAJSONDataGenerator#generateInfiniteStream(java.util.List)}.
	 */
	@Test
	public void testGenerateInfiniteStream() {
		Stream<NMAJSONData> generateInfiniteStream = NMAJSONDataGenerator.generateInfiniteStream(null);
		generateInfiniteStream
			.limit(5000)
			.forEach(System.out::println);
	}
	
	@Test
	public void testGenerateIps() {
		Stream<NMAJSONData> generateInfiniteStream = NMAJSONDataGenerator.generateInfiniteStream(null);
		
		Hashtable<String, Long> ips = new Hashtable<String, Long>();

		generateInfiniteStream
			.limit(5000)
			.forEach(new Consumer<NMAJSONData>() {

				@Override
				public void accept(NMAJSONData t) {
					if (ips.containsKey(t.getSrc_ip())) {
						ips.put(t.getSrc_ip(), ips.get(t.getSrc_ip())+1l);
					}else {
						ips.put(t.getSrc_ip(), 1l);
					}
					if (ips.containsKey(t.getDst_ip())) {
						ips.put(t.getDst_ip(), ips.get(t.getDst_ip())+1l);	
					}else {
						ips.put(t.getDst_ip(), 1l);
					}
					
				}
				
			});
		
		LOG.info(ips.toString());
		
		}	

	/**
	 * Test method for {@link it.consulthink.oe.ingest.NMAJSONDataGenerator#generateAnomaly(java.util.List)}.
	 */
	@Test
	public void testGenerateAnomaly() {
		fail("Not yet implemented");
	}

	/**
	 * Test method for {@link it.consulthink.oe.ingest.NMAJSONDataGenerator#generateStandard(java.util.List)}.
	 */
	@Test
	public void testGenerateStandard() {
		fail("Not yet implemented");
	}

}
