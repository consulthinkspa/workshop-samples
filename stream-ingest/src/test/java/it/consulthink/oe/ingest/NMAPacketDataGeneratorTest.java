/**
 * 
 */
package it.consulthink.oe.ingest;

import com.dellemc.oe.serialization.JsonDeserializationSchema;
import com.dellemc.oe.serialization.JsonSerializer;
import it.consulthink.oe.model.NMAJSONData;
import it.consulthink.oe.model.NMAPacketData;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Hashtable;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.junit.Assert.fail;

/**
 * @author selia
 *
 */
public class NMAPacketDataGeneratorTest {

	private static final Logger LOG = LoggerFactory.getLogger(NMAPacketDataGeneratorTest.class);
	/**
	 * Test method for {@link NMAPacketDataGenerator#generateInfiniteStream(java.util.Collection, java.util.Collection)}.
	 */
	@Test
	public void testGenerateInfiniteStream() {
		Stream<NMAPacketData> generateInfiniteStream = NMAPacketDataGenerator.generateInfiniteStream(null,null);
		generateInfiniteStream
			.limit(5000)
			.forEach(System.out::println);
	}
	
	@Test
	public void testGenerateDate() {
		
		JsonSerializer<NMAPacketData> serializer = new JsonSerializer<NMAPacketData>(NMAPacketData.class);
		JsonDeserializationSchema deserializer = new JsonDeserializationSchema(NMAPacketData.class);
		//deserializer.deserialize(message)

				
		Stream<NMAPacketData> generateInfiniteStream = NMAPacketDataGenerator.generateInfiniteStream(null, null);
		generateInfiniteStream
			.limit(1)
			.forEach(data -> {
				try {
					System.out.println(deserializer.deserialize(serializer.serializeToByteArray(data)));
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}}
			);
		
		
	}
	
	@Test
	public void testGenerateIps() {
		Stream<NMAPacketData> generateInfiniteStream = NMAPacketDataGenerator.generateInfiniteStream(null,null);
		
		Hashtable<String, Long> ips = new Hashtable<String, Long>();

		generateInfiniteStream
			.limit(5000)
			.forEach(new Consumer<NMAPacketData>() {

				@Override
				public void accept(NMAPacketData t) {
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
	 * Test method for {@link NMAPacketDataGenerator#generateAnomaly(java.util.Collection, java.util.Collection )}.
	 */
	@Test
	public void testGenerateAnomaly() {
		fail("Not yet implemented");
	}

	/**
	 * Test method for {@link NMAPacketDataGenerator#generateStandard(java.util.Collection, java.util.Collection)}.
	 */
	@Test
	public void testGenerateStandard() {
		fail("Not yet implemented");
	}

}
