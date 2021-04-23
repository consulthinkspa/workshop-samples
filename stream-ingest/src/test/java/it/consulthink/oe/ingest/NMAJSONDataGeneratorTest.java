/**
 * 
 */
package it.consulthink.oe.ingest;

import static org.junit.Assert.*;

import java.util.stream.Stream;

import org.junit.Test;

import it.consulthink.oe.model.NMAJSONData;

/**
 * @author svetr
 *
 */
public class NMAJSONDataGeneratorTest {

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
