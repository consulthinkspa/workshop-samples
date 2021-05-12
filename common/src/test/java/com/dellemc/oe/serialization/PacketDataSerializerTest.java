package com.dellemc.oe.serialization;

import it.consulthink.oe.model.NMAJSONData;
import it.consulthink.oe.model.NMAPacketData;
import junit.framework.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

public class PacketDataSerializerTest {

	@Test
	public void testSerialize() {
		NMAPacketData test = new NMAPacketData();
		JsonSerializer<NMAPacketData> jsonSerializer = new JsonSerializer<NMAPacketData>(NMAPacketData.class);
		ByteBuffer serialized = jsonSerializer.serialize(test);
		Assert.assertNotNull(serialized);
	}

	@Test
	public void testDeserialize() {
		NMAPacketData test = new NMAPacketData();
		JsonSerializer<NMAPacketData> jsonSerializer = new JsonSerializer<NMAPacketData>(NMAPacketData.class);
		ByteBuffer serialized = jsonSerializer.serialize(test);
		NMAPacketData result = jsonSerializer.deserialize(serialized);
		Assert.assertEquals(test, result);
		
	}

}
