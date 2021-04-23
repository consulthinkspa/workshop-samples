package com.dellemc.oe.serialization;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;

import org.junit.Test;

import it.consulthink.oe.model.NMAJSONData;
import junit.framework.Assert;

public class JsonSerializerTest {

	@Test
	public void testSerialize() {
		NMAJSONData test = new NMAJSONData();
		JsonSerializer<NMAJSONData> jsonSerializer = new JsonSerializer<NMAJSONData>(NMAJSONData.class);
		ByteBuffer serialized = jsonSerializer.serialize(test);
		Assert.assertNotNull(serialized);
	}

	@Test
	public void testDeserialize() {
		NMAJSONData test = new NMAJSONData();
		JsonSerializer<NMAJSONData> jsonSerializer = new JsonSerializer<NMAJSONData>(NMAJSONData.class);
		ByteBuffer serialized = jsonSerializer.serialize(test);
		NMAJSONData result = jsonSerializer.deserialize(serialized);
		Assert.assertEquals(test, result);
		
	}

}
