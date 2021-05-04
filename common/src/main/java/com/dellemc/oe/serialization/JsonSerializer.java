/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package com.dellemc.oe.serialization;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.pravega.client.stream.Serializer;
import com.google.common.reflect.TypeToken;
import java.lang.reflect.Type;

public class JsonSerializer<T> implements Serializer<T> {
	  
    private final ObjectMapper objectMapper;
    private Class<T> valuetype;

    @SuppressWarnings("unchecked")
	public JsonSerializer(Class<T> valueType) {
    	this.valuetype = valueType;
        this.objectMapper = new ObjectMapper();
    }

    public JsonSerializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public ByteBuffer serialize(T value) {
        try {
            byte[] result = objectMapper.writeValueAsBytes(value);
            return ByteBuffer.wrap(result);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public T deserialize(ByteBuffer serializedValue) {
        ByteArrayInputStream bin = new ByteArrayInputStream(serializedValue.array(),
                serializedValue.position(),
                serializedValue.remaining());
        try {
                    T result = objectMapper.readValue(bin, this.valuetype);
					return result;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
    
    public byte[] serializeToByteArray(T o) {
        try {
            byte[] result = this.objectMapper.writeValueAsBytes(o);
            return result;
        } catch (Exception e) {
            return null;
        }
    }

}