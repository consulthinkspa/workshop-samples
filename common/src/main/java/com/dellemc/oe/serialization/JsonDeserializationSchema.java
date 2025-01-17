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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class JsonDeserializationSchema<T> implements DeserializationSchema<T> {
    private Class<T> valueType;

    public JsonDeserializationSchema(Class<T> valueType) {
        this.valueType = valueType;
    }

    @Override
    public T deserialize(byte[] message) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        T readValue = objectMapper.readValue(message, valueType);
        try {
			System.out.println("JsonDeserializationSchema << " + new String(message, "UTF-8"));
		} catch (Throwable e) {
			//NOP
		}
        System.out.println("JsonDeserializationSchema >> " + readValue);
		return readValue;
    }

    @Override
    public boolean isEndOfStream(T nextElement) {
        return false;
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return TypeInformation.of(valueType);
    }
}
