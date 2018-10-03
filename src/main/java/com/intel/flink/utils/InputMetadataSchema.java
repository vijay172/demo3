package com.intel.flink.utils;

import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisSerializationSchema;

import com.intel.flink.datatypes.InputMetadata;

import java.nio.ByteBuffer;

/**
 * Implements a SerializationSchema for InputMetadata for Kinesis data sinks to send data as byte array to Kinesis.
 */
public class InputMetadataSchema implements KinesisSerializationSchema<InputMetadata> {

    private static final long serialVersionUID = 6298573834520052886L;

    @Override
    public ByteBuffer serialize(InputMetadata inputMetadata) {
        return ByteBuffer.wrap(inputMetadata.toString().getBytes());//TODO: check this conversion
    }

    @Override
    public String getTargetStream(InputMetadata element) {
        return null;//TODO:Name of default Kinesis stream not needed as long as you set setDefaultStream on KinesisProducer
    }

     /*@Override
    public InputMetadata deserialize(byte[] message) throws IOException {
        return InputMetadata.fromString(new String(message));
    }

    @Override
    public boolean isEndOfStream(InputMetadata nextElement) {
        return false;
    }

    @Override
    public byte[] serialize(InputMetadata inputMetadata) {
        return inputMetadata.toString().getBytes();
    }

    @Override
    public TypeInformation<InputMetadata> getProducedType() {
        return TypeExtractor.getForClass(InputMetadata.class);
    }*/
}
