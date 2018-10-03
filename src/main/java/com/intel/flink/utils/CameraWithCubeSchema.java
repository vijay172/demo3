package com.intel.flink.utils;

import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisSerializationSchema;

import com.intel.flink.datatypes.CameraWithCube;

import java.nio.ByteBuffer;

/**
 * Implements a SerializationSchema and DeserializationSchema for CameraWithCube for Kinesis data sources and sinks.
 */
public class CameraWithCubeSchema  implements KinesisSerializationSchema<CameraWithCube> {
    @Override
    public ByteBuffer serialize(CameraWithCube cameraWithCube) {
        return ByteBuffer.wrap(cameraWithCube.toString().getBytes());
    }

    @Override
    public String getTargetStream(CameraWithCube cameraWithCube) {
        return null;
    }
    /*@Override
    public CameraWithCube deserialize(byte[] message) throws IOException {
        return CameraWithCube.fromString(new String(message));
    }

    @Override
    public boolean isEndOfStream(CameraWithCube nextElement) {
        return false;
    }

    @Override
    public byte[] serialize(CameraWithCube cameraWithCube) {
        return cameraWithCube.toString().getBytes();
    }

    @Override
    public TypeInformation<CameraWithCube> getProducedType() {
        return TypeExtractor.getForClass(CameraWithCube.class);
    }*/
}
