package com.intel.flink.utils;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchema;
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisSerializationSchema;

import com.intel.flink.datatypes.CameraTuple;
import com.intel.flink.datatypes.CameraWithCube;
import com.intel.flink.datatypes.InputMetadata;
import com.intel.flink.state.MapTwoStreamsProducer1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Implements a SerializationSchema for InputMetadata for Kinesis data sinks to send data as byte array to Kinesis.
 */
public class Tuple2KinesisSchema implements KinesisSerializationSchema<Tuple2<InputMetadata, CameraWithCube>>,
        KinesisDeserializationSchema<Tuple2<InputMetadata, CameraWithCube>> {
    private static final Logger logger = LoggerFactory.getLogger(Tuple2KinesisSchema.class);
    private static final long serialVersionUID = 6307373834529952001L;

    @Override
    public ByteBuffer serialize(Tuple2<InputMetadata, CameraWithCube> inputMetadataCameraWithCubeTuple2) {
        logger.debug("Entered serialize");
        byte[] inputMetadataByteArr = inputMetadataCameraWithCubeTuple2.f0.toString().getBytes();
        ByteBuffer inputMetadataByteBuf = ByteBuffer.wrap(inputMetadataByteArr);
        byte[] cameraWithCubeArr = inputMetadataCameraWithCubeTuple2.f1.toString().getBytes();
        ByteBuffer cameraWithCubeByteBuf = ByteBuffer.wrap(cameraWithCubeArr);
        ByteBuffer combinedByteBuffer = ByteBuffer.allocate(8 + inputMetadataByteBuf.limit() + cameraWithCubeByteBuf.limit());// 4 each for length
        combinedByteBuffer.putInt(inputMetadataByteBuf.limit());
        combinedByteBuffer.put(inputMetadataByteBuf);
        combinedByteBuffer.putInt(cameraWithCubeByteBuf.limit());
        combinedByteBuffer.put(cameraWithCubeByteBuf);
        combinedByteBuffer.flip();
        return combinedByteBuffer;
    }

    @Override
    public String getTargetStream(Tuple2<InputMetadata, CameraWithCube> inputMetadataCameraWithCubeTuple2) {
        return null;
    }

    @Override
    public Tuple2<InputMetadata, CameraWithCube> deserialize(byte[] inputMetadataCameraWithCubeTuple2Bytes,
                                                             String partitionKey, String seqNum,
                                                             long approxArrivalTimestamp, String stream, String shardId)
            throws IOException {
        logger.debug("Entered deserialize with bytes length:"+inputMetadataCameraWithCubeTuple2Bytes.length);
        final ByteBuffer inputMetadataCameraWithCubeTuple2BytesBuf = ByteBuffer.wrap(inputMetadataCameraWithCubeTuple2Bytes);
        final ByteBuffer inputMetadataCameraWithCubeTuple2BytesBufDup = inputMetadataCameraWithCubeTuple2BytesBuf.duplicate();
        inputMetadataCameraWithCubeTuple2BytesBufDup.rewind();
        int inputMetadataLen = inputMetadataCameraWithCubeTuple2BytesBufDup.getInt(0);
        final byte[] inputMetaByteArr = new byte[inputMetadataLen];
        inputMetadataCameraWithCubeTuple2BytesBufDup.position(4);
        inputMetadataCameraWithCubeTuple2BytesBufDup.get(inputMetaByteArr, 0, inputMetadataLen - 1);
        InputMetadata inputMetadata = InputMetadata.fromString(new String(inputMetaByteArr));
        int cameraOffset = 4 + inputMetadataLen;//idx starts at 0
        int cameraLen = inputMetadataCameraWithCubeTuple2BytesBufDup.getInt(cameraOffset);
        final byte[] cameraByteArr = new byte[cameraLen];
        inputMetadataCameraWithCubeTuple2BytesBufDup.position(4 + cameraOffset);
        inputMetadataCameraWithCubeTuple2BytesBufDup.get(cameraByteArr, 0, cameraLen - 1);
        CameraWithCube camera = CameraWithCube.fromString(new String(cameraByteArr));

        return new Tuple2<>(inputMetadata, camera);
    }

    @Override
    public TypeInformation<Tuple2<InputMetadata, CameraWithCube>> getProducedType() {
        return TypeInformation.of(new TypeHint<Tuple2<InputMetadata, CameraWithCube>>(){});
        //return TypeExtractor.getForClass(Tuple2.class); //cannot use for Parameterized type
    }

    /*@Override
    public ByteBuffer serialize(InputMetadata inputMetadata) {
        return ByteBuffer.wrap(inputMetadata.toString().getBytes());//TODO: check this conversion
    }

    @Override
    public String getTargetStream(InputMetadata element) {
        return null;//TODO:Name of default Kinesis stream not needed as long as you set setDefaultStream on KinesisProducer
    }
*/
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
    public static void main(String[] args) {
        Tuple2KinesisSchema schema = new Tuple2KinesisSchema();
        //serialize
        List<CameraTuple> cameraLst = new ArrayList<CameraTuple>();
        CameraTuple ct = new CameraTuple("cam1", "roi1","/tmp");
        cameraLst.add(ct);
        HashMap<String, Long> timingMap = new HashMap<>();
        timingMap.put("Generated", 10L);
        timingMap.put("StartTime", 11L);

        InputMetadata inputMeta = new InputMetadata(1, "cu1", cameraLst);
        inputMeta.setTimingMap(timingMap);
        List<String> cubeLst = new ArrayList<String>();
        cubeLst.add("cu1");
        String fileLocation = "/tmp";
        CameraWithCube camera = new CameraWithCube(1, "cam1", cubeLst, true, fileLocation);
        HashMap<String, Long> timingMap1 = new HashMap<>();
        timingMap1.put("Generated", 10L);
        timingMap1.put("StartTime", 11L);
        camera.setTimingMap(timingMap1);
        Tuple2<InputMetadata, CameraWithCube> tuple2 = new Tuple2(inputMeta, camera);

        ByteBuffer byteBuf = schema.serialize(tuple2);


        //deserialize
        byte[] deserArr = new byte[byteBuf.remaining()];
        byteBuf.get(deserArr);
        try {
            final Tuple2<InputMetadata, CameraWithCube> deserializeTuple = schema.deserialize(deserArr, "", "", 0l, "", "");
            System.out.println("inputmetadata:" + deserializeTuple.f0);
            System.out.println("###camera:" + deserializeTuple.f1);
        } catch (IOException e) {
            System.out.println("error:" + e.getMessage());
        }


    }
}
