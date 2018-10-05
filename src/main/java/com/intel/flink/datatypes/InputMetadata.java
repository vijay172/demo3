package com.intel.flink.datatypes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;

/**
 * Mutable DTO
 */
public class InputMetadata implements Comparable<InputMetadata> {
    private static final Logger logger = LoggerFactory.getLogger(InputMetadata.class);
    public static final String SEPARATOR = "@";
    public InputMetadata() {
    }

    public InputMetadata(InputMetadataKey inputMetadataKey, List<CameraTuple> cameraLst) {
        this.inputMetadataKey = inputMetadataKey;
        this.cameraLst = cameraLst;
        this.count = getCount();
    }

    public InputMetadata(long ts, String cube, List<CameraTuple> cameraLst) {
        this.inputMetadataKey = new InputMetadataKey(ts, cube);
        this.cameraLst = cameraLst;
        this.count = getCount();
    }

    /**
     * Convert input line to InputMetadata
     * ts,cube,2,cam1,cam2
     *
     * @param line input line from file
     * @return InputMetadata object
     */
    public static InputMetadata fromString(String line) {
        logger.debug("fromString line:{}", line);
        String[] tokens = line.split(SEPARATOR);

        if (tokens.length < 4) {
            throw new RuntimeException("Invalid record: " + line);
        }
        InputMetadata inputMetadata = new InputMetadata();
        //InputMetadata{inputMetadataKey=InputMetadataKey{ts=1; cube='cu1'}: cameraLst=[CameraTuple{f0=cam1; f1=roi1}]: count=1: timingMap={Generated=10, StartTime=11}
        try {
            String firstToken = tokens[0];
            //InputMetadata{inputMetadataKey=InputMetadataKey{ts=1; cube='cu1'}
            String[] firstTokens = firstToken.split(";");
            String firstTokenTs = firstTokens[0];
            int firstTokenTsLastIdx = firstTokenTs.lastIndexOf("=");
            String tsStr = firstTokenTs.substring(firstTokenTsLastIdx + 1);
            long ts = Long.parseLong(tsStr);
            String cubeTokenStr = firstTokens[1];
            int cubeStrFirstIdx = cubeTokenStr.indexOf("='");
            int cubeStrLastIdx = cubeTokenStr.lastIndexOf("'}");
            String cube = cubeTokenStr.substring(cubeStrFirstIdx + 2, cubeStrLastIdx);
            inputMetadata.inputMetadataKey = new InputMetadataKey(ts, cube);

            List<CameraTuple> cameraLst = new ArrayList<>();
            String camTupleStr = tokens[1];
            int firstCamIdx = camTupleStr.indexOf("[");
            int lastCamIdx = camTupleStr.lastIndexOf("]");
            String camTupleStr1 = camTupleStr.substring(firstCamIdx+1, lastCamIdx);
            String[] camTupleTokens = camTupleStr1.split(",");
            for (String camTupleToken : camTupleTokens) {
                CameraTuple cameraTuple = CameraTuple.fromString(camTupleToken);
                cameraLst.add(cameraTuple);
            }
            inputMetadata.cameraLst = cameraLst;
            String countStr = tokens[2];
            inputMetadata.count = Integer.parseInt(countStr.substring(7));
            //timingMap={Generated=10, StartTime=11}
            String timingMapStr = tokens[3];
            int firstTimingIdx =  timingMapStr.indexOf("{");
            int lastTimingIdx = timingMapStr.lastIndexOf("}");
            String timingMapStrStripped = timingMapStr.substring(firstTimingIdx, lastTimingIdx);
            String[] timingTokens = timingMapStrStripped.split(",");
            HashMap<String, Long> timingMapRetrieved = new HashMap<>();
            for (String timingToken : timingTokens) {
                String[] timingEachTokenArr = timingToken.split("=");
                Long timingMapValue = Long.parseLong(timingEachTokenArr[1]);
                timingMapRetrieved.put(timingEachTokenArr[0], timingMapValue);
            }
            inputMetadata.setTimingMap(timingMapRetrieved);

        } catch (NumberFormatException nfe ) {
            throw new RuntimeException("Invalid record: " + line, nfe);
        }
        return inputMetadata;
    }

    public static class InputMetadataKey {
        public long ts;
        public String cube;

        public InputMetadataKey() {
        }

        public InputMetadataKey(long ts, String cube) {
            this.ts = ts;
            this.cube = cube;
        }

        public long getTs() {
            return ts;
        }

        public void setTs(long ts) {
            this.ts = ts;
        }

        public String getCube() {
            return cube;
        }

        public void setCube(String cube) {
            this.cube = cube;
        }

        @Override
        public String toString() {
            return "InputMetadataKey{" +
                    "ts=" + ts +
                    "; cube='" + cube + '\'' +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            InputMetadataKey that = (InputMetadataKey) o;
            return ts == that.ts &&
                    Objects.equals(cube, that.cube);
        }

        @Override
        public int hashCode() {
            return Objects.hash(ts, cube);
        }

        public int compareTo(InputMetadataKey other) {
            if (other == null) {
                return 1;
            } else {
                int i = Long.compare(ts, other.ts);
                if (i != 0) {
                    return i;
                }

                i = cube != null ? cube.compareTo(other.cube) : -1;
                return i;
            }
        }
    }

    public InputMetadataKey inputMetadataKey;
    public List<CameraTuple> cameraLst;
    public long count;
    public HashMap<String, Long> timingMap = new HashMap<>();

    public InputMetadataKey getInputMetadataKey() {
        return inputMetadataKey;
    }

    public void setInputMetadataKey(InputMetadataKey inputMetadataKey) {
        this.inputMetadataKey = inputMetadataKey;
    }

    public List<CameraTuple> getCameraLst() {
        return cameraLst;
    }

    public void setCameraLst(List<CameraTuple> cameraLst) {
        this.cameraLst = cameraLst;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public HashMap<String, Long> getTimingMap() {
        return timingMap;
    }

    public void setTimingMap(HashMap<String, Long> timingMap) {
        this.timingMap = timingMap;
    }

    public long getCount() {
        if (cameraLst != null && cameraLst.size() > 0) {
            count = cameraLst.size();
        } else {
            count = 0;
        }
        return count;
    }

    @Override
    public int compareTo(InputMetadata other) {
        if (other == null) {
            return 1;
        } else {
            return inputMetadataKey != null ? inputMetadataKey.compareTo(other.inputMetadataKey) : -1;
        }
    }

    @Override
    public String toString() {
        return "InputMetadata{" +
                "inputMetadataKey=" + inputMetadataKey + SEPARATOR +
                " cameraLst=" + cameraLst  + SEPARATOR +
                " count=" + count  + SEPARATOR +
                " timingMap=" + timingMap  + SEPARATOR +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        InputMetadata that = (InputMetadata) o;
        return Objects.equals(inputMetadataKey, that.inputMetadataKey) &&
                Objects.equals(cameraLst, that.cameraLst);
    }

    @Override
    public int hashCode() {

        return Objects.hash(inputMetadataKey, cameraLst);
    }
}
