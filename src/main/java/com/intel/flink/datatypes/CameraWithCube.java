package com.intel.flink.datatypes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;

/**
 * Mutable DTO
 */
public class CameraWithCube implements Comparable<CameraWithCube> {
    private static final Logger logger = LoggerFactory.getLogger(CameraWithCube.class);

    public static class CameraKey {
        public long ts;
        public String cam;

        public CameraKey() {
        }

        public CameraKey(long ts, String cam) {
            this.ts = ts;
            this.cam = cam;
        }

        @Override
        public String toString() {
            return "CameraKey{" +
                    "ts=" + ts +
                    "; cam='" + cam + '\'' +
                    '}';
        }

        public long getTs() {
            return ts;
        }

        public void setTs(long ts) {
            this.ts = ts;
        }

        public String getCam() {
            return cam;
        }

        public void setCam(String cam) {
            this.cam = cam;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CameraKey cameraKey = (CameraKey) o;
            return ts == cameraKey.ts &&
                    Objects.equals(cam, cameraKey.cam);
        }

        @Override
        public int hashCode() {

            return Objects.hash(ts, cam);
        }

        public int compareTo(CameraKey other) {
            if (other == null) {
                return 1;
            } else {
                int i = Long.compare(ts, other.ts);
                if (i != 0) return i;

                i = cam != null ? cam.compareTo(other.cam): -1;
                return i;
            }
        }
    }

    public CameraKey cameraKey;
    public List<String> cubeLst;
    public String fileLocation;
    public boolean tileExists;
    public HashMap<String, Long> timingMap = new HashMap<>();

    public CameraWithCube() {
    }

    public CameraWithCube(CameraKey cameraKey, List<String> cubeLst, boolean tileExists) { //TODO: call base constructor
        this.cameraKey = cameraKey;
        this.cubeLst = cubeLst;
        this.tileExists = tileExists;
    }

    public CameraWithCube(CameraKey cameraKey, List<String> cubeLst, boolean tileExists, String fileLocation) {
        this.cameraKey = cameraKey;
        this.cubeLst = cubeLst;
        this.tileExists = tileExists;
        String outputFile1 = fileLocation + "/" + cameraKey.ts + "/" + cameraKey.cam + ".jpg";
        logger.debug("CameraWithCube fileLocation: {}", outputFile1);
        this.fileLocation = outputFile1;
    }

    public CameraWithCube(long ts, String cam, List<String> cubeLst, boolean tileExists) {
        this.cameraKey = new CameraKey(ts, cam);
        this.cubeLst = cubeLst;
        this.tileExists = tileExists;
    }

    public CameraWithCube(long ts, String cam, List<String> cubeLst, boolean tileExists, String fileLocation) {
        this.cameraKey = new CameraKey(ts, cam);
        this.cubeLst = cubeLst;
        this.tileExists = tileExists;
        String outputFile1 = fileLocation + "/" + ts + "/" + cam + ".jpg";
        logger.debug("CameraWithCube fileLocation: {}", outputFile1);
        this.fileLocation = outputFile1;
    }

    public CameraKey getCameraKey() {
        return cameraKey;
    }

    public void setCameraKey(CameraKey cameraKey) {
        this.cameraKey = cameraKey;
    }

    public List<String> getCubeLst() {
        return cubeLst;
    }

    public void setCubeLst(List<String> cubeLst) {
        this.cubeLst = cubeLst;
    }

    public boolean isTileExists() {
        return tileExists;
    }

    public void setTileExists(boolean tileExists) {
        this.tileExists = tileExists;
    }

    public String getFileLocation() {
        return fileLocation;
    }

    public void setFileLocation(String fileLocation) {
        this.fileLocation = fileLocation;
    }

    public HashMap<String, Long> getTimingMap() {
        return timingMap;
    }

    public void setTimingMap(HashMap<String, Long> timingMap) {
        this.timingMap = timingMap;
    }

    @Override
    public String toString() {
        return "CameraWithCube{" +
                "cameraKey=" + cameraKey +
                ": cubeLst=" + cubeLst +
                ": fileLocation='" + fileLocation + '\'' +
                ": tileExists=" + tileExists +
                ": timingMap=" + timingMap +
                '}';
    }

    /**
     * Convert input line to CameraWithCube
     * Timestamp,camera1
     * ts1,cam1
     *
     * @param line input line from file
     * @return converted CameraWithCube object
     */
    public static CameraWithCube fromString(String line) {
        String[] tokens = line.split(":");

        if (tokens.length < 2) {
            throw new RuntimeException("Invalid record: " + line);
        }
        CameraWithCube cameraWithCube = new CameraWithCube();
        try {
            //CameraWithCube{cameraKey=CameraKey{ts=1; cam='cam1'}: cubeLst=[cu1]: fileLocation='/tmp/1/cam1.jpg': tileExists=true: timingMap={} 
            String firstToken = tokens[0];
            //CameraWithCube{cameraKey=CameraKey{ts=1; cam='cam1'}
            String[] firstTokens = firstToken.split(";");
            String firstTokenTs = firstTokens[0];
            int firstTokenTsLastIdx = firstTokenTs.lastIndexOf("=");
            String tsStr = firstTokenTs.substring(firstTokenTsLastIdx + 1);
            long ts = Long.parseLong(tsStr);
            String camTokenStr = firstTokens[1];
            int camStrFirstIdx = camTokenStr.indexOf("='");
            int camStrLastIdx = camTokenStr.lastIndexOf("'}");
            String cam = camTokenStr.substring(camStrFirstIdx + 2, camStrLastIdx);
            cameraWithCube.cameraKey = new CameraKey(ts, cam);

            List<String> cubeLst = new ArrayList<>();
            String cubeTupleStr = tokens[1];
            int firstCubeIdx = cubeTupleStr.indexOf("[");
            int lastCubeIdx = cubeTupleStr.lastIndexOf("]");
            String cubeTupleStr1 = cubeTupleStr.substring(firstCubeIdx+1, lastCubeIdx);
            String[] cubeTupleTokens = cubeTupleStr1.split(",");
            cubeLst = Arrays.asList(cubeTupleTokens);
            cameraWithCube.cubeLst = cubeLst;

            String fileLocStr = tokens[2];
            int firstFileLocIdx = fileLocStr.indexOf("'");
            int lastFileLocIdx = fileLocStr.lastIndexOf("'");
            String fileLocation = fileLocStr.substring(firstFileLocIdx+1, lastFileLocIdx);
            cameraWithCube.fileLocation = fileLocation;

            String tileExistsStrToken = tokens[3];
            String[] tileExistsStrTokens = tileExistsStrToken.split("=");
            String tileExistsStr = tileExistsStrTokens[1];
            boolean tileExists = Boolean.valueOf(tileExistsStr);
            cameraWithCube.tileExists = tileExists;
            /*
            //InputMetadata{inputMetadataKey=InputMetadataKey{ts=1; cube='cu1'}: cameraLst=[CameraTuple{f0=cam1; f1=roi1}]: count=1: timingMap={}
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
             */

        } catch (NumberFormatException nfe) {
            throw new RuntimeException("Invalid record: " + line, nfe);
        }
        return cameraWithCube;
    }

    @Override
    public int compareTo(CameraWithCube other) {
        if (other == null) {
            return 1;
        } else {
            int i = cameraKey != null ? cameraKey.compareTo(other.getCameraKey()) : -1;
            return i;

            //TODO: do we need to compare cubeLst ?
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CameraWithCube that = (CameraWithCube) o;
        return tileExists == that.tileExists &&
                Objects.equals(cameraKey, that.cameraKey) &&
                Objects.equals(cubeLst, that.cubeLst) &&
                Objects.equals(fileLocation, that.fileLocation);
    }

    @Override
    public int hashCode() {

        return Objects.hash(cameraKey, cubeLst, fileLocation, tileExists);
    }
}
