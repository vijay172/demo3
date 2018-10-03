package com.intel.flink.sources;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import com.intel.flink.datatypes.CameraWithCube;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class ParallelCameraSource extends RichParallelSourceFunction<CameraWithCube> {

    private static final Logger logger = LoggerFactory.getLogger(ParallelCameraSource.class);

    private final long maxSeqCnt;
    //An event generated every servingSpeedMs
    private final int servingSpeedMs;
    //Initial kickoff time passed in
    private final long currentTimeMs;
    //Number of cameras being processed for every seq
    private final int nbrCameras;
    //file location to be used with CameraCube
    private final String fileLocation;


    private static final int NBR_OF_CAMERAS = 38;
    private static final long MAX_SEQ_CNT = 36000; //20 mins * 60 secs/min * 30 frames/sec
    private static final int SERVING_SPEED_FREQ_MILLIS = 33; //event generated in msec every 1000 / (30 frames per sec)
    private static final String CAM = "cam";
    
    private Boolean running = true;

    public ParallelCameraSource() {
        this(MAX_SEQ_CNT, SERVING_SPEED_FREQ_MILLIS, System.currentTimeMillis(), NBR_OF_CAMERAS, "");
    }

    public ParallelCameraSource(long maxSeqCnt, int servingSpeedMs) {
        this(maxSeqCnt, servingSpeedMs, System.currentTimeMillis(), NBR_OF_CAMERAS, "");
    }

    public ParallelCameraSource(long currentTimeMs) {
        this(MAX_SEQ_CNT, SERVING_SPEED_FREQ_MILLIS, currentTimeMs, NBR_OF_CAMERAS, "");
    }

    public ParallelCameraSource(long maxSeqCnt, int servingSpeedMs, long currentTimeMs, int nbrCameras, String fileLocation) {
        if (maxSeqCnt < 0) {
            throw new IllegalArgumentException("Max sequence count must be positive");
        } else if (servingSpeedMs < 0) {
            throw new IllegalArgumentException("Serving speed in millisec must be positive");
        } else if (currentTimeMs < 0) {
            throw new IllegalArgumentException("Current time in millisec must be positive");
        } else if (nbrCameras < 0) {
            throw new IllegalArgumentException("Number of cameras must be positive");
        }
        this.maxSeqCnt = maxSeqCnt;
        this.currentTimeMs = currentTimeMs;
        this.servingSpeedMs = servingSpeedMs;
        this.nbrCameras = nbrCameras;
        this.fileLocation = fileLocation;
    }

    @Override
    public void run(SourceContext<CameraWithCube> sourceContext) throws Exception {

        // look up index of this parallel task
        int camIdx = this.getRuntimeContext().getIndexOfThisSubtask();
        logger.debug("For each Parallel run - camIdx:{}", camIdx);
        final Object lock = sourceContext.getCheckpointLock();
        long prevKickOffTime = currentTimeMs; //currentTimeMs passed in initially as kickoff time
        final CameraWithCube camWithCube = new CameraWithCube();
        final CameraWithCube.CameraKey cameraKey = new CameraWithCube.CameraKey(); //mutable
        long seqCnt = 0;
        while (running) {
            for (; seqCnt < maxSeqCnt; seqCnt++) {
                cameraKey.ts = seqCnt;
                //for (int camCnt = 1; camCnt <= nbrCameras; camCnt++) {
                    //cameraKey.cam = CAM + camCnt + UUID.randomUUID().toString();
                    cameraKey.cam = CAM + camIdx;
                    camWithCube.setCameraKey(cameraKey);
                    String outputFile1 = fileLocation + "/" + cameraKey.ts + "/" + cameraKey.cam + ".jpg";
                    logger.debug("Source fileLocation: {}", outputFile1);
                    camWithCube.setFileLocation(outputFile1);

                    synchronized (lock) {
                        //insert createTS
                        camWithCube.getTimingMap().put("Generated", System.currentTimeMillis());
                        //camWithCube.setCreateTS(createTS)
                        sourceContext.collect(camWithCube);
                    }
                    logger.debug("CameraSource - Emitting each Camera event {}", camWithCube);
                //}
                //next seqCnt after servingSpeedMs
                long currWorkingTime = System.currentTimeMillis();//1240 ; 1267
                long nextKickOffTime = prevKickOffTime + (servingSpeedMs * (seqCnt+1) + 20000);// 1234 + 33 *1= 1267; 1267 + 33= 1300
                long diffGreaterThanZero = nextKickOffTime - currWorkingTime;//1267 - 1240 = 27; 1300 - 1267 = 33
                if (diffGreaterThanZero > 0) {
                    Thread.sleep(diffGreaterThanZero);
                }
            }
            if (seqCnt >= maxSeqCnt) {
                logger.debug("Reached end of Parallel Camera event generation");
                break;
            }
        }
        //sourceContext.close();
        logger.debug("Stopping Parallel Camera events generator");
    }

    @Override
    public void cancel() {
        running = false;
    }
}
