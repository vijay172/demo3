package com.intel.flink.state;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisProducer;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;
import org.apache.flink.util.Collector;

import com.codahale.metrics.SlidingTimeWindowReservoir;
import com.intel.flink.datatypes.CameraTuple;
import com.intel.flink.datatypes.CameraWithCube;
import com.intel.flink.datatypes.InputMetadata;
import com.intel.flink.jni.NativeLoader;
import com.intel.flink.sources.CheckpointedCameraWithCubeSource;
import com.intel.flink.sources.CheckpointedInputMetadataSource;
import com.intel.flink.utils.CameraAssigner;
import com.intel.flink.utils.InputMetadataAssigner;
import com.intel.flink.utils.Tuple2KinesisSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.supercsv.cellprocessor.constraint.NotNull;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvMapWriter;
import org.supercsv.io.ICsvMapWriter;
import org.supercsv.prefs.CsvPreference;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class MapTwoStreamsProducer1 {
    private static final Logger logger = LoggerFactory.getLogger(MapTwoStreamsProducer1.class);
    public static final String KINESIS_TOPIC = "app1-stream";
    public static final String REGION = "us-west-1";
    private static final String ALL = "all";
    private static final String COPY = "copy";
    private static final String READ = "read";
    private static final String ERROR = "ERROR";
    private static final String OK = "OK:";

    public static void main(String[] args) throws Exception {
        //logger.info("args:", args);
        ParameterTool params = ParameterTool.fromArgs(args);
        final int parallelCamTasks = params.getInt("parallelCam", 38);
        final int parallelCubeTasks = params.getInt("parallelCube", 1000);
        final int servingSpeedMs = params.getInt("servingSpeedMs", 33);
        final int nbrCameras = params.getInt("nbrCameras", 38);
        final long maxSeqCnt = params.getLong("maxSeqCnt", 36000);
        final int nbrCubes = params.getInt("nbrCubes", 1000);
        final int nbrCameraTuples = params.getInt("nbrCameraTuples", 19);
        final String inputFile = params.get("inputFile");
        final String outputFile = params.get("outputFile");
        final String options = params.get("options");
        final String outputPath = params.get("outputPath");
        final long timeout = params.getLong("timeout", 10000L);
        final long shutdownWaitTS = params.getLong("shutdownWaitTS", 20000);
        final int nThreads = params.getInt("nThreads", 30);
        final int nCapacity = params.getInt("nCapacity", 100);
        final Boolean local = params.getBoolean("local", false);
        final int bufferTimeout = params.getInt("bufferTimeout", 10);
        final String host = params.get("host", "localhost");
        final int port = params.getInt("port", 50051);
        final long deadlineDuration = params.getInt("deadlineDuration", 5000);
        final int sourceDelay = params.getInt("sourceDelay", 15000);
        final String action = params.get("action", ALL);//values are: copy,read,all
        final Boolean checkpoint = params.getBoolean("checkpoint", true);
        final String kinesisTopic = params.get("kinesisTopic", KINESIS_TOPIC);
        final String region = params.get("region", REGION);
        final String accessKey = params.get("accessKey");
        final String secretKey = params.get("secretKey");
        final String jobName = params.get("jobName");

        logger.info("parallelCam:{}, parallelCube:{}, servingSpeedMs:{}, nbrCameras:{}, maxSeqCnt:{}, nbrCubes:{}, " +
                        "nbrCameraTuples:{}, inputFile:{}, outputFile:{}, options:{}, outputPath:{}" +
                        " kinesisTopic:{}, accessKey:{}, secretKey:{}" +
                        " bufferTimeout:{}, host:{}, port:{}, deadlineDuration:{}, sourceDelay:{}, action:{}, jobName:{}", parallelCamTasks, parallelCubeTasks,
                servingSpeedMs, nbrCameras, maxSeqCnt, nbrCubes, nbrCameraTuples, inputFile, outputFile, options, outputPath,
                kinesisTopic, accessKey, secretKey, bufferTimeout, host, port, deadlineDuration, sourceDelay, action, jobName);
        //zipped files
        final StreamExecutionEnvironment env;
        if (local) {
            Configuration configuration = new Configuration();
            //configuration.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, 2);
            env = StreamExecutionEnvironment.createLocalEnvironment(1, configuration);
            env.setStateBackend(new FsStateBackend("file:///tmp/checkpoints", true));//TODO: parm
        } else {
            env = StreamExecutionEnvironment.getExecutionEnvironment();
        }
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);//EventTime not needed here as we are not dealing with EventTime timestamps
        //TODO: restart and checkpointing strategies
        if (checkpoint) {
            env.enableCheckpointing(3500L, CheckpointingMode.AT_LEAST_ONCE);
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, Time.of(20, TimeUnit.SECONDS)));//changed from 60 to 3 for restartAttempts
            env.getCheckpointConfig().setMinPauseBetweenCheckpoints(100);
            //Flink offers optional compression (default: off) for all checkpoints and savepoints.
            env.getConfig().setUseSnapshotCompression(true);
        /*env.setRestartStrategy(RestartStrategies.failureRateRestart(10, Time.minutes(1), Time.milliseconds(100)));
        env.enableCheckpointing(100);*/
            //env.enableCheckpointing(10000L, CheckpointingMode.AT_LEAST_ONCE);
            //period of the emitted Latency markers is 5 milliseconds
            //env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
        }
        env.getConfig().setLatencyTrackingInterval(5L);
        env.setBufferTimeout(bufferTimeout);
        long startTime = System.currentTimeMillis();
        DataStream<InputMetadata> inputMetadataDataStream = env
                .addSource(new CheckpointedInputMetadataSource(maxSeqCnt, servingSpeedMs, startTime, nbrCubes, nbrCameraTuples, sourceDelay, outputFile), "InputMetadata")
                .uid("InputMetadata")
                .assignTimestampsAndWatermarks(new InputMetadataAssigner())
                .keyBy((inputMetadata) ->
                        inputMetadata.inputMetadataKey != null ? inputMetadata.inputMetadataKey.ts : new Object());
        logger.debug("past inputMetadataFile source");

        DataStream<CameraWithCube> keyedByCamCameraStream = env
                .addSource(new CheckpointedCameraWithCubeSource(maxSeqCnt, servingSpeedMs, startTime, nbrCameras, outputFile, sourceDelay), "TileDB Camera")
                .uid("TileDB-Camera")
                .assignTimestampsAndWatermarks(new CameraAssigner())
                .setParallelism(1);
        DataStream<CameraWithCube> cameraWithCubeDataStream;
        //for copy, only perform copyImage functionality
        if (action != null && (action.equalsIgnoreCase(ALL) || action.equalsIgnoreCase(COPY))) {
            AsyncFunction<CameraWithCube, CameraWithCube> cameraWithCubeAsyncFunction =
                    new SampleCopyAsyncFunction(shutdownWaitTS, inputFile, options, nThreads, jobName);
            String copySlotSharingGroup = "default";
            if (!local) {
                copySlotSharingGroup = "copyImage";
            }
            DataStream<CameraWithCube> cameraWithCubeDataStreamAsync =
                    AsyncDataStream.unorderedWait(keyedByCamCameraStream, cameraWithCubeAsyncFunction, timeout, TimeUnit.MILLISECONDS, nCapacity)
                            .slotSharingGroup(copySlotSharingGroup)
                            .setParallelism(parallelCamTasks)
                            .rebalance();
            cameraWithCubeDataStream = cameraWithCubeDataStreamAsync.keyBy((cameraWithCube) -> cameraWithCube.cameraKey != null ?
                    cameraWithCube.cameraKey.getTs() : new Object());
        } else {
            cameraWithCubeDataStream = keyedByCamCameraStream.keyBy((cameraWithCube) -> cameraWithCube.cameraKey != null ?
                    cameraWithCube.cameraKey.getTs() : new Object());
        }
        logger.debug("past cameraFile source");

        if (action != null && (action.equalsIgnoreCase(ALL) || action.equalsIgnoreCase(READ))) {
            String uuid = UUID.randomUUID().toString();
            DataStream<Tuple2<InputMetadata, CameraWithCube>> enrichedCameraFeed = inputMetadataDataStream
                    .connect(cameraWithCubeDataStream)
                    .flatMap(new SyncLatchFunction(outputFile, outputPath, uuid, jobName))
                    .uid("connect2Streams")
                    .setParallelism(1).startNewChain(); //TODO:is this efficient or has to be for a logically correct Latch
            //enrichedCameraFeed.print();
            logger.debug("before KinesisProducerSink");
            /*String readSlotSharingGroup = "default";
            if (!local) {
                readSlotSharingGroup = "readImage";
            }*/
            //Setup Kinesis Producer
            Properties kinesisProducerConfig = new Properties();
            kinesisProducerConfig.setProperty(AWSConfigConstants.AWS_REGION, region);

            if (local) {
                kinesisProducerConfig.setProperty(AWSConfigConstants.AWS_ACCESS_KEY_ID, accessKey);
                kinesisProducerConfig.setProperty(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, secretKey);
            } else {
                kinesisProducerConfig.setProperty(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, "AUTO");
            }

            //only for Consumer
            //kinesisConsumerConfig.setProperty(ConsumerConfigConstants.SHARD_GETRECORDS_MAX, "10000");

            FlinkKinesisProducer<Tuple2<InputMetadata, CameraWithCube>> kinesisProducer = new FlinkKinesisProducer<>(
                    new Tuple2KinesisSchema(), kinesisProducerConfig);

            //TODO: kinesisProducer.setFailOnError(true);
            kinesisProducer.setDefaultStream(kinesisTopic);
            kinesisProducer.setDefaultPartition("0");//TODO: why from start ?

            //enrichedCameraFeed.addSink(new CubeProcessingSink(options, outputPath, uuid)) //, shutdownWaitTS
            enrichedCameraFeed
                    //.map(inputMetadataCameraWithCubeTuple2 -> (inputMetadataCameraWithCubeTuple2.f0))
                    .addSink(kinesisProducer)
                    .setParallelism(parallelCubeTasks)
                    //.slotSharingGroup("cubeProcessing")
                    .uid("KinesisProducerApp1-Sink");
            logger.debug("after KinesisProducerSink");
        }
        // execute program
        long t = System.currentTimeMillis();
        logger.debug("Execution Start env Time(millis) = " + t);
        //env.execute("Producer1 - Join InputMetadata with Camera feed using JNI & feed to KinesisProducerSink");
        env.execute(jobName);
        logger.debug("Execution Total duration env Time = " + (System.currentTimeMillis() - t) + " millis");

    }

    /**
     * An sample of {@link AsyncFunction} using a thread pool and executing working threads
     * to simulate multiple async operations. Only 1 instance of AsyncFunction exists.
     * It is called sequentially for each record in the respective partition of the stream.
     * Unless the asyncInvoke(...) method returns fast and relies on a callback (by the client),
     * it will not result in proper asynchronous I/O.
     * <p>For the real use case in production environment, the thread pool may stay in the
     * async client.
     */
    private static class SampleCopyAsyncFunction extends RichAsyncFunction<CameraWithCube, CameraWithCube> {
        private static final long serialVersionUID = 2098635244857937781L;

        private static ExecutorService executorService;

        private int counter;

        private final long shutdownWaitTS;
        private final String inputFile;
        private final String options;
        private final int nThreads;
        private final String jobName;
        private static NativeLoader nativeLoader;

        private transient Histogram histogram;
        private transient Histogram beforeCopyDiffGeneratedHistogram;

        SampleCopyAsyncFunction(long shutdownWaitTS, String inputFile, String options, int nThreads, String jobName) {
            this.shutdownWaitTS = shutdownWaitTS;
            this.inputFile = inputFile;
            this.options = options;
            this.nThreads = nThreads;
            this.jobName = jobName;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            synchronized (SampleCopyAsyncFunction.class) {
                if (counter == 0) {
                    executorService = Executors.newFixedThreadPool(nThreads);
                    logger.debug("SampleCopyAsyncFunction - executorService starting with {} threads", nThreads);
                    nativeLoader = NativeLoader.getInstance();
                }
                ++counter;
            }
            com.codahale.metrics.Histogram dropwizardHistogram =
                    new com.codahale.metrics.Histogram(new SlidingTimeWindowReservoir(5, TimeUnit.SECONDS)); //TODO: what value should it be ?
            this.histogram = getRuntimeContext()
                    .getMetricGroup()
                    .addGroup(jobName + "CopyMetrics")
                    .histogram(jobName + "copyImageMetrics", new DropwizardHistogramWrapper(dropwizardHistogram));
            this.beforeCopyDiffGeneratedHistogram = getRuntimeContext()
                    .getMetricGroup()
                    .addGroup(jobName + "DiffOfCopyAndGeneratedMetrics")
                    .histogram(jobName + "diffOfCopyAndGeneratedMetrics", new DropwizardHistogramWrapper(dropwizardHistogram));

        }

        @Override
        public void close() throws Exception {

            super.close();

            synchronized (SampleCopyAsyncFunction.class) {
                --counter;
                logger.debug("counter:{}", counter);
                if (counter == 0) {
                    logger.debug("SampleCopyAsyncFunction - executorService shutting down");
                    executorService.shutdown();
                    nativeLoader = null;
                    try {
                        if (!executorService.awaitTermination(shutdownWaitTS, TimeUnit.MILLISECONDS)) {
                            executorService.shutdownNow();
                        }
                    } catch (InterruptedException e) {
                        executorService.shutdownNow();
                    }
                }
            }
        }

        /**
         * Trigger async operation for each Stream input of CameraWithCube
         *
         * @param cameraWithCube input - Emitter value
         * @param resultFuture   output async Collector of the result when query returns - Wrap Emitter in a Promise/Future
         * @throws Exception exception thrown
         */
        @Override
        public void asyncInvoke(final CameraWithCube cameraWithCube, final ResultFuture<CameraWithCube> resultFuture) throws Exception {
            // each seq# in a diff thread
            this.executorService.submit(() -> {
                try {
                    logger.info("SampleCopyAsyncFunction - before JNI copyImage to copy S3 file to EFS");
                    //add ts/cam.jpg
                    String outputFile1 = cameraWithCube.getFileLocation();
                    logger.debug("outputFile1: {}", outputFile1);
                    long beforeCopyImageTS = System.currentTimeMillis();
                    //TODO: make it an async request through NativeLoader- currently synchronous JNI call - not reqd
                    final HashMap<String, Long> cameraWithCubeTimingMap = cameraWithCube.getTimingMap();
                    Long generatedTSL = (Long)cameraWithCubeTimingMap.get("Generated");
                    long generatedTS = generatedTSL != null ? generatedTSL.longValue() : 0l;
                    cameraWithCubeTimingMap.put("BeforeCopyImage", beforeCopyImageTS);
                    long diffOfCopyToGenerated = beforeCopyImageTS - generatedTS;
                    this.beforeCopyDiffGeneratedHistogram.update(diffOfCopyToGenerated);
                    if (diffOfCopyToGenerated > 500) {
                        //print error msg in log & size of queue? & AfterCopyImage -1 & emit it
                        logger.error("CopyImage Diff from BeforeCopyImage for GeneratedTS:{} is:{} which is > 500 ms for outputFile1:{}", generatedTS, diffOfCopyToGenerated, outputFile1);
                        //size of queue?
                        cameraWithCubeTimingMap.put("AfterCopyImage", -1L);
                    } else {
                        String checkStrValue = nativeLoader.copyImage(inputFile, outputFile1, options);
                        logger.debug("copyImage response checkStrValue:{}", checkStrValue);
                        //ERROR:FILE_OPEN:Could not open file for reading
                        //OK:1204835 bytes
                        if (checkStrValue != null) {
                            if (checkStrValue.startsWith(ERROR)) {
                                logger.error("Error copyImage:%s for CameraWithCube:%s", checkStrValue, cameraWithCube);
                                cameraWithCube.getTimingMap().put("Error", -1L);
                            } else if (checkStrValue.startsWith(OK)) {
                                String redisHost = checkStrValue.substring(3);
                                logger.debug("redisHost:{}", redisHost);
                                //TODO:cameraWithCube.fileLocation = redisHost;
                            }
                            long afterCopyImageTS = System.currentTimeMillis();
                            long timeTakenForCopyImage = afterCopyImageTS - beforeCopyImageTS;
                            this.histogram.update(timeTakenForCopyImage);
                            cameraWithCubeTimingMap.put("AfterCopyImage", afterCopyImageTS);
                            logger.info("copyImage checkStrValue: {}", checkStrValue);
                            logger.debug("SampleCopyAsyncFunction - after JNI copyImage to copy S3 file to EFS with efsLocation:{}", outputFile1);
                        }
                    }
                    //polls Queue of wrapped Promises for Completed
                    //The ResultFuture is completed with the first call of ResultFuture.complete. All subsequent complete calls will be ignored.
                    resultFuture.complete(
                            Collections.singletonList(cameraWithCube));
                } catch (Exception e) {
                    logger.error("SampleCopyAsyncFunction - Exception while making copyImage call: {}", e);
                    resultFuture.complete(new ArrayList<>(0));
                }
            });
        }
    }

    /*static final class CubeProcessingSink extends RichSinkFunction<Tuple2<InputMetadata, CameraWithCube>> {
        private String options;
        private String outputPath;
        private String fileName;
        ICsvMapWriter csvMapWriter = null;
        private String uuid;

        public CubeProcessingSink() {
        }

        public CubeProcessingSink(String options, String outputPath, String uuid) {
            this.options = options;
            this.outputPath = outputPath;
            this.uuid = uuid;
            this.fileName = this.outputPath + "/InputMetadata-" + this.uuid + ".csv";
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            csvMapWriter = new CsvMapWriter(new FileWriter(fileName),
                    CsvPreference.STANDARD_PREFERENCE);
            String[] header = {"ts", "cube", "cameraLst", "timingMap"};
            csvMapWriter.writeHeader(header);
            logger.debug("Exit CubeProcessingSink.open()");
        }

        @Override
        public void close() throws Exception {
            super.close();
            logger.debug("Entered CubeProcessingSink.close()");
            if (csvMapWriter != null) {
                try {
                    csvMapWriter.close();
                } catch (IOException ex) {
                    logger.error("Error closing the inputMetadata writer: " + ex);
                }
            }
        }

        @Override
        public void invoke(Tuple2<InputMetadata, CameraWithCube> tuple2, Context context) throws Exception {
            logger.info("CubeProcessingSink - before JNI readImage to retrieve EFS file from efsFileLocation: {}", tuple2.f1.fileLocation);
            logger.debug("tuple2.f0:{}, tuple2.f1:{}, options:{}", tuple2.f0, tuple2.f1, options);
            //TODO: change to use ROI values ??
            InputMetadata inputMetadata = tuple2.f0;
            long t = System.currentTimeMillis();
            inputMetadata.getTimingMap().put("BeforeReadImage", t);
            String checkStrValue = NativeLoader.getInstance().readImage(tuple2.f1.fileLocation, 0, 0, 10, 5, options);
            *//*String checkStrValue = "";
            Thread.sleep(10);*//*
            inputMetadata.getTimingMap().put("AfterReadImage", System.currentTimeMillis());
            writeInputMetadataCsv(tuple2, outputPath);
            logger.info("readImage checkStrValue: {}", checkStrValue);
            logger.debug("CubeProcessingSink - after JNI readImage to retrieve EFS file from efsFileLocation: {}", tuple2.f1.fileLocation);
        }

        private void writeInputMetadataCsv(Tuple2<InputMetadata, CameraWithCube> tuple2, String outputPath) throws Exception {
            logger.info("writeInputMetadataCsv start with outputPath:{}", outputPath);
            InputMetadata inputMetadata = tuple2.f0;
            InputMetadata.InputMetadataKey inputMetadataKey = inputMetadata.inputMetadataKey;
            // create mapper and schema
            CellProcessor[] processors = new CellProcessor[]{
                    new NotNull(), // ts
                    new NotNull(), // cube
                    new NotNull(), // cameraLst
                    new NotNull() // timingMap
            };

            try {
                String[] header = {"ts", "cube", "cameraLst", "timingMap"};
                final Map<String, Object> inputMetadataRow = new HashMap<String, Object>();
                inputMetadataRow.put(header[0], inputMetadataKey.ts);
                inputMetadataRow.put(header[1], inputMetadataKey.cube);
                inputMetadataRow.put(header[2], inputMetadata.cameraLst);
                inputMetadataRow.put(header[3], inputMetadata.timingMap);

                csvMapWriter.write(inputMetadataRow, header, processors);

            } catch (IOException ex) {
                logger.error("Error writing the inputMetadata CSV file: " + ex);
            } finally {
            }
        }
    }
*/

    /**
     * SyncLatchFunction FlatMap function for keyed managed store.
     * Latch only  allows Cube InputMetadata to flow through when all the corrresponding camera input arrives.
     * Stores Cube InputMetadata in memory buffer till all the camera data arrives.
     */
    static final class SyncLatchFunction extends RichCoFlatMapFunction<InputMetadata, CameraWithCube, Tuple2<InputMetadata, CameraWithCube>> {
        private String outputFile;
        private String outputPath;
        private String fileName;
        ICsvMapWriter csvCameraMapWriter = null;
        private String uuid;
        private final String jobName;

        public SyncLatchFunction(String outputFile, String outputPath, String uuid, String jobName) {
            this.outputFile = outputFile;
            this.outputPath = outputPath;
            this.uuid = uuid;
            fileName = outputPath + "/Camera-" + this.uuid + ".csv";
            this.jobName = jobName;
        }

        private transient Meter meter;
        //keyed, managed state
        //1,cu1, [(cam1,roi1),(cam2,roi2)...],count
        private MapState<InputMetadata.InputMetadataKey, InputMetadata> inputMetadataState;
        //1,cam1,fileName
        private MapState<CameraWithCube.CameraKey, CameraWithCube> cameraWithCubeState;

        /**
         * Register all State declaration
         *
         * @param config
         */
        @Override
        public void open(Configuration config) throws Exception {
            logger.debug("SyncLatchFunction Entered open");
            MapStateDescriptor<InputMetadata.InputMetadataKey, InputMetadata> inputMetadataMapStateDescriptor =
                    new MapStateDescriptor<>("inputMetadataState",
                            InputMetadata.InputMetadataKey.class, InputMetadata.class);
            inputMetadataState = getRuntimeContext().getMapState(inputMetadataMapStateDescriptor);
            MapStateDescriptor<CameraWithCube.CameraKey, CameraWithCube> cameraMapStateDescriptor =
                    new MapStateDescriptor<>("cameraWithCubeState",
                            CameraWithCube.CameraKey.class, CameraWithCube.class);
            cameraWithCubeState = getRuntimeContext().getMapState(cameraMapStateDescriptor);
            this.meter = getRuntimeContext()
                    .getMetricGroup()
                    .addGroup(jobName + "LatchMetrics")
                    .meter(jobName + "syncLatchMeter", new DropwizardMeterWrapper(new com.codahale.metrics.Meter()));
            csvCameraMapWriter = new CsvMapWriter(new FileWriter(fileName),
                    CsvPreference.STANDARD_PREFERENCE);
            String[] header = {"ts", "cam", "timingMap"};
            csvCameraMapWriter.writeHeader(header);
        }

        @Override
        public void close() throws Exception {
            logger.debug("SyncLatchFunction Entered close");
            super.close();
            inputMetadataState.clear();

            if (csvCameraMapWriter != null) {
                try {
                    csvCameraMapWriter.close();
                } catch (IOException ex) {
                    logger.error("Error closing the camera writer: " + ex);
                }
            }
        }

        /**
         * Data comes in from Input Metadata with (TS1,Cube1) as key with values [Camera1, Camera2], count = 2
         * Insert into InputMetadata state 1st.
         * Then check CameraWithCubeState with key (TS1, Camera1) key for existence in Camera(cameraWithCubeState) state.
         * If it doesn't exist, insert into CameraWithCube state TS1, Camera1 as key, values- [CU1], tileExists=false
         * if Camera row exists with tileExists=true(Tile Camera input exists), reduce count for (TS1,CU1) etc in a loop for all cubeLst entries.
         * if Camera row exists with tileExists=false (No Tile Camera input), update Camera state with new CU2 in cubeLst entry.
         *
         * @param inputMetadata incoming InputMetadata
         * @param collector     Collects data for output
         * @throws Exception Exception thrown
         */
        @Override
        public void flatMap1(InputMetadata inputMetadata, Collector<Tuple2<InputMetadata, CameraWithCube>> collector) throws Exception {
            long t2 = System.currentTimeMillis();
            logger.info("SyncLatchFunction- Entered flatMap1 with inputMetadata:{} count:{} with timestamp:{}", inputMetadata, inputMetadata.count, t2);
            inputMetadata.getTimingMap().put("IntoLatch", t2);
            final List<CameraTuple> inputMetaCameraLst = inputMetadata.cameraLst;
            inputMetadata.count = inputMetaCameraLst != null ? inputMetaCameraLst.size() : 0L;
            final InputMetadata.InputMetadataKey inputMetadataKey = inputMetadata.inputMetadataKey; //(1,CU1)
            final long inputMetaTs = inputMetadataKey != null ? inputMetadataKey.ts : 0L;
            final String inputMetaCube = inputMetadataKey != null ? inputMetadataKey.cube : null;
            if (inputMetaCube == null) {
                return;
            }
            //Insert into InputMetadata state 1st.
            inputMetadataState.put(inputMetadataKey, inputMetadata);
            //check in a loop for incoming inputMetadata with TS1, C1 key against existing Camera state data - cameraWithCube Map entries
            Iterator<CameraTuple> inputMetaCameraLstIterator = inputMetaCameraLst != null ? inputMetaCameraLst.iterator() : null;
            for (; inputMetaCameraLstIterator != null && inputMetaCameraLstIterator.hasNext(); ) {
                CameraTuple inputMetaCam = inputMetaCameraLstIterator.next();
                String inputMetaCamera = inputMetaCam != null ? inputMetaCam.getCamera() : null;
                //TS1,C1
                CameraWithCube.CameraKey cameraKeyFromInputMetadata = new CameraWithCube.CameraKey(inputMetaTs, inputMetaCamera);
                //check with key in cameraWithCubeState
                CameraWithCube cameraWithCube = cameraWithCubeState.get(cameraKeyFromInputMetadata);
                if (cameraWithCube != null) {
                    //key exists - hence check if tileExists
                    if (cameraWithCube.tileExists) {
                        logger.debug("[flatMap1] inputMetadata cameraWithCube tileExists:{}", cameraWithCube);
                        //reduce count in inputMetadata for TS1,CU1
                        List<String> existingCameraWithCubeLst = cameraWithCube.cubeLst;
                        //if tile exists & empty cubeLst, then reduce inputMetadata state count for ts1,cu1 by 1
                        if (existingCameraWithCubeLst != null && existingCameraWithCubeLst.size() == 0) {
                            //TODO: DUPLICATE CODE - REFACTOR LATER
                            final InputMetadata.InputMetadataKey existingMetadataKey = new InputMetadata.InputMetadataKey(inputMetaTs, inputMetaCube); //(TS1,CU1), (TS1,CU2)
                            final InputMetadata existingInputMetadata = inputMetadataState.get(existingMetadataKey); //(1,CU1)
                            if (existingInputMetadata != null) {
                                //reduce count by 1 for inputMetadata state
                                existingInputMetadata.count -= 1;
                                inputMetadataState.put(existingMetadataKey, existingInputMetadata);

                                if (existingInputMetadata.count == 0) {
                                    logger.info("$$$$$[flatMap1]Release Countdown latch with inputMetadata Collecting existingInputMetadata:{}, cameraWithCube:{}", existingInputMetadata, cameraWithCube);
                                    long t = System.currentTimeMillis();
                                    existingInputMetadata.getTimingMap().put("OutOfLatch", t);
                                    //cameraWithCube.getTimingMap().put("OutOfLatch", t);//TODO: is this needed ?

                                    Tuple2<InputMetadata, CameraWithCube> tuple2 = new Tuple2<>(existingInputMetadata, cameraWithCube);
                                    collector.collect(tuple2);
                                    //writeCameraCsv(tuple2.f1, outputPath);

                                } else {
                                    logger.debug("!!!!![flatMap1]  with inputMetadata reducing count:{} ,existingInputMetadata:{}, cameraWithCube:{}", existingInputMetadata.count, existingInputMetadata, cameraWithCube);
                                }
                            }
                        } else {
                            //reduce count for (1,CU1) from InputMetadata etc in a loop for all cubeLst entries.
                            Iterator<String> existingCameraWithCubeIterator = existingCameraWithCubeLst.iterator();
                            for (; existingCameraWithCubeIterator.hasNext(); ) {
                                String existingCameraWithCube = existingCameraWithCubeIterator.next(); //CU1, CU2
                                // if incoming inputMetadata's cu1 (inputCube) matches existingCube, remove from cameraWithCubeState's cubeLst
                                //if cameraWithCubeState's cubeLst's size is 0, remove key from cameraWithCubeState
                                if (existingCameraWithCube != null && existingCameraWithCube.equals(inputMetaCube)) {
                                    //TODO: do we need to do this - remove existingCube
                                    existingCameraWithCubeIterator.remove();
                                }
                                //for tile exists condition, reduce count for [1,cu1] key of inputMetadataState
                                final InputMetadata.InputMetadataKey existingMetadataKey = new InputMetadata.InputMetadataKey(inputMetaTs, inputMetaCube); //(1,CU1), (1,CU2)
                                final InputMetadata existingInputMetadata = inputMetadataState.get(existingMetadataKey); //(1,CU1)
                                if (existingInputMetadata != null) {
                                    //reduce count by 1 for inputMetadata state
                                    existingInputMetadata.count -= 1;
                                    inputMetadataState.put(existingMetadataKey, existingInputMetadata);
                                    //match InputMetadata.cameraTuple's with cameraWithCube and on matching camera, update fileLocation
                                    for (CameraTuple cameraTuple: existingInputMetadata.getCameraLst()) {
                                        if (cameraTuple.getCamera().equalsIgnoreCase(cameraWithCube.cameraKey.cam)) { //matches camera
                                            cameraTuple.setCamFileLocation(cameraWithCube.fileLocation);
                                            logger.debug("InputMetadata.cameraTuple matched camera and camFileLocation set to:{}", cameraWithCube.fileLocation);
                                        }
                                    }
                                    if (existingInputMetadata.count == 0) {
                                        logger.info("$$$$$[flatMap1] Release Countdown latch with inputMetadata Collecting existingInputMetadata:{}, cameraWithCube:{}", existingInputMetadata, cameraWithCube);
                                        long t = System.currentTimeMillis();
                                        existingInputMetadata.getTimingMap().put("OutOfLatch", t);
                                        //cameraWithCube.getTimingMap().put("OutOfLatch", t);
                                        Tuple2<InputMetadata, CameraWithCube> tuple2 = new Tuple2<>(existingInputMetadata, cameraWithCube);
                                        collector.collect(tuple2);
                                        //writeCameraCsv(tuple2.f1, outputPath);

                                    } else {
                                        logger.debug("$$$$$[flatMap1]  with inputMetadata reducing count existingInputMetadata.count:{}, cameraWithCube:{}", existingInputMetadata.count, cameraWithCube);
                                    }
                                }

                            }
                        }


                        if (existingCameraWithCubeLst != null && existingCameraWithCubeLst.size() == 0) {
                            //if no cubeLst and tileExists=true, remove the key from the camera state ???
                            //TODO: Use ProcessFunction with a timer to remove the camera data after a certain stipulated time
                            // cameraWithCubeState.remove(cameraKeyFromInputMetadata);
                            logger.info("[flatMap1] inputMetadata if no cubeLst and tileExists=true, remove the cameraKeyFromInputMetadata key from the camera state :{}", cameraKeyFromInputMetadata);
                        } else {
                            //update state with reduced cubeLst
                            cameraWithCubeState.put(cameraKeyFromInputMetadata, cameraWithCube);
                        }
                    } else {
                        //update into CameraWithCube with updated cubeLst containing new inputCube from inputMetadata
                        List<String> existingCubeLst = cameraWithCube.cubeLst;
                        if (existingCubeLst == null) {
                            existingCubeLst = new ArrayList<>();
                        }

                        if (!existingCubeLst.contains(inputMetaCube)) {
                            existingCubeLst.add(inputMetaCube);
                        }

                        cameraWithCube.getTimingMap().put("IntoLatch", t2);
                        cameraWithCubeState.put(cameraKeyFromInputMetadata, cameraWithCube);
                    }
                } else {
                    //insert into CameraWithCube with tileExists=false - i.e waiting for TileDB camera input to come in
                    List<String> newCubeLst = new ArrayList<>(Collections.singletonList(inputMetaCube));
                    CameraWithCube newCameraWithCube = new CameraWithCube(inputMetaTs, inputMetaCam.getCamera(), newCubeLst, false, outputFile);
                    newCameraWithCube.getTimingMap().put("IntoLatch", t2);
                    cameraWithCubeState.put(cameraKeyFromInputMetadata, newCameraWithCube);
                }
            }
            this.meter.markEvent();
        }

        /**
         * Data comes in from the Camera feed(cameraWithCube) with (TS1, Camera1) as key
         * Check if key exists in CameraWithCubeState
         * If camera key doesn't exist, insert into CameraWithCubeState with key & value- { empty cubeLst and tileExists= true }
         * If camera key exists, update CameraWithCubeState with key & value having tileExists= true
         * For camera key exists, check if value has a non-empty cubeLst. If no, stop.
         * If value has a non-empty cubeLst, reduce count for (TS1,CU1) etc in a loop for all cubeLst entries of Camera feed
         *
         * @param cameraWithCube Incoming Camera data
         * @param collector      Output collector
         * @throws Exception Exception thrown
         */
        @Override
        public void flatMap2(CameraWithCube cameraWithCube, Collector<Tuple2<InputMetadata, CameraWithCube>> collector) throws Exception {
            long t1 = System.currentTimeMillis();
            //TS1, C1
            logger.info("SyncLatchFunction- Entered flatMap2 with Camera data:{} with timestamp:{}", cameraWithCube, t1);
            //start TS cameIntoLatch
            cameraWithCube.getTimingMap().put("IntoLatch", t1);
            writeCameraCsv(cameraWithCube);
            final CameraWithCube.CameraKey cameraKey = cameraWithCube.cameraKey;
            final long cameraTS = cameraKey.ts;
            final String cameraKeyCam = cameraKey.getCam();
            final CameraWithCube existingCameraWithCube = cameraWithCubeState.get(cameraKey);
            if (existingCameraWithCube != null) {
                boolean tileExists = existingCameraWithCube.tileExists;
                final List<String> existingCubeLst = existingCameraWithCube.cubeLst;
                if (!tileExists) {
                    //update tileExists to true in camera state
                    existingCameraWithCube.tileExists = true;
                    cameraWithCubeState.put(cameraKey, existingCameraWithCube);
                }
                //if cubeLst exists
                logger.debug("[flatMap2] cameraWithCube existingCubeLst:{}", existingCubeLst);
                if (existingCubeLst != null && existingCubeLst.size() > 0) {
                    for (String existingCube : existingCubeLst) { //CU1, CU2
                        final InputMetadata.InputMetadataKey existingMetadataKey = new InputMetadata.InputMetadataKey(cameraTS, existingCube); //(TS1,CU1), (TS1,CU2)
                        final InputMetadata existingInputMetadata = inputMetadataState.get(existingMetadataKey);
                        if (existingInputMetadata != null) {
                            List<CameraTuple> existingInputMetaCameraLst = existingInputMetadata.cameraLst;
                            for (Iterator<CameraTuple> existingInputMetaCameraLstIterator = existingInputMetaCameraLst.iterator(); existingInputMetaCameraLstIterator.hasNext(); ) {
                                CameraTuple existingInputMetaCam = existingInputMetaCameraLstIterator.next();
                                String existingInputMetaCamStr = existingInputMetaCam.getCamera();
                                if (existingInputMetaCamStr != null && existingInputMetaCamStr.equals(cameraKeyCam)) {
                                    //want to keep existing inputMetaData & not remove incoming camera from cameraLst of inputMetadata state
                                    existingInputMetadata.count -= 1;
                                    //update cameraTuple's camFileLocation for matched CameraWithCube
                                    existingInputMetaCam.setCamFileLocation(cameraWithCube.fileLocation);
                                }
                            }

                            if (existingInputMetadata.count == 0) {
                                logger.info("$$$$$[flatMap2] Release Countdown latch with Camera data Collecting existingInputMetadata:{}, cameraWithCube:{}", existingInputMetadata, cameraWithCube);
                                long t = System.currentTimeMillis();
                                existingInputMetadata.getTimingMap().put("OutOfLatch", t);
                                //cameraWithCube.getTimingMap().put("OutOfLatch", t);
                                Tuple2<InputMetadata, CameraWithCube> tuple2 = new Tuple2<>(existingInputMetadata, cameraWithCube);
                                collector.collect(tuple2);
                                //writeCameraCsv(tuple2.f1, outputPath);
                                //remove from inputMetadata state if count is 0
                                //TODO: combine all inputMetadataState into 1 update operation for performance
                                //TODO:put this remove back in
                                //inputMetadataState.remove(existingMetadataKey);
                            } else {
                                //updated reduced count in inputMetadata
                                inputMetadataState.put(existingMetadataKey, existingInputMetadata);
                                Tuple2<InputMetadata, CameraWithCube> tuple2 = new Tuple2<>(existingInputMetadata, cameraWithCube);
                                //writeCameraCsv(tuple2.f1, outputPath);
                                logger.debug("$$$$$[flatMap2] with Camera data reducing count of existingInputMetadata:{}", existingInputMetadata);
                            }
                        }
                    }
                }
            } else {
                //insert into CameraWithCubeState with key & value- { empty cubeLst and tileExists= true }
                CameraWithCube newCameraWithCube = new CameraWithCube(cameraKey, Collections.emptyList(), true, outputFile);
                cameraWithCubeState.put(cameraKey, newCameraWithCube);
            }
            this.meter.markEvent();
        }

        private void writeCameraCsv(CameraWithCube cameraWithCube) throws Exception {
            logger.info("writeCameraCsv start with cameraWithCube:{}", cameraWithCube);
            CameraWithCube.CameraKey cameraKey = cameraWithCube.cameraKey;
            // create mapper and schema
            //ICsvMapWriter csvCameraMapWriter = null;
            CellProcessor[] processors = new CellProcessor[]{
                    new NotNull(), // ts
                    new NotNull(), // cam
                    new NotNull() // timingMap
            };

            try {
                String[] header = {"ts", "cam", "timingMap"};
                final Map<String, Object> cameraRow = new HashMap<String, Object>();
                cameraRow.put(header[0], cameraKey.ts);
                cameraRow.put(header[1], cameraKey.cam);
                cameraRow.put(header[2], cameraWithCube.timingMap);

                csvCameraMapWriter.write(cameraRow, header, processors);
            } catch (IOException ex) {
                logger.error("Error writing the camera CSV file: " + ex);
            } finally {
            }
        }
    }


}
