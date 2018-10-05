#!/bin/bash
cd /Users/vkbalakr/work/flink-examples/understanding-apache-flink/03-processing-infinite-streams-of-data/demo3/
mvn clean package

# Create a temporary filename in /tmp directory
jar_files=$(mktemp)
# Create classpath string of dependencies from the local repository to a file
mvn -Dmdep.outputFile=$jar_files dependency:build-classpath
#export LD_LIBRARY_PATH=/Users/vkbalakr/work/flink-examples/understanding-apache-flink/03-processing-infinite-streams-of-data/demo1/target/classes:$LD_LIBRARY_PATH
classpath_values=$(cat $jar_files)
#echo "classpath_values:" $classpath_values
#java "-javaagent:/Applications/IntelliJ IDEA CE.app/Contents/lib/idea_rt.jar=51214:/Applications/IntelliJ IDEA CE.app/Contents/bin" -Dfile.encoding=UTF-8 -classpath target/flink-intel-1.0.jar:$classpath_values com.intel.flink.state.MapTwoStreamsProducer1 --maxSeqCnt 3 --parallelCam 3 --nbrCameras 3 --nbrCameraTuples 3 --parallelCube 3 --nbrCubes 3
#worked - NativeLoader
#java -Djava.library.path=/Users/vkbalakr/work/flink-examples/understanding-apache-flink/03-processing-infinite-streams-of-data/demo1/src/main/resources -classpath target/flink-intel-1.0.jar:/Users/vkbalakr/work/cmake-swig-java/cmake-swig-java/build:$classpath_values com.intel.flink.jni.NativeLoader file:///tmp/file.jpg
#worked
java -Djava.library.path=/Users/vkbalakr/work/flink-examples/understanding-apache-flink/03-processing-infinite-streams-of-data/demo3/target/classes -classpath target/flink-intel-demo3-1.0.jar:$classpath_values -jar target/flink-intel-demo3-1.0.jar --maxSeqCnt 50 --parallelCam 1 --nbrCameras 1 --nbrCameraTuples 1 --parallelCube 1 --nbrCubes 1 --timeout 100000 --shutdownWaitTS 100000 --nThreads 2 --nCapacity 100 --inputFile file:///Users/vkbalakr/Downloads/test-img.jpg --outputFile file:///tmp --options test --outputPath /tmp/demo --local true --checkpoint false --kinesisTopic app1-stream --region us-west-2 --jobName App1 --accessKey .. --secretKey ..
#java -Djava.library.path=/usr/local/lib -classpath target/flink-intel-1.0.jar:$classpath_values -jar target/flink-intel-1.0.jar --maxSeqCnt 3 --parallelCam 3 --nbrCameras 3 --nbrCameraTuples 3 --parallelCube 3 --nbrCubes 3 --inputFile file:///Users/vkbalakr/Downloads/test-img.jpg --outputFile file:///tmp --options test
#java -classpath target/flink-intel-1.0.jar:$classpath_values -jar target/flink-intel-1.0.jar --maxSeqCnt 3 --parallelCam 3 --nbrCameras 3 --nbrCameraTuples 3 --parallelCube 3 --nbrCubes 3 --inputFile file:///Users/vkbalakr/Downloads/test-img.jpg --outputFile file:///tmp/ --options test
#./bin/flink run target/flink-intel-1.0.jar --maxSeqCnt 3 --parallelCam 3 --nbrCameras 3 --nbrCameraTuples 3 --parallelCube 3 --nbrCubes 3 --inputFile file:///Users/vkbalakr/Downloads/test-img.jpg --outputFile file:///tmp/file.jpg --options test
#java -Dhttp.proxyHost=http://proxy-chain.intel.com -Dhttp.proxyPort=911 -Djava.library.path=. -jar Main.jar s3://netowrkingteam-test/FD19_SAMPLE_SET/ImageEnhancement/F0000/ForReconstruction/0001.tif file:///home/ubuntu/temp test