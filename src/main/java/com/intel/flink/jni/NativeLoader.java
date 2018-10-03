package com.intel.flink.jni;

import com.intel.proxy.StoragePOC;
import com.intel.proxy.example;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NativeLoader {
    private static final Logger logger = LoggerFactory.getLogger(NativeLoader.class);

    static {
        try {
            //The path of file inside JAR as absolute path (beginning with '/'), e.g. /package/File.ext
            //NativeUtils.loadLibraryFromJar("/Users/vkbalakr/work/flink-examples/understanding-apache-flink/03-processing-infinite-streams-of-data/demo1/src/main/resources/lib/mac/libexample.jnilib"); //TODO: use variable
            System.loadLibrary("example"); //needs -Djava.library.path to point to libexample.so or libexample.dylib
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static StoragePOC storagePOC;

    private NativeLoader() {
        if (storagePOC == null) {
            storagePOC = new StoragePOC("test");
        }
    }

    private static NativeLoader nativeLoader;

    public static NativeLoader getInstance() {
        if (nativeLoader == null) {
            nativeLoader = new NativeLoader();
        }
        return nativeLoader;
    }

    public  String copyImage(String inputFile, String outputFile, String options) {
        logger.info("Entered copyImage() - inputFile:{}, outputFile:{}, options:{}", inputFile, outputFile, options);

        //String returnValue = example.CopyImage(inputFile, outputFile, options);
        String returnValue = storagePOC.CopyImage(inputFile, outputFile, options);
        //String returnValue = new StoragePOC(options).CopyImage(inputFile, outputFile, options);
        logger.debug(returnValue);
        return returnValue;
    }

    public String readImage(String inputFile, int x, int y, int width, int height, String options) {
        logger.info("readImage() - inputFile:{}, x:{},y:{},width:{},height:{}, options:{}", inputFile, x, y, width, height, options);
        //String returnValue = example.ReadImage(inputFile, new StoragePOC.ROI(x, y, width, height), options);
        String returnValue = storagePOC.ReadImage(inputFile, new StoragePOC.ROI(x, y, width, height), options);
        //String returnValue = new StoragePOC(options).ReadImage(inputFile, new StoragePOC.ROI(x, y, width, height), options);
        logger.debug(returnValue);
        return returnValue;
    }

    public static void main(String[] args) {

        //NativeLoader.copyImage(args);
        //NativeLoader.readImage(args[0], 0, 0, 10, 5, "test");
        NativeLoader.getInstance().readImage(args[0], 0, 0, 10, 5, "test");
    }

    //Declaration of native method
    //public final static native int foo();

}
