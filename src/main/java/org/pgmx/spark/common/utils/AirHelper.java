package org.pgmx.spark.common.utils;

import org.apache.log4j.Logger;
import org.apache.spark.streaming.api.java.JavaDStream;


public class AirHelper {

    private static final Logger LOG = Logger.getLogger(AirHelper.class);

    /**
     * Persists the RDD at the given path
     *
     * @param javaDStream
     */
    public static void persist(JavaDStream<? extends Object> javaDStream, Class clazz) {
        String path = AirConstants.RAW_OUTPUT_DIR + "/" + clazz.getSimpleName();
        persist(javaDStream, path);
    }

    private static void persist(JavaDStream<? extends Object> javaDStream, String path) {
        javaDStream.foreachRDD(rdd -> {
            if (rdd.count() > 0) {
                LOG.info("Saving output to " + path);
                rdd.saveAsTextFile(path);
            } else {
                LOG.info("-- no data to save --");
            }
        });
    }

    public static void persist(JavaDStream<? extends Object> javaDStream, String subDir, Class clazz) {
        String path = AirConstants.RAW_OUTPUT_DIR + "/" + clazz.getSimpleName() + "/" + subDir;
        persist(javaDStream, path);
    }
}
