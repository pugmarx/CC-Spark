package org.pgmx.spark.common.utils;

import java.util.UUID;

public class AirConstants {

    //******** command-line props
    public static final String ORIGIN_CODE_PROP = "originCode";
    public static final String TRANSIT_CODE_PROP = "transitCode";
    public static final String DEST_CODE_PROP = "destCode";
    public static final String FLT_LEG1_DATE_PROP = "flightLeg1Date";
    public static final String FLT_LEG2_DATE_PROP = "flightLeg2Date";
    public static final String FLT_DATE_PROP_FORMAT = "dd/MM/yyyy";
    public static final String FLT_DATE_INPUT_FORMAT = "yyyy-MM-dd";
    public static final String FLT_TIME_INPUT_FORMAT = "HHmm";

    //******** indices
    public static final int YEAR_INDEX = 0;
    public static final int DAY_OF_WEEK_INDEX = 1;
    public static final int FLT_DATE_INDEX = 2;
    public static final int UNIQUE_CARRIER_INDEX = 3;
    public static final int FLT_NUM_INDEX = 4;
    public static final int ORIGIN_INDEX = 5;
    public static final int DEST_INDEX = 6;
    public static final int DEP_TIME_INDEX = 7;
    public static final int ARR_TIME_INDEX = 8;
    public static final int DEP_DELAY_INDEX = 9;
    public static final int ARR_DELAY_INDEX = 10;
    public static final int CANCELLED_INDEX = 11;

    //********* misc props
    public static final String TEMP_DIR = "tmp";
    public static final String FLT_LEG_PROP = "flightLeg";

    public static final String RAW_OUTPUT_DIR = "./output"; //FIXME

    public static final String ZK_HOST = "localhost:2181"; // FIXME
    public static final String IN_TOPIC = "testspark"; // FIXME


    public static final String IN_GROUP = "spark_" + UUID.randomUUID(); //FIXME
    public static final String NUM_THREADS = "1";

    public static final String CHECKPOINT_DIR = "/tmp/spark_checkpoints";

    public static final String STREAMING_JOB_COUNT = "10";
    public static final int FETCH_COUNT_INTERVAL = 20000; // FIXME
    public static final String MASTER_STRING = "local[*]"; // FIXME


}
