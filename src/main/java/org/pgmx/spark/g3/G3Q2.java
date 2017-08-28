package org.pgmx.spark.g3;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.pgmx.spark.common.utils.AirHelper;
import scala.Tuple2;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.pgmx.spark.common.utils.AirConstants.*;
import static org.spark_project.guava.base.Preconditions.checkNotNull;

/**
 * Group 3 Q2
 */
public final class G3Q2 {

    // FIXME ************************************************
    // ****** 2. Change topic name as this is for 2008 data
    // FIXME ************************************************

    private static final Logger LOG = Logger.getLogger(G3Q2.class);


    private G3Q2() {
    }


    public static void main(String[] args) throws Exception {

        try {
            checkNotNull(args[0], "No origin code specified, cannot continue");
            checkNotNull(args[1], "No transit code specified, cannot continue");
            checkNotNull(args[2], "No destination code specified, cannot continue");
            checkNotNull(args[3], "No start date specified, cannot continue");

            String origin = args[0];
            String transit = args[1];
            String dest = args[2];
            String startDate = args[3];

            SparkConf sparkConf = new SparkConf().setAppName("G3Q2").setMaster(MASTER_STRING);
            sparkConf.set("spark.streaming.concurrentJobs", STREAMING_JOB_COUNT);

            // Create the context with 2 seconds batch size
            JavaStreamingContext jssc = new JavaStreamingContext(sparkConf,
                    new Duration(FETCH_COUNT_INTERVAL));

            // Checkpoint -- needed for persistence (maintaining state)
            jssc.checkpoint(CHECKPOINT_DIR);

            int numThreads = Integer.parseInt(NUM_THREADS);
            Map<String, Integer> topicMap = new HashMap<>();
            String[] topics = IN_TOPIC.split(",");
            for (String topic : topics) {
                topicMap.put(topic, numThreads);
            }

            // Pick the messages
            JavaDStream<String> lines = KafkaUtils.createStream(jssc, ZK_HOST, IN_GROUP,
                    topicMap).map(Tuple2::_2);

            // Leg1
            processLeg1(origin, transit, startDate, lines);

            // Leg2
            processLeg2(transit, dest, startDate, lines);

            jssc.start();
            jssc.awaitTermination();

        } catch (Exception e) {
            LOG.error("----- Error while running spark subscriber -------", e);
        }
    }

    private static class Leg1Filter implements Function<String, Boolean> {
        private String origin;
        private String dest;
        private Validator v;

        public Leg1Filter(String origin, String dest, Validator v) {
            this.origin = origin;
            this.dest = dest;
            this.v = v;
        }

        @Override
        public Boolean call(String s) throws Exception {
            String[] arr = s.split(",");
            return StringUtils.equals(arr[ORIGIN_INDEX], origin)
                    && StringUtils.equals(arr[DEST_INDEX], dest)
                    && v.isValidLeg1Date(arr[FLT_DATE_INDEX])
                    && v.isValidLeg1Time(arr[DEP_TIME_INDEX]);
        }
    }

    private static class Leg2Filter implements Function<String, Boolean> {
        private String origin;
        private String dest;
        private Validator v;

        public Leg2Filter(String origin, String dest, Validator v) {
            this.origin = origin;
            this.dest = dest;
            this.v = v;
        }

        @Override
        public Boolean call(String s) throws Exception {
            String[] arr = s.split(",");
            return StringUtils.equals(arr[ORIGIN_INDEX], origin)
                    && StringUtils.equals(arr[DEST_INDEX], dest)
                    && v.isValidLeg2Date(arr[FLT_DATE_INDEX])
                    && v.isValidLeg2Time(arr[DEP_TIME_INDEX]);
        }
    }


    //////////////////////////////////////////////////////////////////////
    //////////////////////////////// L E G 1 /////////////////////////////
    //////////////////////////////////////////////////////////////////////
    private static void processLeg1(String leg1Origin, String leg1Dest, String startDate,
                                    JavaDStream<String> lines) {

        Validator validator = new Validator(startDate);
        JavaDStream<String> filteredLines = lines.filter(new Leg1Filter(leg1Origin, leg1Dest, validator));

        // Get pairs from these filtered lines output is (ORG,DEST) -> ARR_DELAY
        JavaPairDStream<FlightLegKey, Integer> carrierArrDelay
                = filteredLines.mapToPair(new OrgDestArrDelay("LEG_1"));

        // FIXME partitioner count is hard-coded
        JavaPairDStream<FlightLegKey, AvgCount> avgCounts =
                carrierArrDelay.combineByKey(createAcc, addAndCount, combine, new HashPartitioner(10),
                        false);

        // Store in a stateful ds
        JavaPairDStream<FlightLegKey, AvgCount> statefulMap = avgCounts.updateStateByKey(COMPUTE_RUNNING_AVG);

        // unsorted // FIXME for debug
        //statefulMap.print();

        // Puts the juicy stuff in FlightLegKey, the Integer is useless -- just a placeholder
        JavaPairDStream<FlightLegKey, Integer> airports = statefulMap.mapToPair(new PairConverter());

        // Sorted airport list
        JavaDStream<FlightLegKey> sortedOrgDestArrAvgs = airports.transform(new RDDSortTransformer());

        // **** Print top 10 from sorted map ***
        sortedOrgDestArrAvgs.print();

        // Persist!
        AirHelper.persist(sortedOrgDestArrAvgs, "LEG_1", G3Q2.class);
    }

    //////////////////////////////////////////////////////////////////////
    //////////////////////////////// L E G 2 /////////////////////////////
    //////////////////////////////////////////////////////////////////////
    private static void processLeg2(String leg2Origin, String leg2Dest, String startDate,
                                    JavaDStream<String> lines) {

        Validator validator = new Validator(startDate);
        JavaDStream<String> filteredLines = lines.filter(new Leg2Filter(leg2Origin, leg2Dest, validator));

        // Get pairs from these filtered lines output is (ORG,DEST) -> ARR_DELAY
        JavaPairDStream<FlightLegKey, Integer> carrierArrDelay
                = filteredLines.mapToPair(new OrgDestArrDelay("LEG_2"));

        // FIXME partitioner count is hard-coded
        JavaPairDStream<FlightLegKey, AvgCount> avgCounts =
                carrierArrDelay.combineByKey(createAcc, addAndCount, combine, new HashPartitioner(10),
                        false);

        // Store in a stateful ds
        JavaPairDStream<FlightLegKey, AvgCount> statefulMap = avgCounts.updateStateByKey(COMPUTE_RUNNING_AVG);

        // unsorted // FIXME for debug
        //statefulMap.print();

        // Puts the juicy stuff in FlightLegKey, the Integer is useless -- just a placeholder
        JavaPairDStream<FlightLegKey, Integer> airports = statefulMap.mapToPair(new PairConverter());

        // Sorted airport list
        JavaDStream<FlightLegKey> sortedOrgDestArrAvgs = airports.transform(new RDDSortTransformer());

        // **** Print top 10 from sorted map ***
        sortedOrgDestArrAvgs.print();

        // Persist!
        AirHelper.persist(sortedOrgDestArrAvgs, "LEG_2", G3Q2.class);
    }

    public static class AvgCount implements Serializable {
        public AvgCount(int total, int num) {
            total_ = total;
            num_ = num;
        }

        public int total_;
        public int num_;

        public Float avg() {
            return total_ / (float) num_;
        }

        @Override
        public String toString() {
            return num_ == 0 ? "0.0" : "" + avg();
        }
    }

    // FIXME num is init to 1 -- should be OK?
    static Function<Integer, AvgCount> createAcc = x -> new AvgCount(x, 1);

    static Function2<AvgCount, Integer, AvgCount> addAndCount =
            (a, x) -> {
                a.total_ += x;
                a.num_ += 1;
                return a;
            };

    static Function2<AvgCount, AvgCount, AvgCount> combine =
            (a, b) -> {
                a.total_ += b.total_;
                a.num_ += b.num_;
                return a;
            };


    /**
     * We used this transformer because sortByKey is only available here. Other (non-pair-based) options did
     * not have a built-in option to sort by keys
     */
    static class RDDSortTransformer implements Function<JavaPairRDD<FlightLegKey, Integer>,
            JavaRDD<FlightLegKey>> {
        @Override
        public JavaRDD<FlightLegKey> call(JavaPairRDD<FlightLegKey, Integer> unsortedRDD) throws Exception {
            return unsortedRDD.sortByKey().keys(); // ASC sort
        }
    }

    /**
     * Used to prepare a CustomKey (@AirportKey) based RDD out of the "raw" (String,Integer) RDD
     */
    static class PairConverter implements PairFunction<Tuple2<FlightLegKey, AvgCount>, FlightLegKey,
            Integer> {
        @Override
        public Tuple2<FlightLegKey, Integer> call(Tuple2<FlightLegKey, AvgCount> tuple2) throws Exception {
            FlightLegKey key = tuple2._1();
            key.setAvgCount(tuple2._2());
            return new Tuple2(key, 0); // ignored key
        }
    }


    // List of incoming vals, currentVal, returnVal
    private static Function2<List<AvgCount>, Optional<AvgCount>, Optional<AvgCount>>
            COMPUTE_RUNNING_AVG = (nums, current) -> {

        AvgCount running = current.orElse(new AvgCount(0, 0));

        for (AvgCount avgCount : nums) {
            running.total_ += avgCount.total_;
            running.num_ += avgCount.num_;
        }
        return Optional.of(running);
    };


    public static class FlightLegKey implements Serializable, Comparable<FlightLegKey> {
        private String flightLeg;
        private String origin;
        private String dest;
        private String fltDate;
        private String airline;
        private String fltNum;
        private String depTime;
        private AvgCount avgCount;

        public void setAvgCount(AvgCount avgCount) {
            this.avgCount = avgCount;
        }


        @Override
        public int compareTo(FlightLegKey that) {
            return this.avgCount.avg().compareTo(that.avgCount.avg());
        }

        public FlightLegKey(String origin, String dest, String fltDate, String airline, String fltNum) {
            this.origin = origin;
            this.dest = dest;
            this.fltDate = fltDate;
            this.airline = airline;
            this.fltNum = fltNum;
            //this.depTime = depTime;
        }

        public FlightLegKey(String flightLeg, String origin, String dest, String fltDate, String airline,
                            String fltNum, String depTime) {
            this.flightLeg = flightLeg;
            this.origin = origin;
            this.dest = dest;
            this.fltDate = fltDate;
            this.airline = airline;
            this.fltNum = fltNum;
            this.depTime = depTime;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof FlightLegKey)) return false;

            FlightLegKey that = (FlightLegKey) o;

            if (!origin.equals(that.origin)) return false;
            if (!dest.equals(that.dest)) return false;
            if (!fltDate.equals(that.fltDate)) return false;
            if (!airline.equals(that.airline)) return false;
            return fltNum.equals(that.fltNum);
        }

        @Override
        public int hashCode() {
            int result = origin.hashCode();
            result = 31 * result + dest.hashCode();
            result = 31 * result + fltDate.hashCode();
            result = 31 * result + airline.hashCode();
            result = 31 * result + fltNum.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return flightLeg + "," +
                    origin + ',' +
                    dest + ',' +
                    fltDate + ',' +
                    depTime + ',' +
                    airline + ',' +
                    fltNum + ',' +
                    (null == avgCount ? "" : avgCount.avg().toString());
        }
    }

    public static class OrgDestArrDelay implements PairFunction<String, FlightLegKey, Integer> {
        private String flightLeg;

        public OrgDestArrDelay(String flightLeg) {
            this.flightLeg = flightLeg;
        }

        public Tuple2<FlightLegKey, Integer> apply(String s) {
            String[] arr = s.split(",");
            FlightLegKey key = new FlightLegKey(flightLeg, arr[ORIGIN_INDEX], arr[DEST_INDEX], arr[FLT_DATE_INDEX],
                    arr[UNIQUE_CARRIER_INDEX], arr[FLT_NUM_INDEX], arr[DEP_TIME_INDEX]);
            Integer arrDelay = StringUtils.isEmpty(arr[ARR_DELAY_INDEX]) ?
                    0 : Float.valueOf(arr[ARR_DELAY_INDEX]).intValue();
            return new Tuple2(key, arrDelay);
        }

        @Override
        public Tuple2<FlightLegKey, Integer> call(String s) throws Exception {
            String[] arr = s.split(",");
            FlightLegKey key = new FlightLegKey(flightLeg, arr[ORIGIN_INDEX], arr[DEST_INDEX], arr[FLT_DATE_INDEX],
                    arr[UNIQUE_CARRIER_INDEX], arr[FLT_NUM_INDEX], arr[DEP_TIME_INDEX]);
            Integer arrDelay = StringUtils.isEmpty(arr[ARR_DELAY_INDEX]) ? 0
                    : Float.valueOf(arr[ARR_DELAY_INDEX]).intValue();
            return new Tuple2(key, arrDelay);
        }
    }
}
