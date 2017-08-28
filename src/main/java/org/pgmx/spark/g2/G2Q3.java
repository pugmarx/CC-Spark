package org.pgmx.spark.g2;

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
import org.pgmx.spark.common.utils.AirConstants;
import org.pgmx.spark.common.utils.AirHelper;
import scala.Tuple2;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.spark_project.guava.base.Preconditions.checkNotNull;

/**
 * Group 2 Q1
 */
public final class G2Q3 {
    private static final Logger LOG = Logger.getLogger(G2Q3.class);

    private G2Q3() {
    }


    public static void main(String[] args) throws Exception {

        try {
            checkNotNull(args[0], "No origin code specified, cannot continue");
            checkNotNull(args[1], "No destination code specified, cannot continue");

            SparkConf sparkConf = new SparkConf().setAppName("G2Q3").setMaster(AirConstants.MASTER_STRING);
            sparkConf.set("spark.streaming.concurrentJobs", AirConstants.STREAMING_JOB_COUNT);

            // Create the context with 2 seconds batch size
            JavaStreamingContext jssc = new JavaStreamingContext(sparkConf,
                    new Duration(AirConstants.FETCH_COUNT_INTERVAL));
            jssc.checkpoint(AirConstants.CHECKPOINT_DIR);

            int numThreads = Integer.parseInt(AirConstants.NUM_THREADS);
            Map<String, Integer> topicMap = new HashMap<>();
            String[] topics = AirConstants.IN_TOPIC.split(",");
            for (String topic : topics) {
                topicMap.put(topic, numThreads);
            }

            // Pick the messages
            JavaDStream<String> lines = KafkaUtils.createStream(jssc, AirConstants.ZK_HOST, AirConstants.IN_GROUP,
                    topicMap).map(Tuple2::_2);

            // Filter by origin code
            JavaDStream<String> filteredLines = lines.filter(s ->
                    StringUtils.equals(s.split(",")[AirConstants.ORIGIN_INDEX], args[0])
                            && StringUtils.equals(s.split(",")[AirConstants.DEST_INDEX], args[1]));

            // Get pairs from these filtered lines output is AIRLINE -> ARR_DELAY
            JavaPairDStream<String, Integer> carrierArrDelay = filteredLines.mapToPair(new AirlineArrivalDelay());

            // FIXME partitioner count is hard-coded
            JavaPairDStream<String, AvgCount> avgCounts =
                    carrierArrDelay.combineByKey(createAcc, addAndCount, combine, new HashPartitioner(10),
                            false);

            // FIXME debug
            //avgCounts.print();

            // Store in a stateful ds
            JavaPairDStream<String, AvgCount> statefulMap = avgCounts.updateStateByKey(COMPUTE_RUNNING_AVG);

            // unsorted // FIXME for debug
            statefulMap.print(10);

            // Puts the juicy stuff in OriginDestCarrierArrDelayKey, the Integer is useless -- just a placeholder
            JavaPairDStream<OriginDestCarrierArrDelayKey, Integer> airports =
                    statefulMap.mapToPair(new PairConverter(args[0], args[1]));

            // Sorted airport list
            JavaDStream<OriginDestCarrierArrDelayKey> sortedOrgDestCarrierArrAvgs
                    = airports.transform(new RDDSortTransformer());

            // **** Print top 10 from sorted map ***
            sortedOrgDestCarrierArrAvgs.print(10);

            // Persist! //TODO restrict to 10?
            AirHelper.persist(sortedOrgDestCarrierArrAvgs, G2Q3.class);

            jssc.start();
            jssc.awaitTermination();

        } catch (Exception e) {
            LOG.error("----- Error while running spark subscriber -------", e);
        }
    }


    public static class AvgCount implements Serializable {
        public AvgCount(int total, int num) {
            total_ = total;
            num_ = num;
        }

        public int total_;
        public int num_;

        public float avg() {
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
    static class RDDSortTransformer implements Function<JavaPairRDD<OriginDestCarrierArrDelayKey, Integer>,
            JavaRDD<OriginDestCarrierArrDelayKey>> {
        @Override
        public JavaRDD<OriginDestCarrierArrDelayKey> call(JavaPairRDD<OriginDestCarrierArrDelayKey, Integer>
                                                                  unsortedRDD)
                throws Exception {
            return unsortedRDD.sortByKey().keys(); // ASC sort
        }
    }


    /**
     * Used to prepare a CustomKey (@AirportKey) based RDD out of the "raw" (String,Integer) RDD
     */
    static class PairConverter implements PairFunction<Tuple2<String, AvgCount>, OriginDestCarrierArrDelayKey,
            Integer> {
        String origin;
        String dest;

        public PairConverter(String org, String dest) {
            this.origin = org;
            this.dest = dest;
        }

        @Override
        public Tuple2<OriginDestCarrierArrDelayKey, Integer> call(Tuple2<String, AvgCount> tuple2) throws Exception {
            return new Tuple2(new OriginDestCarrierArrDelayKey(origin, dest, tuple2._1(), tuple2._2().avg()), 0);
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


    static class OriginDestCarrierArrDelayKey implements Comparable<OriginDestCarrierArrDelayKey>, Serializable {
        private String origin;
        private String destination;
        private String airline;
        private Float avgArrivalDelay;


        public String getDestination() {
            return destination;
        }

        public OriginDestCarrierArrDelayKey(String origin, String destination, String airline, Float avgArrDelay) {
            this.origin = origin;
            this.destination = destination;
            this.airline = airline;
            this.avgArrivalDelay = avgArrDelay;
        }

        @Override
        public int compareTo(OriginDestCarrierArrDelayKey o) {
            return this.avgArrivalDelay.compareTo(o.avgArrivalDelay);
        }

        @Override
        public String toString() {
            return origin + "," + destination + "," + airline + "," + avgArrivalDelay;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof OriginDestCarrierArrDelayKey)) return false;
            OriginDestCarrierArrDelayKey that = (OriginDestCarrierArrDelayKey) o;
            return getDestination().equals(that.getDestination());
        }

        @Override
        public int hashCode() {
            return getDestination().hashCode();
        }
    }

    public static class AirlineArrivalDelay implements PairFunction<String, String, Integer> {

        public Tuple2<String, Integer> apply(String s) {
            String[] arr = s.split(",");

            Integer arrDelay = StringUtils.isEmpty(arr[AirConstants.ARR_DELAY_INDEX]) ?
                    0 : Float.valueOf(arr[AirConstants.ARR_DELAY_INDEX]).intValue();
            return new Tuple2(arr[AirConstants.UNIQUE_CARRIER_INDEX], arrDelay);
        }

        @Override
        public Tuple2<String, Integer> call(String s) throws Exception {
            String[] arr = s.split(",");

            Integer arrDelay = StringUtils.isEmpty(arr[AirConstants.ARR_DELAY_INDEX]) ?
                    0 : Float.valueOf(arr[AirConstants.ARR_DELAY_INDEX]).intValue();
            return new Tuple2(arr[AirConstants.UNIQUE_CARRIER_INDEX], arrDelay);
        }
    }
}
