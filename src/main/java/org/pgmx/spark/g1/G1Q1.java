package org.pgmx.spark.g1;

import org.apache.log4j.Logger;
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
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.pgmx.spark.common.utils.AirConstants;
import org.pgmx.spark.common.utils.AirHelper;
import scala.Tuple2;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Group 1 Q1
 */
public final class G1Q1 {

    private static final Logger LOG = Logger.getLogger(G1Q1.class);

    private G1Q1() {
    }

    public static void main(String[] args) throws Exception {

        try {
            SparkConf sparkConf = new SparkConf().setAppName("G1Q1").setMaster(AirConstants.MASTER_STRING);
            sparkConf.set("spark.streaming.concurrentJobs", AirConstants.STREAMING_JOB_COUNT);

            // Create the context with 2 seconds batch size
            JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(AirConstants.FETCH_COUNT_INTERVAL));
            jssc.checkpoint(AirConstants.CHECKPOINT_DIR);

            int numThreads = Integer.parseInt(AirConstants.NUM_THREADS);
            Map<String, Integer> topicMap = new HashMap<>();
            String[] topics = AirConstants.IN_TOPIC.split(",");
            for (String topic : topics) {
                topicMap.put(topic, numThreads);
            }

            JavaPairReceiverInputDStream<String, String> messages =
                    KafkaUtils.createStream(jssc, AirConstants.ZK_HOST, AirConstants.IN_GROUP, topicMap);

            // Pick the messages
            JavaDStream<String> lines = messages.map(Tuple2::_2);

            // FIXME, smarter way to do this??
            JavaPairDStream<String, Integer> origins = lines.mapToPair(new RelevantIndexFetcher(AirConstants.ORIGIN_INDEX));
            JavaPairDStream<String, Integer> destinations = lines.mapToPair(new RelevantIndexFetcher(AirConstants.DEST_INDEX));
            JavaPairDStream<String, Integer> allRecs = origins.union(destinations);

            // Uses lambda to indicate that the values should be added (reduceByKey)
            JavaPairDStream<String, Integer> summarized = allRecs.reduceByKey((i1, i2) -> i1 + i2);

            // Store in a stateful ds
            JavaPairDStream<String, Integer> statefulMap = summarized.updateStateByKey(COMPUTE_RUNNING_SUM);

            // Puts the juicy stuff in AirportKey, the Integer is useless -- just a placeholder
            JavaPairDStream<AirportKey, Integer> airports = statefulMap.mapToPair(new PairConverter());

            // Sorted airport list
            JavaDStream<AirportKey> sortedAirports = airports.transform(new RDDSortTransformer());

            // **** Print top 10 from sorted map ***
            sortedAirports.print(10);

            // Persist! //TODO restrict to 10?
            AirHelper.persist(sortedAirports, G1Q1.class);


            jssc.start();
            jssc.awaitTermination();

        } catch (Exception e) {
            LOG.error("----- Error while running spark subscriber -------", e);
        }
    }


    /**
     * We used this transformer because sortByKey is only available here. Other (non-pair-based) options did
     * not have a built-in option to sort by keys
     */
    static class RDDSortTransformer implements Function<JavaPairRDD<AirportKey, Integer>, JavaRDD<AirportKey>> {
        @Override
        public JavaRDD<AirportKey> call(JavaPairRDD<AirportKey, Integer> unsortedRDD) throws Exception {
            return unsortedRDD.sortByKey(false).keys(); // DESC sort
        }
    }


    /**
     * Used to prepare a CustomKey (@AirportKey) based RDD out of the "raw" (String,Integer) RDD
     */
    static class PairConverter implements PairFunction<Tuple2<String, Integer>, AirportKey, Integer> {
        @Override
        public Tuple2<AirportKey, Integer> call(Tuple2<String, Integer> tuple2) throws Exception {
            return new Tuple2(new AirportKey(tuple2._1(), tuple2._2()), 0);
        }
    }

    @SuppressWarnings("unused")
    /**
     * Can be used to return a non pair-RDD
     */
    static class Transformer implements Function<Tuple2<String, Integer>, AirportKey> {
        @Override
        public AirportKey call(Tuple2<String, Integer> entry) throws Exception {
            return new AirportKey(entry._1(), entry._2());
        }
    }


    // List of incoming vals, currentVal, returnVal
    private static Function2<List<Integer>, Optional<Integer>, Optional<Integer>>
            COMPUTE_RUNNING_SUM = (nums, current) -> {
        int sum = current.orElse(0);
        for (int i : nums) {
            sum += i;
        }
        return Optional.of(sum);
    };


    static class AirportKey implements Comparable<AirportKey>, Serializable {

        private String airportCode;
        private Integer flightCount;


        public String getAirportCode() {
            return airportCode;
        }

        public Integer getFlightCount() {
            return flightCount;
        }

        public void setFlightCount(Integer flightCount) {
            this.flightCount = flightCount;
        }

        public void addFlightCount(Integer countToAdd) {
            this.flightCount += countToAdd;
        }


        public AirportKey() {
            airportCode = "";
            flightCount = 0;
        }

        public AirportKey(String airportCode, Integer flightCount) {
            this.airportCode = airportCode;
            this.flightCount = flightCount;
        }


        @Override
        public int compareTo(AirportKey o) {
            return this.flightCount.compareTo(o.getFlightCount()); // reverse sort
        }

        @Override
        public String toString() {
            return airportCode + ", " + flightCount;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof AirportKey)) return false;

            AirportKey that = (AirportKey) o;

            return getAirportCode().equals(that.getAirportCode());
        }

        @Override
        public int hashCode() {
            return getAirportCode().hashCode();
        }
    }

    public static class RelevantIndexFetcher implements PairFunction<String, String, Integer> {
        int relIndex = 0;

        @SuppressWarnings("unused")
        public RelevantIndexFetcher() {
        }

        public RelevantIndexFetcher(int index) {
            relIndex = index;
        }

        public Tuple2<String, Integer> apply(String line) {
            return new Tuple2(line.split(",")[relIndex], Integer.valueOf(1));
        }

        @Override
        public Tuple2<String, Integer> call(String s) throws Exception {
            return new Tuple2(s.split(",")[relIndex], Integer.valueOf(1));
        }
    }
}
