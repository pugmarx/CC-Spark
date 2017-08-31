package org.pgmx.spark.g1;

import kafka.serializer.StringDecoder;
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
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.pgmx.spark.common.utils.AirConstants;
import org.pgmx.spark.common.utils.AirHelper;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

/**
 * Group 1 Q2
 */
public final class G1Q2 {

    private static final Logger LOG = Logger.getLogger(G1Q2.class);

    private G1Q2() {
    }


    public static void main(String[] args) throws Exception {

        try {

            String zkHost = args.length > 0 ? args[0] : AirConstants.ZK_HOST;
            String kafkaTopic = args.length > 1 ? args[1] : AirConstants.IN_TOPIC;
            int streamJobs = args.length > 2 ? Integer.valueOf(args[2]) : AirConstants.STREAMING_JOB_COUNT;
            int fetchIntervalMs = args.length > 3 ? Integer.valueOf(args[3]) : AirConstants.FETCH_COUNT_INTERVAL;
            String kafkaOffset = args.length > 4 && args[4].equalsIgnoreCase("Y") ?
                    AirConstants.KAFKA_OFFSET_SMALLEST : AirConstants.KAFKA_OFFSET_LARGEST;

            SparkConf sparkConf = new SparkConf().setAppName("G1Q2");
            sparkConf.set("spark.streaming.concurrentJobs", "" + streamJobs);

            // Create the context with 2 seconds batch size
            JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(fetchIntervalMs));
            jssc.checkpoint(AirConstants.CHECKPOINT_DIR);

            Set<String> topicsSet = new HashSet<>(Arrays.asList(kafkaTopic.split(",")));

            Map<String, String> kafkaParams = new HashMap<>();
            kafkaParams.put("metadata.broker.list", zkHost);
            kafkaParams.put("auto.offset.reset", kafkaOffset);

            // JavaPairReceiverInputDStream<String, String> messages =
            //         KafkaUtils.createStream(jssc, zkHostOrBrokers, consGroup, topicMap);

            // Need to pass kafkaParams
            JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
                    jssc,
                    String.class,
                    String.class,
                    StringDecoder.class,
                    StringDecoder.class,
                    kafkaParams,
                    topicsSet
            );

            // Pick the messages
            JavaDStream<String> lines = messages.map(Tuple2::_2);

            // Remove cancelled flights
            JavaDStream<String> filteredLines = lines.filter(new RemoveCancelledFilter());

            // Convert to paired RDD
            JavaPairDStream<String, Integer> carrierArrDelay =
                    filteredLines.mapToPair(new CarrierArrivalDelay(AirConstants.UNIQUE_CARRIER_INDEX));


            // FIXME see if combineByKey can be used for G1Q1 as it has a mapSideCombine option
            // FIXME partitioner count is hard-coded
            JavaPairDStream<String, AvgCount> avgCounts =
                    carrierArrDelay.combineByKey(createAcc, addAndCount, combine, new HashPartitioner(40), false);

            // FIXME debug
            // avgCounts.print();

            // Store in a stateful ds
            JavaPairDStream<String, AvgCount> statefulMap = avgCounts.updateStateByKey(COMPUTE_RUNNING_AVG);

            // unsorted
            // statefulMap.print(10);

            // Puts the juicy stuff in AirportKey, the Integer is useless -- just a placeholder
            JavaPairDStream<FlightAvgArrivalKey, Integer> airports = statefulMap.mapToPair(new PairConverter());

            // Sorted airport list
            JavaDStream<FlightAvgArrivalKey> sortedFlightAvgs = airports.transform(new RDDSortTransformer());

            // **** Print top 10 from sorted map ***
            sortedFlightAvgs.print(10);

            // Persist! //TODO restrict to 10?
            AirHelper.persist(sortedFlightAvgs, G1Q2.class);

            jssc.start();
            jssc.awaitTermination();

        } catch (Exception e) {
            LOG.error("----- Error while running spark subscriber -------", e);
        }
    }

    private static class RemoveCancelledFilter implements Function<String, Boolean> {
        @Override
        public Boolean call(String s) throws Exception {
            String c = s.split(",")[AirConstants.CANCELLED_INDEX];
            boolean cancelled = StringUtils.isNotEmpty(c) && Float.valueOf(c).equals(1);
            return !cancelled;
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

    static Function<Integer, AvgCount> createAcc = x -> (new AvgCount(x, 1));

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
    static class RDDSortTransformer implements Function<JavaPairRDD<FlightAvgArrivalKey, Integer>,
            JavaRDD<FlightAvgArrivalKey>> {
        @Override
        public JavaRDD<FlightAvgArrivalKey> call(JavaPairRDD<FlightAvgArrivalKey, Integer> unsortedRDD)
                throws Exception {
            return unsortedRDD.sortByKey().keys(); // ASC sort
        }
    }


    /**
     * Used to prepare a CustomKey (@AirportKey) based RDD out of the "raw" (String,Integer) RDD
     */
    static class PairConverter implements PairFunction<Tuple2<String, AvgCount>, FlightAvgArrivalKey, Integer> {
        @Override
        public Tuple2<FlightAvgArrivalKey, Integer> call(Tuple2<String, AvgCount> tuple2) throws Exception {
            //return new Tuple2(new FlightAvgArrivalKey(tuple2._1(), tuple2._2()), 0);
            return new Tuple2(new FlightAvgArrivalKey(tuple2._1(), tuple2._2().avg()), 0);
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


    static class FlightAvgArrivalKey implements Comparable<FlightAvgArrivalKey>, Serializable {
        private String fltCode;
        private Float avg;


        public String getFltCode() {
            return fltCode;
        }

        public FlightAvgArrivalKey(String airportCode, Float avgArrivalDelay) {
            this.fltCode = airportCode;
            this.avg = avgArrivalDelay;
        }

        @Override
        public int compareTo(FlightAvgArrivalKey o) {
            return this.avg.compareTo(o.avg); // reverse sort
        }

        @Override
        public String toString() {
            return fltCode + "," + avg;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof FlightAvgArrivalKey)) return false;

            FlightAvgArrivalKey that = (FlightAvgArrivalKey) o;

            return getFltCode().equals(that.getFltCode());
        }

        @Override
        public int hashCode() {
            return getFltCode().hashCode();
        }
    }

    public static class CarrierArrivalDelay implements PairFunction<String, String, Integer> {
        int relIndex = 0;

        @SuppressWarnings("unused")
        public CarrierArrivalDelay() {
        }

        public CarrierArrivalDelay(int index) {
            relIndex = index;
        }

        public Tuple2<String, Integer> apply(String line) {
            String[] arr = line.split(",");
            Integer arrDelay = StringUtils.isEmpty(arr[AirConstants.ARR_DELAY_INDEX]) ?
                    0 : Float.valueOf(arr[AirConstants.ARR_DELAY_INDEX]).intValue();
            return new Tuple2(arr[relIndex], arrDelay);
        }

        @Override
        public Tuple2<String, Integer> call(String s) throws Exception {
            String[] arr = s.split(",");
            Integer arrDelay = StringUtils.isEmpty(arr[AirConstants.ARR_DELAY_INDEX]) ?
                    0 : Float.valueOf(arr[AirConstants.ARR_DELAY_INDEX]).intValue();
            return new Tuple2(arr[relIndex], arrDelay);
        }
    }
}
