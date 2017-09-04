package org.pgmx.spark.g2;

import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraStreamingJavaUtil;
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

import static org.spark_project.guava.base.Preconditions.checkNotNull;

/**
 * Group 2 Q1
 */
public final class G2Q2 {

    private static final Logger LOG = Logger.getLogger(G2Q2.class);


    private G2Q2() {
    }


    public static void main(String[] args) throws Exception {

        try {
            checkNotNull(args[0], "No origin code specified, cannot continue");

            String zkHost = args.length > 1 ? args[1] : AirConstants.ZK_HOST;
            String kafkaTopic = args.length > 2 ? args[2] : AirConstants.IN_TOPIC;
            int streamJobs = args.length > 3 ? Integer.valueOf(args[3]) : AirConstants.STREAMING_JOB_COUNT;
            int fetchIntervalMs = args.length > 4 ? Integer.valueOf(args[4]) : AirConstants.FETCH_COUNT_INTERVAL;
            String kafkaOffset = args.length > 5 && args[5].equalsIgnoreCase("Y") ?
                    AirConstants.KAFKA_OFFSET_SMALLEST : AirConstants.KAFKA_OFFSET_LARGEST;
            String cassandraHost = args.length > 6 ? args[6] : AirConstants.CASSANDRA_HOST;

            SparkConf sparkConf = new SparkConf().setAppName("G2Q2");
            sparkConf.set("spark.streaming.concurrentJobs", "" + streamJobs);
            sparkConf.set("spark.cassandra.connection.host", cassandraHost);
            sparkConf.set("spark.cassandra.connection.keep_alive_ms", "" + (fetchIntervalMs + 5000));

            // Create the context with 2 seconds batch size
            JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(fetchIntervalMs));
            jssc.checkpoint(AirConstants.CHECKPOINT_DIR);

            Set<String> topicsSet = new HashSet<>(Arrays.asList(kafkaTopic.split(",")));

            Map<String, String> kafkaParams = new HashMap<>();
            kafkaParams.put("metadata.broker.list", zkHost);
            kafkaParams.put("auto.offset.reset", kafkaOffset);

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
            //JavaDStream<String> filtered = lines.filter(new AirlineFilter(args[0]));

            // Filter by origin code
            JavaDStream<String> filteredLines = lines.filter(s ->
                    StringUtils.equals(s.split(",")[AirConstants.ORIGIN_INDEX], args[0]));

            // Get pairs from these filtered lines output is DEST -> DEP_DELAY
            JavaPairDStream<String, Integer> carrierArrDelay =
                    filteredLines.mapToPair(new DestinationDepartureDelay());

            // FIXME partitioner count is hard-coded
            JavaPairDStream<String, AvgCount> avgCounts =
                    carrierArrDelay.combineByKey(createAcc, addAndCount, combine, new HashPartitioner(10),
                            false);

            // FIXME debug
            //avgCounts.print();

            // Store in a stateful ds
            JavaPairDStream<String, AvgCount> statefulMap = avgCounts.updateStateByKey(COMPUTE_RUNNING_AVG);

            // unsorted // FIXME for debug
            //.print(10);

            // Puts the juicy stuff in AirportKey, the Integer is useless -- just a placeholder
            JavaPairDStream<OriginDestDepDelayKey, Integer> airports =
                    statefulMap.mapToPair(new PairConverter(args[0]));

            // Sorted airport list
            JavaDStream<OriginDestDepDelayKey> sortedDestDepAvgs = airports.transform(new RDDSortTransformer());

            // **** Print top 10 from sorted map ***
            sortedDestDepAvgs.print(10);

            // Persist! //TODO restrict to 10?
            AirHelper.persist(sortedDestDepAvgs, G2Q2.class);
            persistInDB(sortedDestDepAvgs, G2Q2.class, sparkConf, args[0]);

            jssc.start();
            jssc.awaitTermination();

        } catch (Exception e) {
            LOG.error("----- Error while running spark subscriber -------", e);
        }
    }

    private static class AirlineFilter implements Function<String, Boolean> {
        String origin;

        public AirlineFilter(String o) {
            this.origin = o;
        }

        @Override
        public Boolean call(String s) throws Exception {
            String a[] = s.split(",");
            boolean cancelled = StringUtils.isNotEmpty(a[AirConstants.CANCELLED_INDEX])
                    && Float.valueOf(AirConstants.CANCELLED_INDEX).equals(1);
            return !cancelled && StringUtils.equals(a[AirConstants.ORIGIN_INDEX], origin);
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
    static class RDDSortTransformer implements Function<JavaPairRDD<OriginDestDepDelayKey, Integer>,
            JavaRDD<OriginDestDepDelayKey>> {
        @Override
        public JavaRDD<OriginDestDepDelayKey> call(JavaPairRDD<OriginDestDepDelayKey, Integer> unsortedRDD)
                throws Exception {
            return unsortedRDD.sortByKey().keys(); // ASC sort
        }
    }


    /**
     * Used to prepare a CustomKey (@AirportKey) based RDD out of the "raw" (String,Integer) RDD
     */
    static class PairConverter implements PairFunction<Tuple2<String, AvgCount>, OriginDestDepDelayKey, Integer> {
        String origin;

        public PairConverter(String org) {
            this.origin = org;
        }

        @Override
        public Tuple2<OriginDestDepDelayKey, Integer> call(Tuple2<String, AvgCount> tuple2) throws Exception {
            return new Tuple2(new OriginDestDepDelayKey(origin, tuple2._1(), tuple2._2().avg()), 0);
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


    public static class DestinationDepartureDelay implements PairFunction<String, String, Integer> {

        @SuppressWarnings("unused")
        public DestinationDepartureDelay() {
        }

        public Tuple2<String, Integer> apply(String s) {
            String[] arr = s.split(",");
            Integer depDelay = StringUtils.isEmpty(arr[AirConstants.DEP_DELAY_INDEX]) ?
                    0 : Float.valueOf(arr[AirConstants.DEP_DELAY_INDEX]).intValue();
            return new Tuple2(arr[AirConstants.DEST_INDEX], depDelay);
        }

        @Override
        public Tuple2<String, Integer> call(String s) throws Exception {
            String[] arr = s.split(",");
            Integer depDelay = StringUtils.isEmpty(arr[AirConstants.DEP_DELAY_INDEX]) ?
                    0 : Float.valueOf(arr[AirConstants.DEP_DELAY_INDEX]).intValue();
            return new Tuple2(arr[AirConstants.DEST_INDEX], depDelay);
        }
    }

    private static void persistInDB(JavaDStream<OriginDestDepDelayKey> javaDStream, Class clazz, SparkConf conf, String org) {
        LOG.info("- Will save in DB table: " + clazz.getSimpleName() + " -");
        String keySpace = StringUtils.lowerCase("T2");
        String tableName = StringUtils.lowerCase(clazz.getSimpleName());
        String delQuery = "DELETE FROM " + keySpace + "." + tableName + " WHERE origin='" + org + "'";

        CassandraConnector connector = CassandraConnector.apply(conf);
        try (Session session = connector.openSession()) {
            session.execute("CREATE KEYSPACE IF NOT EXISTS " + keySpace + " WITH replication = {'class': 'SimpleStrategy', " +
                    "'replication_factor': 1}");
            session.execute("CREATE TABLE IF NOT EXISTS " + keySpace + "." + tableName
                    + " (origin text, dest text, avgdepdelay double, primary key(origin, dest))");

//            javaDStream.foreachRDD(rdd -> {
//                if (rdd.count() > 0) {
//                    session.execute(delQuery);
//                }
//            });

            Map<String, String> fieldToColumnMapping = new HashMap<>();
            fieldToColumnMapping.put("origin", "origin");
            fieldToColumnMapping.put("destination", "dest");
            fieldToColumnMapping.put("avgDepDelay", "avgdepdelay");
            CassandraStreamingJavaUtil.javaFunctions(javaDStream).writerBuilder(keySpace, tableName,
                    CassandraJavaUtil.mapToRow(OriginDestDepDelayKey.class, fieldToColumnMapping)).saveToCassandra();
        }

    }
}
