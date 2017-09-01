package org.pgmx.spark.g1;

import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraStreamingJavaUtil;
import kafka.serializer.StringDecoder;
import org.apache.commons.lang.StringUtils;
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
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.pgmx.spark.common.utils.AirConstants;
import org.pgmx.spark.common.utils.AirHelper;
import scala.Tuple2;

import java.util.*;

/**
 * Group 1 Q1
 */
public final class G1Q1 {

    private static final Logger LOG = Logger.getLogger(G1Q1.class);

    private G1Q1() {
    }

    public static void main(String[] args) throws Exception {

        try {
            String zkHostOrBrokers = args.length > 0 ? args[0] : AirConstants.ZK_HOST;
            String kafkaTopic = args.length > 1 ? args[1] : AirConstants.IN_TOPIC;
            //String consGroup = args.length > 2 ? args[2] : AirConstants.CONSUMER_GROUP;
            //int fetcherThreads = args.length > 3 ? Integer.valueOf(args[3]) : AirConstants.NUM_THREADS;
            int streamJobs = args.length > 2 ? Integer.valueOf(args[2]) : AirConstants.STREAMING_JOB_COUNT;
            int fetchIntervalMs = args.length > 3 ? Integer.valueOf(args[3]) : AirConstants.FETCH_COUNT_INTERVAL;
            String kafkaOffset = args.length > 4 && args[4].equalsIgnoreCase("Y") ?
                    AirConstants.KAFKA_OFFSET_SMALLEST : AirConstants.KAFKA_OFFSET_LARGEST;
            String cassandraHost = args.length > 5 ? args[5] : AirConstants.CASSANDRA_HOST;

            SparkConf sparkConf = new SparkConf().setAppName("G1Q1");
            sparkConf.set("spark.streaming.concurrentJobs", "" + streamJobs);
            sparkConf.set("spark.cassandra.connection.host", cassandraHost);
            sparkConf.set("spark.cassandra.connection.keep_alive_ms", "" + (fetchIntervalMs + 5000));
            //.set("spark.cassandra.auth.username", "cassandra")
            //.set("spark.cassandra.auth.password", "cassandra")


            // Create the context with 2 seconds batch size
            JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(fetchIntervalMs));
            jssc.checkpoint(AirConstants.CHECKPOINT_DIR);

            Set<String> topicsSet = new HashSet<>(Arrays.asList(kafkaTopic.split(",")));

            Map<String, String> kafkaParams = new HashMap<>();
            kafkaParams.put("metadata.broker.list", zkHostOrBrokers);
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

            JavaPairDStream<String, Integer> origins = lines.mapToPair(new RelevantIndexFetcher(AirConstants.ORIGIN_INDEX));

            JavaPairDStream<String, Integer> destinations = lines.mapToPair(new RelevantIndexFetcher(AirConstants.DEST_INDEX));
            JavaPairDStream<String, Integer> allRecs = origins.union(destinations);

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
            //persistInDB(sortedAirports, G1Q1.class, sparkConf);

            jssc.start();
            jssc.awaitTermination();

        } catch (Exception e) {
            LOG.error("----- Error while running spark subscriber -------", e);
        }
    }

    static Function<Integer, Integer> create = x -> 1;
    static Function2<Integer, Integer, Integer> add = (a, x) -> a + x;
    static Function2<Integer, Integer, Integer> combine = (a, b) -> a + b;


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

    private static void persistInDB(JavaDStream<AirportKey> javaDStream, Class clazz, SparkConf conf) {
        LOG.info("- Will save in DB table: " + clazz.getSimpleName() + " -");
        String keySpace = StringUtils.lowerCase("T2");
        String tableName = StringUtils.lowerCase(clazz.getSimpleName());

        CassandraConnector connector = CassandraConnector.apply(conf);
        try (Session session = connector.openSession()) {
            session.execute("CREATE KEYSPACE IF NOT EXISTS " + keySpace + " WITH replication = {'class': 'SimpleStrategy', " +
                    "'replication_factor': 1}");
            session.execute("CREATE TABLE IF NOT EXISTS " + keySpace + "." + tableName
                    + " (airportcode text, flightcount double, primary key(airportcode))");

            Map<String, String> fieldToColumnMapping = new HashMap<>();
            fieldToColumnMapping.put("airportCode", "airportcode");
            fieldToColumnMapping.put("flightCount", "flightcount");
            CassandraStreamingJavaUtil.javaFunctions(javaDStream).writerBuilder(keySpace, tableName,
                    CassandraJavaUtil.mapToRow(AirportKey.class, fieldToColumnMapping)).saveToCassandra();
        }
    }
}
