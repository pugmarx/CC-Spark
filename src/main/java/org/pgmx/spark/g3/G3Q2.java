package org.pgmx.spark.g3;

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
import org.pgmx.spark.common.utils.AirHelper;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

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

            String zkHost = args.length > 4 ? args[4] : ZK_HOST;
            String kafkaTopic = args.length > 5 ? args[5] : IN_TOPIC;
            int streamJobs = args.length > 6 ? Integer.valueOf(args[6]) : STREAMING_JOB_COUNT;
            int fetchIntervalMs = args.length > 7 ? Integer.valueOf(args[7]) : FETCH_COUNT_INTERVAL;
            String kafkaOffset = args.length > 8 && args[8].equalsIgnoreCase("Y") ?
                    KAFKA_OFFSET_SMALLEST : KAFKA_OFFSET_LARGEST;
            String cassandraHost = args.length > 9 ? args[9] : CASSANDRA_HOST;

            SparkConf sparkConf = new SparkConf().setAppName("G3Q2");
            sparkConf.set("spark.streaming.concurrentJobs", "" + streamJobs);
            sparkConf.set("spark.cassandra.connection.host", cassandraHost);
            sparkConf.set("spark.cassandra.connection.keep_alive_ms", "" + (fetchIntervalMs + 5000));

            // Create the context with 2 seconds batch size
            JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(fetchIntervalMs));
            jssc.checkpoint(CHECKPOINT_DIR);

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

            // Leg1
            processLeg1(sparkConf, origin, transit, dest, startDate, lines);

            // Leg2
            processLeg2(sparkConf, origin, transit, dest, startDate, lines);

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
    private static void processLeg1(SparkConf conf, String origin, String transit, String dest, String startDate,
                                    JavaDStream<String> lines) {

        Validator validator = new Validator(startDate);
        JavaDStream<String> filteredLines = lines.filter(new Leg1Filter(origin, transit, validator));

        // Get pairs from these filtered lines output is (ORG,DEST) -> ARR_DELAY
        JavaPairDStream<FlightLegKey, Integer> carrierArrDelay
                = filteredLines.mapToPair(new OrgDestArrDelay("LEG_1", validator.getFormattedLeg1Date(), origin, transit, dest));

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
        sortedOrgDestArrAvgs.print(1);

        // Persist!
        AirHelper.persist(sortedOrgDestArrAvgs, "LEG_1", G3Q2.class);
        persistInDB(sortedOrgDestArrAvgs, G3Q2.class, conf, origin, transit, dest, startDate, "LEG_1");
    }

    //////////////////////////////////////////////////////////////////////
    //////////////////////////////// L E G 2 /////////////////////////////
    //////////////////////////////////////////////////////////////////////
    private static void processLeg2(SparkConf conf, String origin, String transit, String dest, String startDate,
                                    JavaDStream<String> lines) {

        Validator validator = new Validator(startDate);
        JavaDStream<String> filteredLines = lines.filter(new Leg2Filter(transit, dest, validator));

        // Get pairs from these filtered lines output is (ORG,DEST) -> ARR_DELAY
        JavaPairDStream<FlightLegKey, Integer> carrierArrDelay
                = filteredLines.mapToPair(new OrgDestArrDelay("LEG_2", validator.getFormattedLeg1Date(), origin, transit, dest));

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
        sortedOrgDestArrAvgs.print(1);

        // Persist!
        AirHelper.persist(sortedOrgDestArrAvgs, "LEG_2", G3Q2.class);
        persistInDB(sortedOrgDestArrAvgs, G3Q2.class, conf, origin, transit, dest, startDate, "LEG_2");
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


    public static class OrgDestArrDelay implements PairFunction<String, FlightLegKey, Integer> {
        private String flightLeg;
        private String startDate;
        private String transit;
        private String origin;
        private String dest;

        public OrgDestArrDelay(String flightLeg, String startDate, String o, String t, String d) {
            this.flightLeg = flightLeg;
            this.startDate = startDate;
            this.origin = o;
            this.transit = t;
            this.dest = d;
        }

        public Tuple2<FlightLegKey, Integer> apply(String s) {
            String[] arr = s.split(",");
            FlightLegKey key = new FlightLegKey(flightLeg, startDate, origin, transit, dest, arr[FLT_DATE_INDEX],
                    arr[UNIQUE_CARRIER_INDEX], arr[FLT_NUM_INDEX], arr[DEP_TIME_INDEX]);
            Integer arrDelay = StringUtils.isEmpty(arr[ARR_DELAY_INDEX]) ?
                    0 : Float.valueOf(arr[ARR_DELAY_INDEX]).intValue();
            return new Tuple2(key, arrDelay);
        }

        @Override
        public Tuple2<FlightLegKey, Integer> call(String s) throws Exception {
            String[] arr = s.split(",");
            FlightLegKey key = new FlightLegKey(flightLeg, startDate, origin, transit, dest, arr[FLT_DATE_INDEX],
                    arr[UNIQUE_CARRIER_INDEX], arr[FLT_NUM_INDEX], arr[DEP_TIME_INDEX]);
            Integer arrDelay = StringUtils.isEmpty(arr[ARR_DELAY_INDEX]) ? 0
                    : Float.valueOf(arr[ARR_DELAY_INDEX]).intValue();
            return new Tuple2(key, arrDelay);
        }
    }

    private static void persistInDB(JavaDStream<FlightLegKey> javaDStream, Class clazz, SparkConf conf, String org, String transit,
                                    String dest, String startDate, String leg) {
        LOG.info("- Will save in DB table: " + clazz.getSimpleName() + " -");
        String keySpace = StringUtils.lowerCase("T2");
        String tableName = StringUtils.lowerCase(clazz.getSimpleName());
        CassandraConnector connector = CassandraConnector.apply(conf);
//        String delQuery = "DELETE FROM " + keySpace + "." + tableName + " WHERE origin='" + org + "' and transit='"
//                + transit + "' and dest='" + dest + "' and startdate='"
//                + new Validator(startDate).getFormattedLeg1Date() + "' and leg='" + leg + "'";

        try (Session session = connector.openSession()) {
            session.execute("CREATE KEYSPACE IF NOT EXISTS " + keySpace + " WITH replication = {'class': 'SimpleStrategy', " +
                    "'replication_factor': 1}");
            session.execute("CREATE TABLE IF NOT EXISTS " + keySpace + "." + tableName
                    + " (leg text, origin text, transit text, dest text, startdate timestamp, fltdate timestamp, " +
                    "airline text, fltnum text, deptime text, avgdelay double, " +
                    "primary key(origin, transit, dest, startdate, leg, avgdelay, airline, fltnum))");

//            javaDStream.foreachRDD(rdd -> {
//                if (rdd.count() > 0) {
//                    session.execute(delQuery);
//                }
//            });

            Map<String, String> fieldToColumnMapping = new HashMap<>();
            fieldToColumnMapping.put("flightLeg", "leg");
            fieldToColumnMapping.put("origin", "origin");
            fieldToColumnMapping.put("transit", "transit");
            fieldToColumnMapping.put("destination", "dest");
            fieldToColumnMapping.put("airline", "airline");
            fieldToColumnMapping.put("startDate", "startdate");
            fieldToColumnMapping.put("fltDate", "fltdate");
            fieldToColumnMapping.put("fltNum", "fltnum");
            fieldToColumnMapping.put("depTime", "deptime");
            fieldToColumnMapping.put("avg", "avgdelay");

            CassandraStreamingJavaUtil.javaFunctions(javaDStream).writerBuilder(keySpace, tableName,
                    CassandraJavaUtil.mapToRow(FlightLegKey.class, fieldToColumnMapping)).saveToCassandra();
        }

    }
}
