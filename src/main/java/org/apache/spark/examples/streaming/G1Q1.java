/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.examples.streaming;

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
import scala.Tuple2;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Group 1 Q1
 */
public final class G1Q1 {

    private static final String ZK_HOST = "localhost:2181"; // FIXME
    private static final String IN_TOPIC = "testspark"; // FIXME
    private static final String IN_GROUP = "spark_" + UUID.randomUUID(); //FIXME
    private static final String NUM_THREADS = "1";
    private static final String CHECKPOINT_DIR = "/tmp/spark_checkpoints";
    private static final String STREAMING_JOB_COUNT = "10";
    private static final int FETCH_COUNT_INTERVAL = 20000; // FIXME
    private static final String MASTER_STRING = "local[*]"; // FIXME

    private static final Logger LOG = Logger.getLogger(G1Q1.class);

    private G1Q1() {
    }

    public static void main(String[] args) throws Exception {

        try {
            SparkConf sparkConf = new SparkConf().setAppName("KafkaReaderTest2").setMaster(MASTER_STRING);
            sparkConf.set("spark.streaming.concurrentJobs", STREAMING_JOB_COUNT);

            // Create the context with 2 seconds batch size
            JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(FETCH_COUNT_INTERVAL));
            jssc.checkpoint(CHECKPOINT_DIR);

            int numThreads = Integer.parseInt(NUM_THREADS);
            Map<String, Integer> topicMap = new HashMap<>();
            String[] topics = IN_TOPIC.split(",");
            for (String topic : topics) {
                topicMap.put(topic, numThreads);
            }

            JavaPairReceiverInputDStream<String, String> messages =
                    KafkaUtils.createStream(jssc, ZK_HOST, IN_GROUP, topicMap);

            // Pick the messages
            JavaDStream<String> lines = messages.map(Tuple2::_2);

            // FIXME, smarter way to do this??
            JavaPairDStream<String, Integer> origins = lines.mapToPair(new RelevantIndex(5));
            JavaPairDStream<String, Integer> destinations = lines.mapToPair(new RelevantIndex(6));
            JavaPairDStream<String, Integer> allRecs = origins.union(destinations);

            JavaPairDStream<String, Integer> summarized = allRecs.reduceByKey((i1, i2) -> i1 + i2);

            // Store in a stateful ds
            JavaPairDStream<String, Integer> statefulMap = summarized.updateStateByKey(COMPUTE_RUNNING_SUM);

            // FIXME
            //JavaDStream<AirportKey> airports = statefulMap.map(new Transformer());
            JavaPairDStream<AirportKey, Integer> airports = statefulMap.mapToPair(new PairConverter());

            JavaDStream<AirportKey> sortedAirports = airports.transform(new RDDSortTransformer());

            sortedAirports.print(10);
            //statefulMap.print();

            jssc.start();
            jssc.awaitTermination();
        } catch (Exception e) {
            LOG.error("----- Error while running spark subscriber -------", e);
        }
    }

    static class RDDSortTransformer implements Function<JavaPairRDD<AirportKey, Integer>, JavaRDD<AirportKey>> {
        @Override
        public JavaRDD<AirportKey> call(JavaPairRDD<AirportKey, Integer> unsortedRDD) throws Exception {
            return unsortedRDD.sortByKey(false).keys();
        }
    }

//    static class RDDTransformer implements Function<JavaPairRDD<String, Integer>, JavaRDD<AirportKey>>{
//
//        @Override
//        public JavaRDD<AirportKey> call(JavaPairRDD<String, Integer> pairRDD) throws Exception {
//            return pairRDD.sor
//        }
//    }

    static class PairConverter implements PairFunction<Tuple2<String, Integer>, AirportKey, Integer> {
        @Override
        public Tuple2<AirportKey, Integer> call(Tuple2<String, Integer> tuple2) throws Exception {
            return new Tuple2(new AirportKey(tuple2._1(), tuple2._2()), 0);
        }
    }

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

//
//        @Override
//        public int compare(Object that) {
//            return (-1) * this.flightCount.compareTo(((AirportKey) that).getFlightCount());
//        }

        @Override
        public int compareTo(AirportKey o) {
            return this.flightCount.compareTo(o.getFlightCount()); // reverse sort
        }

        @Override
        public String toString() {
            return "airportCode='" + airportCode + '\'' +
                    ", flightCount=" + flightCount;
        }


//        @Override
//        public int compare(AirportKey o1, AirportKey o2) {
//            return -1 * o1.getFlightCount().compareTo(o2.getFlightCount());
//        }

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

    static class RelevantIndex implements PairFunction<String, String, Integer> {
        int relIndex = 0;

        @SuppressWarnings("unused")
        public RelevantIndex() {
        }

        public RelevantIndex(int index) {
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
