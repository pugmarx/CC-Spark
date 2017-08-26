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

package org.pgmx.spark.tryout;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Consumes messages from one or more topics in Kafka and does wordcount.
 * <p>
 * Usage: JavaKafkaWordCount <zkQuorum> <group> <topics> <numThreads>
 * <zkQuorum> is a list of one or more zookeeper servers that make quorum
 * <group> is the name of kafka consumer group
 * <topics> is a list of one or more kafka topics to consume from
 * <numThreads> is the number of threads the kafka consumer should use
 * <p>
 * To run this example:
 * `$ bin/run-example org.apache.spark.examples.streaming.JavaKafkaWordCount zoo01,zoo02, \
 * zoo03 my-consumer-group topic1,topic2 1`
 */

public final class KafkaReaderTest {

    private static final String ZK_HOST = "localhost:2181"; // FIXME
    private static final String IN_TOPIC = "testspark"; // FIXME
    private static final String IN_GROUP = "spark_" + UUID.randomUUID(); //FIXME
    private static final String NUM_THREADS = "1";
    private static final String CHECKPOINT_DIR = "/tmp/spark_checkpoints";
    private static final String STREAMING_JOB_COUNT = "10";
    private static final int FETCH_COUNT_INTERVAL = 20000; // FIXME
    private static final String MASTER_STRING = "local[*]";

    //private static final Pattern SPACE = Pattern.compile(" ");
    private static final Pattern COMMA = Pattern.compile(",");

    private KafkaReaderTest() {
    }

    public static void main(String[] args) throws Exception {

        try {
            SparkConf sparkConf = new SparkConf().setAppName("KafkaReaderTest2").setMaster(MASTER_STRING);
            sparkConf.set("spark.streaming.concurrentJobs", STREAMING_JOB_COUNT);

            // Create the context with 2 seconds batch size
            JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(FETCH_COUNT_INTERVAL)); //FIXME duration
            jssc.checkpoint(CHECKPOINT_DIR);

            int numThreads = Integer.parseInt(NUM_THREADS);
            Map<String, Integer> topicMap = new HashMap<>();
            String[] topics = IN_TOPIC.split(",");
            for (String topic : topics) {
                topicMap.put(topic, numThreads);
            }

            JavaPairReceiverInputDStream<String, String> messages =
                    KafkaUtils.createStream(jssc, ZK_HOST, IN_GROUP, topicMap);

            // Assuming that this gets the records from the message RDD
            JavaDStream<String> lines = messages.map(Tuple2::_2);

            // FIXME, smarter way to do this??
            JavaPairDStream<String, Integer> origins = lines.mapToPair(new RelevantIndex(5));
            JavaPairDStream<String, Integer> destinations = lines.mapToPair(new RelevantIndex(6));
            JavaPairDStream<String, Integer> allRecs = origins.union(destinations);

            //JavaPairDStream<String, Integer> summarized = allRecs.reduceByKey((i1, i2) -> i1 + i2);
            JavaPairDStream<String, Integer> summarized = allRecs.reduceByKey((i1, i2) -> i1 + i2);

            summarized.foreachRDD(rdd -> {
                if (rdd.count() > 0) {
                    System.out.println("----------->" + rdd.count());
                } else {
                    System.out.println("======== NOTHING IN RDD =============");
                }
            });

            // Store in a stateful ds
            JavaPairDStream<String, Integer> statefulMap = summarized.updateStateByKey(COMPUTE_RUNNING_SUM);

            //summarized.print();
            statefulMap.print();
            jssc.start();
            jssc.awaitTermination();
        } catch (Exception e) {
            e.printStackTrace();
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
