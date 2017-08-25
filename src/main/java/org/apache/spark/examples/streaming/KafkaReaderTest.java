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

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;
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

    private static final String ZK_HOST = "localhost:2181";
    private static final String IN_TOPIC = "testclean";
    private static final String IN_GROUP = "spark_" + UUID.randomUUID(); //FIXME
    private static final String NUM_THREADS = "1";

    //private static final Pattern SPACE = Pattern.compile(" ");
    private static final Pattern COMMA = Pattern.compile(",");


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


    private KafkaReaderTest() {
    }


    public static void main(String[] args) throws Exception {

        try {
            SparkConf sparkConf = new SparkConf().setAppName("KafkaReaderTest2").setMaster("local[*]");
            sparkConf.set("spark.streaming.concurrentJobs", "30");

            // Create the context with 2 seconds batch size
            JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(20000)); //FIXME duration

            Map<String, String> kafkaParams = new HashMap<>();
            kafkaParams.put("metadata.broker.list", "localhost:9092");

            Set<String> topics = new HashSet();
            //topics.add("testclean");
            topics.add("testspark"); //FIXME

            JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(jssc, String.class,
                    String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);

            // Assuming that this gets the records from the message RDD
            JavaDStream<String> lines = messages.map(Tuple2::_2);

            // FIXME, smarter way to do this??
            JavaPairDStream<String, Integer> origins = lines.mapToPair(new RelevantIndex(5));
            JavaPairDStream<String, Integer> destinations = lines.mapToPair(new RelevantIndex(6));
            JavaPairDStream<String, Integer> allRecs = origins.union(destinations);

            JavaPairDStream<String, Integer> summarized = allRecs.reduceByKey((i1, i2) -> i1 + i2);

            //JavaPairDStream<String, Integer> wordCounts = flights.mapToPair(s -> new Tuple2<>(s, 1))
            //         .reduceByKey((i1, i2) -> i1 + i2);


            summarized.foreachRDD(rdd -> {
                if (rdd.count() > 0) {
                    System.out.println("----------->" + rdd.count());
                } else {
                    System.out.println("======== NOTHING IN RDD =============");
                }
            });
            summarized.print();
            jssc.start();
            jssc.awaitTermination();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //FIXME USING OTHER MAIN
    public static void main1(String[] args) throws Exception {
        if (args.length < 4) {
            System.out.println("-- Choosing defaults --");
            //System.err.println("Usage: JavaKafkaWordCount <zkQuorum> <group> <topics> <numThreads>");
            //System.exit(1);
        }


        //StreamingExamples.setStreamingLogLevels();
        SparkConf sparkConf = new SparkConf().setAppName("KafkaReaderTest");

        // Create the context with 2 seconds batch size
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000));

        int numThreads = Integer.parseInt(NUM_THREADS);
        Map<String, Integer> topicMap = new HashMap<>();
        String[] topics = IN_TOPIC.split(",");
        for (String topic : topics) {
            topicMap.put(topic, numThreads);
        }

        JavaPairReceiverInputDStream<String, String> messages =
                KafkaUtils.createStream(jssc, ZK_HOST, IN_GROUP, topicMap);


        JavaDStream<String> lines = messages.map(Tuple2::_2);

        JavaDStream<String> fields = lines.flatMap(x -> Arrays.asList(COMMA.split(x)).iterator());


        JavaPairDStream<String, Integer> wordCounts = fields.mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey((i1, i2) -> i1 + i2);


        System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
        wordCounts.print();
        wordCounts.foreachRDD(rdd -> {
            if (!rdd.isEmpty()) {
                rdd.coalesce(1).saveAsTextFile("/Users/Hobbes/NaqiGDrive/CouldComputingSpecialization/Course6/idea-spark/cc-spark/output.txt");
            }
        });

        System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");

        jssc.start();
        jssc.awaitTermination();
    }
}
