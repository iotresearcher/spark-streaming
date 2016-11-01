package com.iotresearcher.spark.streaming.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.catalyst.plans.logical.Except;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SparkStreamingThermoStatAnalyzer {

    private static final Log LOGGER = LogFactory.getLog(SparkStreamingThermoStatAnalyzer.class);

    // Stats will be computed for the last window length of time.
    private static final Duration WINDOW_LENGTH = new Duration(30 * 1000);

    // Stats will be computed every slide interval time.
    private static final Duration SLIDE_INTERVAL = new Duration(10 * 1000);

    public static void main(String[] args) {
        // Set application name
        String appName = "Spark Streaming Kafka Thermo Stat Analyzer";

        // Create a Spark Context.
        SparkConf conf = new SparkConf()
                .setAppName(appName)
                .setMaster("local[*]")
                .set("spark.executor.memory", "1g");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // This sets the update window to be every 10 seconds.
        JavaStreamingContext jssc = new JavaStreamingContext(sc, SLIDE_INTERVAL);

        String zkQuorum = "localhost:2181";
        String group = "iotresearcher-spark-streaming";
        String strTopics = "LK.Shazin.Home.Hall.ThermoStat";
        int numThreads = 2;

        Map<String, Integer> topicMap = new HashMap<String, Integer>();
        String[] topics = strTopics.split(",");
        for (String topic: topics) {
            topicMap.put(topic, numThreads);
        }

        JavaPairReceiverInputDStream<String, String> thermoStatDataStream =
                KafkaUtils.createStream(jssc, zkQuorum, group, topicMap);

        LOGGER.debug("Received DStream connecting to zookeeper " + zkQuorum + " group " + group + " topics" +
                topicMap);
        LOGGER.debug("Thermo Stat Stream: "+ thermoStatDataStream);

        ObjectMapper objectMapper = new ObjectMapper();

        JavaDStream<ThermoStatReading> thermoStatDStream = thermoStatDataStream.map(
                new Function<Tuple2<String, String>, ThermoStatReading>() {
                    public ThermoStatReading call(Tuple2<String, String> message) {
                        String messageJson = message._2();
                        try {
                            Map<String, String> messageMap = objectMapper.readValue(messageJson, Map.class);
                            Map<String, Object> payloadMap = objectMapper.readValue(messageMap.get("payload"), Map.class);
                            return new ThermoStatReading((Integer) payloadMap.get("temperature"), (Integer) payloadMap.get("humidity"));
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        return null;
                    }
                }
        );
        thermoStatDStream.print();

        JavaDStream<ThermoStatReading> windowDStream = thermoStatDStream.window(
                WINDOW_LENGTH, SLIDE_INTERVAL);

        windowDStream.foreachRDD(new Function<JavaRDD<ThermoStatReading>, Void>() {
            @Override
            public Void call(JavaRDD<ThermoStatReading> accessLogs) {
                if (accessLogs.count() == 0) {
                    LOGGER.debug("No Thermo Stats in this time interval");
                    return null;
                }

                JavaRDD<Integer> temperatures = accessLogs.map(thermoStatReading -> thermoStatReading.getTemperature());
                Integer minTemp = temperatures.min(Functions.INTEGER_NATURAL_ORDER_COMPARATOR);
                Integer maxTemp = temperatures.max(Functions.INTEGER_NATURAL_ORDER_COMPARATOR);
                Long avgTemp = temperatures.reduce(Functions.INTEGER_SUM_REDUCER) / temperatures.count();

                LOGGER.debug("Temperature : Min = "+minTemp+", Max = "+maxTemp+", Average = "+avgTemp);

                JavaRDD<Integer> humidities = accessLogs.map(thermoStatReading -> thermoStatReading.getHumidity());
                Integer minHumi = humidities.min(Functions.INTEGER_NATURAL_ORDER_COMPARATOR);
                Integer maxHumi = humidities.max(Functions.INTEGER_NATURAL_ORDER_COMPARATOR);
                Long avgHumi = humidities.reduce(Functions.INTEGER_SUM_REDUCER) / humidities.count();

                LOGGER.debug("Humidity : Min = "+minHumi+", Max = "+maxHumi+", Average = "+avgHumi);

                return null;
            }
        });

        // Start the streaming server.
        jssc.start(); // Start the computation
        jssc.awaitTermination(); // Wait for the computation to terminate
    }

}
