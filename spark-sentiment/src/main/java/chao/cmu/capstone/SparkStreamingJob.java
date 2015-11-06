package chao.cmu.capstone;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class SparkStreamingJob {
    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: <brokers> <topic>");
            System.exit(1);
        }

        String brokers = args[0];
        String topic = args[1];

        SparkConf sparkConf = new SparkConf().setAppName("spark-sentiment");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));

        HashSet<String> topicSet = new HashSet<String>();
        topicSet.add(topic);

        HashMap<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("bootstrap.servers", brokers);
        kafkaParams.put("auto.offset.reset", "largest");

        JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
                jssc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topicSet);

        JavaDStream<Integer> lines = messages.map(new Function<Tuple2<String, String>, Integer>() {
            @Override
            public Integer call(Tuple2<String, String> tuple2) {
                return 1;
            }
        });

        JavaDStream<Long> counts = lines.count();
        counts.foreach(new Function2<JavaRDD<Long>, Time, Void>() {
            @Override
            public Void call(JavaRDD<Long> longJavaRDD, Time time) throws Exception {
                System.out.print(time);
                System.out.print(' ');
                System.out.print(longJavaRDD.collect().get(0));
                System.out.println();
                return null;
            }
        });
        jssc.start();
        jssc.awaitTermination();
    }
}
