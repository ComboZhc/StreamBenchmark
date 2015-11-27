package chao.cmu.capstone;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.metric.LoggingMetricsConsumer;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

import java.util.*;

public class WordCount {
    public static StormTopology createTopology(String zkhosts, String topic, int hint) {
        SpoutConfig config = new SpoutConfig(
                new ZkHosts(zkhosts),
                topic,
                "/txns",
                WordCount.class.getSimpleName() + Long.toString(System.currentTimeMillis()));
        config.scheme = new SchemeAsMultiScheme(new StringScheme());
        config.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
        config.metricsTimeBucketSizeInSecs = 5;
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new KafkaSpout(config), hint);
        builder.setBolt("bolt", new ProjectBolt(), hint).shuffleGrouping("spout");
        builder.setBolt("word", new WordBolt(), hint).shuffleGrouping("bolt");
        builder.setBolt("count", new CountBolt(), 1).shuffleGrouping("word");
        return builder.createTopology();
    }

    public static class ProjectBolt extends BaseBolt {
        @Override
        public void execute(Tuple tuple) {
            if (!isTickTuple(tuple)) {
                String message = tuple.getString(0);
                JSONObject object = (JSONObject) JSONValue.parse(message);
                String text = (String) object.get("text");
                collector.emit(tuple, new Values(text));
                collector.ack(tuple);
            }
        }
    }

    public static class WordBolt extends BaseBolt {
        @Override
        public void execute(Tuple tuple) {
            if (!isTickTuple(tuple)) {
                String message = tuple.getString(0);
                for (String msg : message.split("\\s+")) {
                    collector.emit(tuple, new Values(msg));
                }
                collector.ack(tuple);
            }
        }
    }

    public static class CountBolt extends BaseBolt {
        public HashMap<String, Integer> counts = new HashMap<>();
        @Override
        public void execute(Tuple tuple) {
            if (isTickTuple(tuple)) {
                for (Map.Entry<String, Integer> entry : counts.entrySet()) {
                    System.out.println(entry.getKey() + ' ' + entry.getValue());
                }
                counts.clear();
            } else {
                String message = tuple.getString(0);
                if (counts.containsKey(message)) {
                    counts.put(message, counts.get(message) + 1);
                } else {
                    counts.put(message, 1);
                }
                collector.ack(tuple);
            }
        }
    }


    public static void main(String[] args) throws Exception {
        String zkHosts = args[0];
        String topic = args[1];
        int workers = Integer.valueOf(args[2]);
        int hint = Integer.valueOf(args[3]);
        Config conf = new Config();
        conf.setNumWorkers(workers);
        conf.put(Config.TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS, 5);
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 1);
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1000);
        conf.registerMetricsConsumer(KafkaMetricsConsumer.class);
        StormSubmitter.submitTopologyWithProgressBar(
                WordCount.class.getSimpleName(),
                conf,
                createTopology(zkHosts, topic, hint));
    }
}
