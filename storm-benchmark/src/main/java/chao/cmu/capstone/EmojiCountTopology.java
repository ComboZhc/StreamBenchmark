package chao.cmu.capstone;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.metric.LoggingMetricsConsumer;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.vdurmont.emoji.Emoji;
import com.vdurmont.emoji.EmojiManager;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

import java.util.*;

public class EmojiCountTopology {
    public static StormTopology createTopology(String zkhosts, String topic, int hint, String func) {
        SpoutConfig config = new SpoutConfig(
                new ZkHosts(zkhosts),
                topic,
                "/txns",
                SingleStageTopology.class.getSimpleName() + Long.toString(System.currentTimeMillis()));
        config.scheme = new SchemeAsMultiScheme(new StringScheme());
        config.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
        config.metricsTimeBucketSizeInSecs = 5;
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new KafkaSpout(config));
        builder.setBolt("bolt", new ProjectBolt(), hint).shuffleGrouping("spout");
        builder.setBolt("emoji", new EmojiBolt(), hint).shuffleGrouping("bolt");
        builder.setBolt("count", new CountBolt(), 1).shuffleGrouping("emoji");
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

    public static List<String> getEmojiAliases(String str) {
        List<String> aliases = new ArrayList<>();
        Collection<Emoji> emojis = EmojiManager.getAll();
        for (Emoji emoji : emojis) {
            int pos = str.indexOf(emoji.getUnicode());
            while (pos >= 0) {
                aliases.add(emoji.getAliases().get(0));
                pos = str.indexOf(emoji.getUnicode(), pos + emoji.getUnicode().length());
            }
        }
        return aliases;
    }

    public static class EmojiBolt extends BaseBolt {
        @Override
        public void execute(Tuple tuple) {
            if (!isTickTuple(tuple)) {
                String message = tuple.getString(0);
                for (String alias : getEmojiAliases(message)) {
                    collector.emit(tuple, new Values(alias));
                }
                collector.ack(tuple);
            }
        }
    }

    public static class CountBolt extends BaseBolt {
        public HashMap<String, Integer> counts;
        @Override
        public void execute(Tuple tuple) {
            if (isTickTuple(tuple)) {
                counts.clear();
                for (Map.Entry<String, Integer> entry : counts.entrySet()) {
                    System.out.print(entry.getKey());
                    System.out.print(' ');
                    System.out.println(entry.getValue());
                }
            } else {
                String message = tuple.getString(0);
                if (counts.containsKey(message)) {
                    counts.put(message, 1);
                } else {
                    counts.put(message, counts.get(message) + 1);
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
        String func = args[4];
        Config conf = new Config();
        conf.setNumWorkers(workers);
        conf.put(Config.TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS, 5);
        conf.registerMetricsConsumer(LoggingMetricsConsumer.class);
        StormSubmitter.submitTopologyWithProgressBar(
                SingleStageTopology.class.getSimpleName(),
                conf,
                createTopology(zkHosts, topic, hint, func));
    }
}
