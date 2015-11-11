package chao.cmu.capstone;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.metric.LoggingMetricsConsumer;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.vdurmont.emoji.Emoji;
import com.vdurmont.emoji.EmojiManager;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Count;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class TridentEmojiCount {
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

    public static StormTopology createTopology(String zkhosts, String topic, int hint) {
        TridentKafkaConfig config = new TridentKafkaConfig(
                new ZkHosts(zkhosts),
                topic,
                TridentSingleStage.class.getSimpleName() + Long.toString(System.currentTimeMillis()));
        config.scheme = new SchemeAsMultiScheme(new StringScheme());
        config.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
        config.metricsTimeBucketSizeInSecs = 5;
        TransactionalTridentKafkaSpout spout = new TransactionalTridentKafkaSpout(config);

        TridentTopology topology = new TridentTopology();
        Stream s = topology.newStream("str", spout).parallelismHint(hint).name("spout")
                .each(new Fields("str"), new ProjectFunction(), new Fields("output")).parallelismHint(hint).name("bolt")
                .each(new Fields("output"), new EmojiFunction(), new Fields("emojis")).parallelismHint(hint).name("bolt2")
                .groupBy(new Fields("emojis"))
                .aggregate(new Fields("emojis"), new Count(), new Fields("count")).parallelismHint(hint).name("agg")
                .each(new Fields("emojis", "count"), new PrintFunction(), new Fields()).parallelismHint(1);
        return topology.build();
    }

    public static class ProjectFunction extends BaseCountFunction {
        @Override
        public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
            String message = tridentTuple.getString(0);
            JSONObject object = (JSONObject) JSONValue.parse(message);
            String text = (String)object.get("text");
            tridentCollector.emit(new Values(text));
            super.execute(tridentTuple, tridentCollector);
        }
    }

    public static class EmojiFunction extends BaseFunction {
        @Override
        public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
            String message = tridentTuple.getString(0);
            for (String emoji : getEmojiAliases(message))
                tridentCollector.emit(new Values(emoji));
        }
    }

    public static class PrintFunction extends BaseFunction {
        @Override
        public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
            System.out.println(tridentTuple.getString(0) + ' ' + tridentTuple.getLong(1));
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
        conf.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, 1000);
        conf.registerMetricsConsumer(LoggingMetricsConsumer.class);
        StormSubmitter.submitTopologyWithProgressBar(
                TridentEmojiCount.class.getSimpleName(),
                conf,
                createTopology(zkHosts, topic, hint));
    }
}
