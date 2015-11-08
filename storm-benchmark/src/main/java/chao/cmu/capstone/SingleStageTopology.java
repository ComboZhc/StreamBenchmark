package chao.cmu.capstone;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.metric.LoggingMetricsConsumer;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

public class SingleStageTopology {
    public static StormTopology createTopology(String zkhosts, String topic, int hint, String func) {
        SpoutConfig config = new SpoutConfig(
                new ZkHosts(zkhosts),
                topic,
                "/txns",
                SingleStageTopology.class.getSimpleName() + Long.toString(System.currentTimeMillis()));
        config.scheme = new SchemeAsMultiScheme(new StringScheme());
        config.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
        config.metricsTimeBucketSizeInSecs = 5;
        BaseRichBolt bolt;
        bolt = new IdBolt();
        if (func.startsWith("p"))
            bolt = new ProjectBolt();
        if (func.startsWith("s"))
            bolt = new SampleBolt();
        if (func.startsWith("f"))
            bolt = new FilterBolt();
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new KafkaSpout(config));
        builder.setBolt("bolt", bolt, hint).shuffleGrouping("spout");
        builder.setBolt("void", new BaseBolt(), hint).shuffleGrouping("bolt");
        return builder.createTopology();
    }


    public static class IdBolt extends BaseBolt {
        @Override
        public void execute(Tuple tuple) {
            collector.emit(tuple, new Values(tuple.getString(0)));
            super.execute(tuple);
        }
    }

    public static class ProjectBolt extends BaseBolt {
        @Override
        public void execute(Tuple tuple) {
            String message = tuple.getString(0);
            JSONObject object = (JSONObject) JSONValue.parse(message);
            String text = (String)object.get("text");
            collector.emit(tuple, new Values(text));
            super.execute(tuple);
        }
    }

    public static class SampleBolt extends BaseBolt {
        int sampleCount = 0;
        @Override
        public void execute(Tuple tuple) {
            if (sampleCount % 10 == 0) {
                collector.emit(tuple, new Values(tuple.getString(0)));
            }
            sampleCount++;
            super.execute(tuple);
        }
    }

    public static class FilterBolt extends BaseBolt {
        @Override
        public void execute(Tuple tuple) {
            String message = tuple.getString(0);
            JSONObject object = (JSONObject) JSONValue.parse(message);
            if ("en".equals(object.get("lang"))) {
                collector.emit(tuple, new Values(tuple.getString(0)));
            }
            super.execute(tuple);
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
