package shakeandquake;

import storm.trident.operation.BaseAggregator;
import storm.trident.operation.builtin.*;
import storm.trident.testing.FixedBatchSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;
import storm.trident.Stream;
import storm.kafka.trident.TridentKafkaConfig;
import org.hackreduce.storm.example.common.Common;
import backtype.storm.spout.SchemeAsMultiScheme;
import storm.kafka.StringScheme;
import static org.hackreduce.storm.HackReduceStormSubmitter.teamPrefix;
import storm.kafka.trident.TransactionalTridentKafkaSpout;


public class TridentQuake {
    public static class Split extends BaseFunction {
        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            String sentence = tuple.getString(0);
            for(String word: sentence.split(" ")) {
                collector.emit(new Values(word));                
            }
        }
    }
    public static class Waffle extends BaseFunction {
        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            String sentence = tuple.getString(0);
            for(String word: sentence.split(" ")) {
                collector.emit(new Values(new Boolean(true)));                
                collector.emit(new Values(new Boolean(false)));                
            }
        }
    }

    static class W_State {
        int words_quake = 0;
        int words_base = 0;
        int tweets_quake = 0;
        int tweets_base = 0;
    }

    public static class P_Word extends BaseAggregator<W_State> {
            /** expect: (Boolean near, Integer word-count, Integer tweet-count)
             *  emit: (Float p_word_base, Float p_word_quake, Float p_quake)
             */

            public W_State init(Object batchId, TridentCollector collector) {
                    return new W_State();
            }
            public void aggregate(W_State state, TridentTuple tuple, TridentCollector collector) {
                    Boolean is_quake = tuple.getBoolean(0);
                    if(is_quake) {
                            state.words_quake = tuple.getInteger(1);
                            state.tweets_quake = tuple.getInteger(2);
                    } else {
                            state.words_base = tuple.getInteger(1);
                            state.tweets_base = tuple.getInteger(2);
                    }
            }
            public void complete(W_State state, TridentCollector collector) {
                    Float p_word_base = new Float((state.words_base)/state.tweets_base);
                    Float p_word_quake = new Float((state.words_quake)/state.tweets_quake);
                    Float p_quake = new Float((state.tweets_quake) / (state.tweets_base + state.tweets_quake));
                    collector.emit(new Values(p_word_base, p_word_quake, p_quake));
            }
    }

    static class Q_State {
        float p_tweet_given_base = 1.0f;
        float p_tweet_given_quake = 1.0f;
        float p_quake = 0.0f;
    }
    
    public static class P_Quake extends BaseAggregator<Q_State> {
            /** expect: (Boolean near, Integer word-count, Integer tweet-count)
             */

            public Q_State init(Object batchId, TridentCollector collector) {
                    return new Q_State();
            }
            public void aggregate(Q_State state, TridentTuple tuple, TridentCollector collector) {
                    float p_word_base = tuple.getFloatByField("p_word_base");
                    float p_word_quake = tuple.getFloatByField("p_word_quake");
                    float p_quake = tuple.getFloatByField("p_quake");
                    state.p_tweet_given_base *= p_word_base;
                    state.p_tweet_given_quake *= p_word_quake;
                    state.p_quake = p_quake;
            }
            public void complete(Q_State state, TridentCollector collector) {
                    float p_quake = state.p_quake;
                    float p_base = 1.0f - p_quake;
                    float p_tweet = ((state.p_tweet_given_base * p_base)
                                    +(state.p_tweet_given_quake * p_quake));
                    float p_quake_given_tweet = state.p_tweet_given_quake * p_quake / p_tweet;
                    collector.emit(new Values(new Float(p_quake_given_tweet)));
            }
    }
    
    public static StormTopology buildTopology(LocalDRPC drpc) {
        // FIXME: This spout needs to be replaced with a KafkaSpout
		/*
        FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3,
                new Values("the cow jumped over the moon"),
                new Values("the man went to the store and bought some candy"),
                new Values("four score and seven years ago"),
                new Values("how many apples can you eat"),
                new Values("to be or not to be the person"));
		*/
		
	    TridentTopology topology = new TridentTopology();        

		TridentKafkaConfig spoutConfig = new TridentKafkaConfig(
            Common.getKafkaHosts(),
            //ImmutableList.of("cluster-7-kafka-00.sl.hackreduce.net:9999"), // list of Kafka brokers
  		   //8, // number of partitions per host
  		  "twitter_gnip-0" // topic to read from
        );
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConfig.forceStartOffsetTime(-2);


		
        TridentState wordCounts = topology.newStream(teamPrefix("shake-and-quake"), new TransactionalTridentKafkaSpout(spoutConfig))
            .parallelismHint(6)
            .each(new Fields("str"), new TwitterKafka.ExtractData(), new Fields("id", "content", "published"))
                .each(new Fields("tweet"), new Split(), new Fields("word"))
                .groupBy(new Fields("near", "word"))
                .persistentAggregate(new MemoryMapState.Factory(),
                                     new Count(), new Fields("count"))
                .parallelismHint(16);
        TridentState tweetCounts = topology.newStream(teamPrefix("shake-and-quake"), new TransactionalTridentKafkaSpout(spoutConfig))
                .groupBy(new Fields("near"))
                .persistentAggregate(new MemoryMapState.Factory(),
                                     new Count(), new Fields("count"))
                .parallelismHint(16);

        topology.newDRPCStream("p_quake", drpc)
                .each(new Fields(), new Waffle(), new Fields("near"))
                .stateQuery(tweetCounts, new Fields("near"), new MapGet(), new Fields("tweet-count"))
                .each(new Fields("args"), new Split(), new Fields("word"))
                .partitionBy(new Fields("near"))
                .partitionAggregate(new Fields("near", "word"), new FirstN.FirstNSortedAgg(1, "word", true), new Fields("near", "word"))
                .stateQuery(wordCounts, new Fields("near", "word"), new MapGet(), new Fields("word-count"))
                .each(new Fields("word-count"), new FilterNull())
                .partitionBy(new Fields("word"))
                .partitionAggregate(new Fields("near", "word-count", "tweet-count"), new P_Word(), new Fields("p_word_base", "p_word_quake", "p_quake"))
                .aggregate(new Fields("p_word_base", "p_word_quake", "p_quake"), new P_Quake(), new Fields("p_quake_given_tweet"));
        return topology.build();
    }
    
    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        conf.setMaxSpoutPending(20);
        if(args.length==0) {
            LocalDRPC drpc = new LocalDRPC();
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("wordCounter", conf, buildTopology(drpc));
            for(int i=0; i<100; i++) {
                System.out.println("DRPC RESULT: " + drpc.execute("words", "cat the dog jumped"));
                Thread.sleep(1000);
            }
        } else {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], conf, buildTopology(null));        
        }
    }
}
