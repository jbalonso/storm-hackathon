package shakeandquake;

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
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.planner.processor.StateQueryProcessor;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;


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
            for(String word: sentence.split(" ")) {
                collector.emit(new Values(new Boolean(true)));                
                collector.emit(new Values(new Boolean(false)));                
            }
        }
    }
    public static class P_Word extends BaseAggregator<W_State> {
            /** expect: (Boolean near, Integer word-count, Integer tweet-count)
             *  emit: (Float p_word_base, Float p_word_quake, Float p_quake)
             */
            static class W_State {
                    int words_quake = 0;
                    int words_base = 0;
                    int tweets_quake = 0;
                    int tweets_base = 0;
            }
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
                    Float p_word_base = new Float(float(state.words_base)/state.tweets_base);
                    Float p_word_quake = new Float(float(state.words_quake)/state.tweets_quake);
                    Float p_quake = new Float(float(state.tweets_quake) / (state.tweets_base + state.tweets_quake));
                    collector.emit(new Values(p_word_base, p_word_quake, p_quake));
            }
    }
    
    public static class P_Quake extends BaseAggregator<Q_State> {
            /** expect: (Boolean near, Integer word-count, Integer tweet-count)
             */
            static class Q_State {
                    float p_tweet_given_base = 0.0;
                    float p_tweet_given_quake = 0.0;
                    float p_quake = 0.0;
                    float p_base = 0.0;
                    float p_quake_given_tweet =0.0;
            }
            public Q_State init(Object batchId, TridentCollector collector) {
                    return new Q_State();
            }
            public void aggregate(Q_State state, TridentTuple tuple, TridentCollector collector) {
            }
            public void complete(Q_State state, TridentCollector collector) {
            }
    }
    
    public static StormTopology buildTopology(LocalDRPC drpc) {
        // FIXME: This spout needs to be replaced with a KafkaSpout
        FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3,
                new Values("the cow jumped over the moon"),
                new Values("the man went to the store and bought some candy"),
                new Values("four score and seven years ago"),
                new Values("how many apples can you eat"),
                new Values("to be or not to be the person"));

        spout.setCycle(true);
        
        TridentTopology topology = new TridentTopology();        
        Stream raw_tweets =
              topology.newStream("tweets-undifferentiated", spout)
                .parallelismHint(16);
        TridentState wordCounts = raw_tweets
                .each(new Fields("tweet"), new Split(), new Fields("word"))
                .groupBy(new Fields("near", "word"))
                .persistentAggregate(new MemoryMapState.Factory(),
                                     new Count(), new Fields("count"))
                .parallelismHint(16);
        TridentState tweetCounts = raw_tweets
                .groupBy(new Fields("near"))
                .persistentAggregate(new MemoryMapState.Factory(),
                                     new Count(), new Fields("count"))
                .parallelismHint(16);

        // FIXME: This DRPC code needs to be fixed to process a tweet correctly
        topology.newDRPCStream("p_quake", drpc)
                .each(new Fields(), new Waffle(), new Fields("near"))
                .stateQuery(tweetCounts, new Fields("near"), new MapGet(), new Fields("tweet-count"))
                .each(new Fields("args"), new Split(), new Fields("word"))
                .groupBy(new Fields("near"))
                .aggregate(new Fields("word"), new FirstN(1, "word"), new Fields("word-uniq"))
				.groupBy(new Fields("near", "word")
                .project(new Fields("near", "word", "tweet-count"))
                .stateQuery(wordCounts, new Fields("near", "word"), new MapGet(), new Fields("word-count"))
                .each(new Fields("word-count"), new FilterNull())
                .aggregate(new Fields("count"), new Sum(), new Fields("sum"))
                ;
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
