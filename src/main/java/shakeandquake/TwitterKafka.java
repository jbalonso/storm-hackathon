package org.hackreduce.storm.example.riak;

import backtype.storm.Config;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import com.google.common.collect.ImmutableList;
import org.hackreduce.storm.HackReduceStormSubmitter;
import org.hackreduce.storm.example.common.Common;
import org.hackreduce.storm.example.stock.MarketCapitalization;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.StringScheme;
import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.CombinerAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import static org.hackreduce.storm.HackReduceStormSubmitter.teamPrefix;

public class TwitterKafka {
	
	public static class ExtractData extends BaseFunction {

	        private static final Logger LOG = LoggerFactory.getLogger(ExtractData.class);

	        @Override
	        public void execute(TridentTuple tuple, TridentCollector collector) {

	            String[] components = tuple.getString(0).split(",");
	
				Tweet.parseXml(entryXml);
				        try {
				            Document doc = (Document) xpath.evaluate("/", source, XPathConstants.NODE);

				            String id = xpath.evaluate("/entry/id", doc);
				            String category = xpath.evaluate("/entry/category", doc);
				            String content = xpath.evaluate("/entry/object/content", doc);
				            String publishedValue = xpath.evaluate("/entry/published", doc);

				            // Ignore any tweets that have been deleted
				            if("Note deleted".equalsIgnoreCase(category)) {
				                return null;
				            }

				            DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ssZ").withZoneUTC();
				            DateTime publishedTimestamp = fmt.parseDateTime(publishedValue);

				            tweet = new Tweet(id, content, publishedTimestamp);
				        } catch (Exception e) {
				            System.out.println("Error parsing tweet... ignoring it");
				        }
	             catch (IndexOutOfBoundsException ioobe) {
	                LOG.warn("Invalid input row", ioobe);
	            } catch (NumberFormatException nfe) {
	                LOG.warn("Could not parse numeric value", nfe);
	            }
	        }
	    }


    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {

        Config config = new Config();

        // The number of processes to spin up for this job
        config.setNumWorkers(10);

        TridentKafkaConfig spoutConfig = new TridentKafkaConfig(
            Common.getKafkaHosts(),
            "twitter_gnip-0" //, "twitter_quake-1"
        );


        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

        // This tells the spout to start at the very beginning of the data stream
        // If you just want to resume where you left off, remove this line
        spoutConfig.forceStartOffsetTime(-2);

        TridentTopology builder = new TridentTopology();

        builder
            .newStream(teamPrefix("shake-and-quake"), new TransactionalTridentKafkaSpout(spoutConfig))
            .parallelismHint(6)
            .each(new Fields("str"), new Tweet.TwitterKafka(), new Fields("id", "content", "published"))
            //.groupBy(new Fields("published"))
            .persistentAggregate(
                    // A nontransactional state built on Riak (Use their HTTP API to see progress)
                    new RiakBackingMap.Factory(
                            teamPrefix("shakeandquake"), // The riak 'bucket' name to store results in
                            Common.getRiakHosts(),
                            Common.getRiakPort(),
                            Double.class               // The type of the data to store (serialized as json)
                    ),
                    new Fields("market_cap"),
                    new MarketCapitalization.MaxValue(),
                    new Fields("max_market_cap")
            )
            .newValuesStream()
            .each(new Fields("exchange", "symbol", "max_market_cap"), new MarketCapitalization.LogInput(), new Fields("never_emits"));

        HackReduceStormSubmitter.submitTopology("market-cap", config, builder.build());
    }
}
