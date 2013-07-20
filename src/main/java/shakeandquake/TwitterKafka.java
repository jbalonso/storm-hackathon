package shakeandquake;

import backtype.storm.Config;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import com.google.common.collect.ImmutableList;
import org.hackreduce.storm.HackReduceStormSubmitter;
import org.hackreduce.storm.example.common.Common;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.StringScheme;
import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import static org.hackreduce.storm.HackReduceStormSubmitter.teamPrefix;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import java.io.StringReader;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
public class TwitterKafka {
	
	public static class ExtractData extends BaseFunction {

	        private static final Logger LOG = LoggerFactory.getLogger(ExtractData.class);

	        @Override
	        public void execute(TridentTuple tuple, TridentCollector collector) {

	            String entryXml = tuple.getString(0);
	            XPathFactory xpathFactory = XPathFactory.newInstance();
	            XPath xpath = xpathFactory.newXPath();

	            InputSource source = new InputSource(new StringReader(entryXml));
				        try {
				            Document doc = (Document) xpath.evaluate("/", source, XPathConstants.NODE);

				            String id = xpath.evaluate("/entry/id", doc);
				            String category = xpath.evaluate("/entry/category", doc);
				            String content = xpath.evaluate("/entry/object/content", doc);
				            String publishedValue = xpath.evaluate("/entry/published", doc);
				            DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ssZ").withZoneUTC();
				            DateTime publishedTimestamp = fmt.parseDateTime(publishedValue);

				            collector.emit(
				                    ImmutableList.<Object>of(id , content , publishedTimestamp.toDate().getTime())
				                );
				        } catch (Exception e) {
				        	e.printStackTrace();
				        }
	            }
	        }
	    
	public static Stream buildSpout(TridentTopology builder){
        TridentKafkaConfig spoutConfig = new TridentKafkaConfig(
            Common.getKafkaHosts(),
            //ImmutableList.of("cluster-7-kafka-00.sl.hackreduce.net:9999"), // list of Kafka brokers
  		   //8, // number of partitions per host
  		  "twitter_gnip-0" // topic to read from
        );

        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

        // This tells the spout to start at the very beginning of the data stream
        // If you just want to resume where you left off, remove this line
        spoutConfig.forceStartOffsetTime(-2);

        return builder
            .newStream(teamPrefix("shake-and-quake"), new TransactionalTridentKafkaSpout(spoutConfig))
            .parallelismHint(6)
            .each(new Fields("str"), new ExtractData(), new Fields("id", "content", "published"));
	}

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {

        Config config = new Config();

        // The number of processes to spin up for this job
        config.setNumWorkers(10);

        TridentKafkaConfig spoutConfig = new TridentKafkaConfig(
            Common.getKafkaHosts(),
            //ImmutableList.of("cluster-7-kafka-00.sl.hackreduce.net:9999"), // list of Kafka brokers
  		   //8, // number of partitions per host
  		  "twitter_gnip-0" // topic to read from
        );
		//KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);


        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

        // This tells the spout to start at the very beginning of the data stream
        // If you just want to resume where you left off, remove this line
        spoutConfig.forceStartOffsetTime(-2);

        TridentTopology builder = new TridentTopology();

        builder
            .newStream(teamPrefix("shake-and-quake"), new TransactionalTridentKafkaSpout(spoutConfig))
            .parallelismHint(6)
            .each(new Fields("str"), new ExtractData(), new Fields("id", "content", "published"));
            //.groupBy(new Fields("published"))
            /*
            .persistentAggregate(
                    // A nontransactional state built on Riak (Use their HTTP API to see progress)
                    new RiakBackingMap.Factory(
                            teamPrefix("shakeandquake"), // The riak 'bucket' name to store results in
                            Common.getRiakHosts(),
                            Common.getRiakPort(),
                            Double.class               // The type of the data to store (serialized as json)
                    )            )
            .newValuesStream()
            .each(new Fields("id", "content", "published"), new TwitterKafka.LogInput(), new Fields("never_emits"));
            */

        HackReduceStormSubmitter.submitTopology("twitter-kafka", config, builder.build());
    }
    
    /**
     * Log each new value for the market cap.
     */
    public static class LogInput extends BaseFunction {

        private static final Logger LOG = LoggerFactory.getLogger(LogInput.class);

        @Override
        public void execute(TridentTuple objects, TridentCollector tridentCollector) {
            
        }
    }
}
