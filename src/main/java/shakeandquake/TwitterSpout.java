class Untitled {
	public static void main(String[] args) {
		
		SpoutConfig spoutConfig = new SpoutConfig(
		  ImmutableList.of("cluster-7-kafka-00.sl.hackreduce.net:9999"), // list of Kafka brokers
		  8, // number of partitions per host
		  "twitter_gnip-0", // topic to read from
		  "/kafkastorm", // the root path in Zookeeper for the spout to store the consumer offsets
		  "discovery"); // an id for this consumer for storing the consumer offsets in Zookeeper
		KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
	}
}