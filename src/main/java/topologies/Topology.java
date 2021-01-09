package topologies;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

import spouts.TwitterStreamingSpout;
import bolts.FilterIrrelevantWordsBolt;
import bolts.PositiveWordsBolt;
import bolts.NegativeWordsBolt;
import bolts.ScoreBolt;

public class Topology {
	
	public static void main(String args[]) throws InterruptedException {
		final TopologyBuilder builder = new TopologyBuilder();
		//spout to stream covid-vaccine related tweets from twitter
		builder.setSpout("twitterSpout", new TwitterStreamingSpout());
		// bolt to remove unnecessary words
		builder.setBolt("filterWords", new FilterIrrelevantWordsBolt(),4).setNumTasks(8).shuffleGrouping("twitterSpout");
		//bolts to determine the sentiment in tweet
		builder.setBolt("positiveCount", new PositiveWordsBolt(),2).setNumTasks(4).shuffleGrouping("filterWords");
		builder.setBolt("negativeCount", new NegativeWordsBolt(),2).setNumTasks(4).shuffleGrouping("filterWords");
		// bolt to keep score of the tweet
		builder.setBolt("score", new ScoreBolt()).shuffleGrouping("positiveCount").shuffleGrouping("negativeCount");
		//set storm config anf submit topology to a local cluster
		Config config = new Config();
		config.setDebug(false);
		config.setNumWorkers(4);
		final LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("big_data_a2", config, builder.createTopology());
		// Run for the specified time
		// Can comment the next 3 lines to keep streaming data indefinitely until manually stopped
		Thread.sleep(60000);
		cluster.killTopology("big_data_a2");
		cluster.shutdown();
		return;
	}
}