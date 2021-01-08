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
		builder.setSpout("twitterSpout", new TwitterStreamingSpout());
		builder.setBolt("filterWords", new FilterIrrelevantWordsBolt()).shuffleGrouping("twitterSpout");
		
		builder.setBolt("positiveCount", new PositiveWordsBolt()).shuffleGrouping("filterWords");
		builder.setBolt("negativeCount", new NegativeWordsBolt()).shuffleGrouping("filterWords");
		
		
		builder.setBolt("score", new ScoreBolt()).shuffleGrouping("positiveCount").shuffleGrouping("negativeCount");
		
		Config config = new Config();
		config.setDebug(false);

		final LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("big_data_a2", config, builder.createTopology());
		
		//Thread.sleep(60000);
		//cluster.killTopology("big_data_a2");
		//cluster.shutdown();
		return;
	}
}
