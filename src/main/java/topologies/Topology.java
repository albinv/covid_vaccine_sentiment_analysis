package topologies;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

import spouts.TwitterStreamingSpout;
import bolts.FilterIrrelevantWordsBolt;


public class Topology {
	
	public static void main(String args[]) throws InterruptedException {
		final TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("twitterSpout", new TwitterStreamingSpout());
		builder.setBolt("filterWords", new FilterIrrelevantWordsBolt()).shuffleGrouping("twitterSpout");
		
		Config config = new Config();
		config.setDebug(false);
		//config.setMessageTimeoutSecs(120);

		final LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("big_data_a2", config, builder.createTopology());
		Thread.sleep(30000);
		cluster.shutdown();
		
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				cluster.killTopology("big_data_a2");
				cluster.shutdown();
			}
		});
	}
}
