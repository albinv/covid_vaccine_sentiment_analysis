package spouts;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.LinkedBlockingQueue;
import java.io.File;
import java.io.FileNotFoundException;

import org.apache.storm.Config;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

@SuppressWarnings({"rawtypes"})
public class TwitterStreamingSpout extends BaseRichSpout {
	private static final long serialVersionUID = 1000001L;
	private SpoutOutputCollector collector;
	private LinkedBlockingQueue<Status> queue;
	private TwitterStream twitterStream;
	
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true)
		  .setOAuthConsumerKey("vVzbGtSxcxqnHdgpjy3pz7H0X")
		  .setOAuthConsumerSecret("tLIe63LVgzxISs78K3hDeZ9L54x4JKb09aehCFpKjDHIMtO0Uq")
		  .setOAuthAccessToken("970096902296555525-4kfcYVBHTYH0OT1wuqXQzeqmb2FdHHu")
		  .setOAuthAccessTokenSecret("MRxX63y6MozUab586PC42peB8fHHL7se8t0cYB0YgaMTj")
		  .setTweetModeExtended(true);
		this.twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
		this.collector = collector;
		this.queue = new LinkedBlockingQueue<>();
		
		StatusListener listener = new StatusListener() {
			@Override
			public void onStatus(Status status) {
				queue.offer(status);}
			@Override
			public void onDeletionNotice(StatusDeletionNotice sdn) {}
			@Override
			public void onTrackLimitationNotice(int i) {}
			@Override
			public void onScrubGeo(long l, long l1) {}
			@Override
			public void onException(Exception e) {}
			@Override
			public void onStallWarning(StallWarning warning) {}
		};
		twitterStream.addListener(listener);
		FilterQuery filterQuery = new FilterQuery();
		filterQuery.language(new String[]{"en"});
		for (String topic : getTopicsToSearchFor()) {
			filterQuery.track(topic);
		}
		twitterStream.filter(filterQuery);
	}
	
	public void nextTuple() {
		final Status status = queue.poll();
		if (status == null) {
			Utils.sleep(10);
		} else {
			collector.emit(new Values(status));
		}
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet"));
	}
	
	private List<String> getTopicsToSearchFor() {
		List<String> topicsToSearchFor = new ArrayList<String>();
		try {
			Scanner scanner = new Scanner(new File("./src/main/resources/topicsToSearchFor.txt"));
			while (scanner.hasNextLine()) {
				topicsToSearchFor.add(scanner.nextLine());
			}
			scanner.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		return topicsToSearchFor;
	}

	@Override
	public void activate() {};
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config ret = new Config();
		ret.setMaxTaskParallelism(1);
		return ret;
	}

	@Override
	public void deactivate() {
		twitterStream.cleanUp();
	};

	@Override
	public void close() {
		twitterStream.shutdown();
	}
}
