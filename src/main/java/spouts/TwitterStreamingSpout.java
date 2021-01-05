package spouts;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import twitter4j.*;

@SuppressWarnings({ "rawtypes", "serial" })
public class TwitterStreamingSpout extends BaseRichSpout {

	private SpoutOutputCollector collector;
	private LinkedBlockingQueue<Status> queue;
	private TwitterStream twitterStream;

	List<String> topicsToSearchFor = Arrays.asList(
			"Covid19", "Covid", "Coronavirus",
			"Vaccine", "CovidVaccine", "Covid19Vaccine",
			"Pfizer", "BioNTech", "AstraZeneca", "Covaxin");

	private static final long serialVersionUID = 1000001L;

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		this.twitterStream = new TwitterStreamFactory().getInstance();
		this.queue = new LinkedBlockingQueue<>();
		StatusListener listener = new StatusListener() {
			@Override
			public void onStatus(Status status) {
				queue.offer(status);
			}

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
		
		for (String topic : topicsToSearchFor) {
			filterQuery.track(topic);
		}
		
		twitterStream.filter(filterQuery);
	}
	
	
	public void nextTuple() {
		final Status status = queue.poll();
		if (status == null) {
			Utils.sleep(50);
		} else {
			collector.emit(new Values(status));
		}
	}

	@Override
	public void activate() {};

	@Override
	public void deactivate() {
		twitterStream.cleanUp();
	};

	@Override
	public void close() {
		twitterStream.shutdown();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet"));
	}

}
