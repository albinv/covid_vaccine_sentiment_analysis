package bolts;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.Arrays;
import java.util.HashSet;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import twitter4j.Status;


public class FilterIrrelevantWordsBolt extends BaseRichBolt {
	
	private OutputCollector collector;
	private static final long serialVersionUID = 1000002L;
	
	@Override
	public void execute(Tuple input) {
		final Status tweet = (Status) input.getValueByField("tweet");
		String tweet_string = tweet.getText().toLowerCase();
		Set<String> uselessWords = new HashSet<String>(Arrays.asList(new String[] {
	            "http", "https", "rt", "the", "you", "and", "for", "that", "have", "this", "just", "with", "all", "get", "so", "by", 
	            "about", "can", "was", "not", "your", "but", "are", "one", "what", "out", "when", "get", "lol", "now", "i",
	            "want", "will", "know", "from", "people", "got", "why", "time", "would", "as", "to", "per", "keep", "we", "be"
	    }));
		
		String filteredTweet = "";
		for (String word : tweet_string.split(" ")) {
			if (!uselessWords.contains(word)) {
				filteredTweet = filteredTweet + " " + word;
			}
		}
		System.out.println("\nGot Tweet: " + tweet_string + "\nFiltered: Tweet: " + filteredTweet + "\n+ ----------------------------------------\n");
		
		collector.emit(new Values(tweet_string));
	}
	
	@Override
	public void cleanup() {}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {}

}
