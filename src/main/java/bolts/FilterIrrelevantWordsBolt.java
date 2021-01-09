package bolts;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.HashSet;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import twitter4j.Status;

public class FilterIrrelevantWordsBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1000002L;
	private OutputCollector collector;
	private Set<String> uselessWords;
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.uselessWords = getUselessWords();
	}
	
	@Override
	public void execute(Tuple input) {
		//get tweet which is stored in the param tuple passed in
		final Status tweet = (Status) input.getValueByField("tweet");
		String tweet_string;
		//make sure that the full text from the tweet is retrieved
		if (tweet.getRetweetedStatus() != null) {
			tweet_string = tweet.getRetweetedStatus().getText().toLowerCase();
        } else {
			tweet_string = tweet.getText().toLowerCase();
        }
		//  filter useless words out
		String filteredTweet = "";
		for (String word : tweet_string.split(" ")) {
			if (!uselessWords.contains(word)) {
				filteredTweet = filteredTweet + " " + word;
			}
		}
		// output new filtered string
		collector.emit(new Values(tweet_string));
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet_string"));
	}
	//method  to get words from the uselessWords file as a Set
	private Set<String> getUselessWords() {
		Set<String> uselessWords = new HashSet<String>();
		try {
			Scanner scanner = new Scanner(new File("./src/main/resources/uselessWords.txt"));
			while (scanner.hasNextLine()) {
				uselessWords.add(scanner.nextLine());
			}
			scanner.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		return uselessWords;
	}
}
