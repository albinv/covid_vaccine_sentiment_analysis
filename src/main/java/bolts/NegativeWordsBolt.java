package bolts;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashSet;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class NegativeWordsBolt extends BaseRichBolt{
	private static final long serialVersionUID = 1000004L;
	private OutputCollector collector;
	private Set<String> negativeWords;
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.negativeWords = getNegativeWords();
	}
	
	@Override
	public void execute(Tuple input) {
		// when a tuple comes in, checks if it contains negative words
		final String tweet_string = (String) input.getValue(0);
		boolean negativeTweet = false;
		for (String word : tweet_string.split(" ")) {
			if (negativeWords.contains(word)) {
				negativeTweet = true;
			}
		}
		collector.emit(new Values(tweet_string, false, negativeTweet));
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet_string", "sentiment", "sentiment_value"));
	}
	//method  to get words from the negativeWords file as a Set
	private Set<String> getNegativeWords() {
		Set<String> negativeWords = new HashSet<String>();
		try {
			Scanner scanner = new Scanner(new File("./src/main/resources/negativeWords.txt"));
			while (scanner.hasNextLine()) {
				negativeWords.add(scanner.nextLine());
			}
			scanner.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		return negativeWords;
	}
}
