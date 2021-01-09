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

public class PositiveWordsBolt extends BaseRichBolt{
	private static final long serialVersionUID = 1000003L;
	private OutputCollector collector;
	private Set<String> positiveWords;
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.positiveWords = getPositiveWords();
	}
	
	@Override
	public void execute(Tuple input) {
		// when a tuple comes in, checks if it contains positive words
		final String tweet_string = (String) input.getValue(0);
		boolean positiveTweet = false;
		for (String word : tweet_string.split(" ")) {
			if (positiveWords.contains(word)) {
				positiveTweet = true;
			}
		}
		collector.emit(new Values(tweet_string, true, positiveTweet));
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet_string", "sentiment", "sentiment_value"));
	}
	//method  to get words from the positiveWords file as a Set
	private Set<String> getPositiveWords() {
		Set<String> positiveWords = new HashSet<String>();
		try {
			Scanner scanner = new Scanner(new File("./src/main/resources/positiveWords.txt"));
			while (scanner.hasNextLine()) {
				positiveWords.add(scanner.nextLine());
			}
			scanner.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		return positiveWords;
	}
}
