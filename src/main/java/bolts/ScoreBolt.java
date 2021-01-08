package bolts;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

public class ScoreBolt extends BaseRichBolt{
	private static final long serialVersionUID = 1000005L;
	private int no_of_tweets_analysed;
	private int positiveScore;
	private int negativeScore;
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.no_of_tweets_analysed = 0;
		this.positiveScore = 0;
		this.negativeScore = 0;
	}
	
	@Override
	public void execute(Tuple input) {
		final String tweet_string = (String) input.getValue(0);
		final boolean positive_sentiment = (boolean) input.getValue(1);
		final boolean sentiment_value = (boolean) input.getValue(2);
		System.out.println("\nAnalysing Tweet: " + tweet_string);
		if (positive_sentiment) {
			positiveScore ++;
			System.out.println(" Tweet shows POSITIVE sentiment \n");
		} else {
			negativeScore ++;
			System.out.println(" Tweet shows NEGATIVE sentiment \n");
			no_of_tweets_analysed ++;
		}
		
		outputSentimentAnalysis();
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {}
	
	@Override
	public void cleanup() {
		outputSentimentAnalysis();
	}
	
	private  void outputSentimentAnalysis() {
		int pos = this.positiveScore;
		int neg = this.negativeScore;
		int total = pos + neg;
		double positive = pos/total;
		double negative = neg/total;
		System.out.println("T:" + total + ", P:" + pos + ", N:" + neg + " . \n");
		System.out.println(
				"\n\n================================================================="
				+ "\n Total tweets analysed so far: " + no_of_tweets_analysed
				+ "\n Percentage of tweets with POSITIVE Sentiment: " + (positive * 100) + "%"
				+ "\n Percentage of tweets with NEGATIVE Sentiment: " + (negative * 100) + "%"
				+  "\n=================================================================");
	}
}
