package bolts;

import java.text.DecimalFormat;
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
	
	// initialising values
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.no_of_tweets_analysed = 0;
		this.positiveScore = 0;
		this.negativeScore = 0;
	}
	
	@Override
	public void execute(Tuple input) {
		// determine if the tuple comes from the positive or negative bolt and checks if sentiment is true
		// Increments relevant score as needed
		final String tweet_string = (String) input.getValue(0);
		final boolean positive_sentiment = (boolean) input.getValue(1);
		final boolean sentiment_value = (boolean) input.getValue(2);
		System.out.println("\nAnalysing Tweet: " + tweet_string);
		if (positive_sentiment) {
			if (sentiment_value) {
				positiveScore ++;
				System.out.println(" Tweet shows POSITIVE sentiment \n");
			}
			
		} else {
			if (sentiment_value) {
				negativeScore ++;
				System.out.println(" Tweet shows NEGATIVE sentiment \n");
			}
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
	// Method  to output current score
	private  void outputSentimentAnalysis() {
		int pos = this.positiveScore;
		int neg = this.negativeScore;
		int total = pos + neg;
		double positivePercent;
		double negativePercent;
		if (total > 0) {
			positivePercent = (double) (100.0 * pos) /total;
			negativePercent = (double) (100.0 * neg) /total;
		} else {
			positivePercent = 0.0;
			negativePercent = 0.0;
		}
		DecimalFormat df = new DecimalFormat("#.##");
		System.out.println(
				"\n\n================================================================="
				+ "\n Total tweets analysed so far: " + no_of_tweets_analysed
				+ "\n Percentage of tweets with POSITIVE Sentiment: " + df.format(positivePercent) + "%"
				+ "\n Percentage of tweets with NEGATIVE Sentiment: " + df.format(negativePercent) + "%"
				+  "\n=================================================================");
	}
}
