package project;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.math.BigInteger;

/**
 * Για κάθε R.a, κάνω sum όλων των partialSumX για να βρώ το totalsumX.
 * Υλοποίηση του Select R.a, sum(x) του Query.
 */

public class Reduce2 extends Reducer <IntWritable, Text, Text, Text> {
	
	protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		BigInteger totalsumX = new BigInteger("0");
		BigInteger sumX = new BigInteger("0");
		for (Text val : values){
			sumX = new BigInteger(val.toString());
			totalsumX=totalsumX.add(sumX);
		}
		
		context.write(null, new Text(key+"\t"+totalsumX));
	}
}
