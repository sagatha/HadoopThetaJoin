package project;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Δημιουργία κατάλληλων key-value pairs με key το R.a έτσι ώστε όλα τα pairs με key το R.a
 * να πάνε στον ίδιο reducer. Με άλλα λόγια, υλοποίηση του Group by R.a του Query.
 */


public class Map2 extends Mapper<LongWritable,Text,IntWritable,Text> {

	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
		StringTokenizer itr = new StringTokenizer(value.toString(), ",");
		String tupleRa = itr.nextToken().toString();
		String sumSx = itr.nextToken().toString();

		context.write(new IntWritable(Integer.parseInt(tupleRa)),new Text(sumSx));
		
	}
}
