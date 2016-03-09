package project;

import Partitioner.Partitioner;
import rows.RRow;
import rows.Row;
import rows.SRow;
import parser.TxtRowParser;
import parser.RowParser;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

/**
 * Δημιουργία key-value pairs με key το id του reducer και value την πλειάδα. 
 */

public class Map extends Mapper<LongWritable, Text, IntWritable, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		Configuration conf = context.getConfiguration();
		int R=Integer.parseInt(conf.get("R"));
		int S=Integer.parseInt(conf.get("S"));
		int cR=Integer.parseInt(conf.get("cR"));
		int cS=Integer.parseInt(conf.get("cS"));
		int optimalHeight=Integer.parseInt(conf.get("optimalHeight"));
		int optimalWidth=Integer.parseInt(conf.get("optimalWidth"));
		
		
		// Διάβασμα μιας γραμμής του αρχείου εισόδου.

		RowParser rowParser = new TxtRowParser();
		Row row = rowParser.convertFromString(value.toString());

		Integer ColX = 0;

		/**
		 * Aν η row προέρχεται από τον πίνακα S, εκτός από το property a
		 * αποθηκεύουμε και το property x.
		 */
		if (row instanceof SRow) {
			ColX = ((SRow) row).getX();
		}
		

		int matrixRow = 0;
		int matrixColumn = 0;

		int range;

		ArrayList<Integer> regionIds = new ArrayList<Integer>();

		// Φιλτράρισμα του property a των γραμμων R να είναι μεγαλύτερο του 10.
		if (row instanceof RRow && row.getA() > 10) {

			/**
			 * Υλοποίηση της πρότασης του paper όπου για κάθε Row είτε τύπου
			 * RRow είτε SRow, δίνεται μια τυχαία τιμή με range 0 -|R| και 0
			 * -|S| αντίστοιχα. Έπειτα καλώντας την Partitioner αποθηκεύουμε όλα
			 * τα ids των reducers που κάνουν intersect με την συγκεκριμένη row.
			 */
			range = ((R - 1) - 1) + 1;//((max-min)-1)+min
			matrixRow = new Random().nextInt(range);

			regionIds = Partitioner.getRegionsInRow(matrixRow,cR,cS,optimalHeight,optimalWidth,R ,S);
		}

		// Φιλτράρισμα του property a των γραμμων S να είναι μεγαλύτερο του 10.
		else if (row instanceof SRow && row.getA() > 10) {

			
			range = ((S - 1) - 1) + 1;//((max-min)-1)+min
			matrixColumn = new Random().nextInt(range);

			regionIds = Partitioner.getRegionsInColumn(matrixColumn,cR,cS,optimalHeight,optimalWidth,R ,S);
					
		}

		/**
         * Δημιουργία των key-value pairs ουτώς ώστε η row να σταλεί σε όλους τους reducers
         * με τους οποίους κάνει intersect.
         */
		for (int i = 0; i < regionIds.size(); i++) {
			Text tuple = new Text();

			tuple.set(rowParser.convertToString(row));
			context.write(new IntWritable(regionIds.get(i)), tuple);
		}

		
	}
}