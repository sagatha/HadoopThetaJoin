package project;

import rows.RRow;
import rows.Row;
import rows.SRow;
import parser.TxtRowParser;
import parser.RowParser;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * Δέχεται σαν key το id του reducer και μια list of values που αναπαριστά μία Row.
 */



public class Reduce extends Reducer<IntWritable, Text, Text, Text> {

    private Text result = new Text();
    
    
    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        

        ArrayList<Row> rRows = new ArrayList<Row>();
        TreeMap<Row,Integer> sRows = new TreeMap<Row, Integer>();

        RowParser rowParser = new TxtRowParser();

        /**
         * Αποθήκευση όλων των RRows σε μία Arraylist και όλων των SRows σε μία Treemap με key το property
         * a και value το property x, ταξινομημένα από το μικρότερο στο μεγαλύτερο. Σε περίπτωση που
         * υπάρχει ήδη το S.a (key) που θέλουμε να προσθέσουμε, κάνουμε override το προηγούμενο value
         * με το άθροισμά αυτού συν το value του S.a που θέλομε να προσθέσουμε.
         */


        for (Text val : values) {
            Row row = rowParser.convertFromString(val.toString());
            if (row instanceof RRow) {
                RRow rRow = (RRow) row;
                rRows.add(rRow);
            } else {
                SRow srow = (SRow)row;

                Integer sumX = sRows.get(srow);
                if (sumX == null) {
                    sumX = 0;
                }

                sumX += srow.getX();
                sRows.put(srow, sumX);
            }
        }

        /**
         * Υλοποίηση του theta join.
         * Για κάθε RRow, εφαρμόζω tailMap στην Treemap με τις SRows και βρίσκω ακριβώς το data set που
         * ικανοποιεί την συνθήκη R.a<S.a. Στην συνέχεια κάνω το sum όλων των properties x γι' αυτό το data
         * set και δημιουργώ τα key-value pairs (R.a, partialSumX) για μείωση των ενδιάμεσων αποτελεσμάτων.
         */

        for (Row rRow : rRows) {
            NavigableMap<Row, Integer> tailMap = sRows.tailMap(rRow, false);
            int countSumX = 0;
            for (Integer sumX : tailMap.values()) {
                countSumX += sumX;
            }

            result.set(rRow.getA() + "," + countSumX);
            context.write(null,result);
        }

        
    }
}
