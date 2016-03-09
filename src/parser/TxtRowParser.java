package parser;



import rows.RRow;
import rows.Row;
import rows.SRow;

import java.util.StringTokenizer;

public class TxtRowParser implements RowParser {
    
	
	/**
     * Δέχεται σαν είσοδο μια inputRow. Αν είναι από τον R, δημιουργεί ένα object RRow και αντίστοιχα αν
     * είναι από τον S, δημιουργεί ένα object SRow και την κάνει tokenizer στα αντίστοιχα properties (a , x).
     * Επιστρέφει το αντίστοιχο object.
     */
    @Override
    public Row convertFromString(String from) {
        StringTokenizer itr = new StringTokenizer(from, ",");
        String origin = itr.nextToken().toString();
        Row row;
        if (origin.equals("R")) {
            row = new RRow();
            row.setA(Integer.parseInt(itr.nextToken()));
        } else {
            row = new SRow();
            row.setA(Integer.parseInt(itr.nextToken()));
            ((SRow) row).setX(Integer.parseInt(itr.nextToken()));
        }

        return row;
    }

    /**
     * Δέχεται σαν είσοδο ένα object Row και επιστρέφει ένα string με όλα τα properties αυτού,
     * χωρισμένα με κόμμα μεταξύ τους. Χρησιμοποιείται για τις εξόδους των Map/Reduce jobs.
     */
    
    
    
    @Override
    public String convertToString(Row row) {

        StringBuilder builder = new StringBuilder(row.getOrigin());
        builder.append(",");
        builder.append(row.getA());
        if (row instanceof SRow) {
            builder.append(",");
            builder.append(((SRow) row).getX());
        }

        return builder.toString();
    }

}
