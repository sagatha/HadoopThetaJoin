package Partitioner;

import java.util.ArrayList;
import java.util.Random;

/*Η μέθοδος Partitioner δέχεται σαν παραμέτρους το inputR για το πλήθος των πλειάδων R,
 * το inputS για το πλήθος των πλειάδων S και το NumOfReducer για το πλήθος των reducers.
 * Βασίζεται στο άρθρο "Processing Theta-Join using MapReduce" των A.Okcan και M.Riedewald
 * για να χωρίσει βέλτιστα τον νοητό πίνακα RxS σε NumOfReducer τμήματα.
 * 
 */

public class Partitioner {

  
    
    /*Σε κάθε πλειάδα του αρχείου εισόδου αντιστοιχίζει μία λίστα από regionIds
     * που δείχνει σε ποιους reducers θα στείλει την κάθε πλειάδα.
     * Ο πίνακας έχει χωριστεί σε cR γραμμές ύψους optimalHeight (optRow) 
     * και cS στήλες πλάτους optimalWidth (optColumn).
     * Κάθε τυχαία γραμμή ή στήλη είτε ανήκει σε κάποια optRow ή optColumn 
     * είτε βρίσκεται μετά από αυτές.Η ανάθεση των γραμμών και των στηλών που βρίσκονται μετά από 
     * αυτές γίνεται με τρόπο που μπορεί να αναχθεί στην τεχνική του round robin.
     * Πληροφορίες υλοποίησης αυτής της τεχνικής περιγράφονται παρακάτω.
     * 
     *  
     *  Γενική σημείωση: ο λόγος για τον οποίο χρησιμοποιείται μεγάλος αριθμός παραμέτρων 
     *  στις ακόλουθες μεθόδους ενώ θα μπορούσαν να υπολογιστούν μεμιάς στην ίδια την κλάση
     *  είναι η αποφυγή επαναλαμβανόμενων υπολογισμών των ίδιων μεταβλητών στα instances της 
     *  Map.
     */
    
    
    

    public static ArrayList<Integer> getRegionsInRow(int row, int cR, int cS, int optimalHeight,int optimalWidth,int R , int S) {

        int optRow;

        double div = (double) row / (double) optimalHeight;

        ArrayList<Integer> regionIds = new ArrayList<Integer>();//λίστα από reducers

        if (row == 0) {
            optRow = 1;
        }
        /*Ελέγχει αν η τυχαία γραμμή row βρίσκεται στις γραμμές που έχουν
         * καλυφθεί με optimal squares.
         */
        
        if (row < cR * optimalHeight) {
            if (row % optimalHeight != 0) {
                optRow = (int) Math.ceil(div);
            } else {
                optRow = (int) div;
                optRow++;
            }
            
            /*Η ανάθεση των reducers γίνεται σειριακά από αριστερά προς τα δεξιά 
             * και από πάνω προς τα κάτω. 
             */
            
            int previousReducer = cS * (optRow - 1);// ο τελευταίος reducer της προηγούμενης optRow
            int currentReducer = previousReducer + 1;//ο πρώτος reducer της optRow που βρισκόμαστε
            for (int i = optimalWidth; i <= S; i += optimalWidth) {
                regionIds.add(currentReducer);
                currentReducer++;
            }
        } else {

        	/*Στη μεταβλητή iter υπολογίζω ένα τυχαίο ακέραιο  
        	 * αριθμό μεταξύ 0 και cR για να αναθέσω την πλειάδα στους 
        	 * αντίστοιχους reducers με την optRow που ισούται με iter.
        	 */
            int iter = new Random().nextInt((cR - 1) + 1);
            for (int i = 1; i <= cS; i++) {

                regionIds.add(i + cS * iter);
            }
        }
        return regionIds;
    }

    public static ArrayList<Integer> getRegionsInColumn(int col , int cR, int cS, int optimalHeight,int optimalWidth,int R , int S) {

        int optColumn;

        double div = (double) col / (double) optimalWidth;

        ArrayList<Integer> regionIds = new ArrayList<Integer>();

        int currentReducer = 0;
        if (col == 0) {
            optColumn = 1;
        }
        
        /*Ελέγχει αν η τυχαία στήλη column βρίσκεται στις στήλες που έχουν
         * καλυφθεί με optimal squares.
         */
        
        if (col < cS * optimalWidth) {
            if (col % optimalWidth != 0) {
                optColumn = (int) Math.ceil((double) div);
            } else {
                optColumn = (int) div;
                optColumn++;
            }

            currentReducer = optColumn;//ο πρώτος reducer της optColumn που βρισκόμαστε
            for (int i = optimalHeight; i <= R; i += optimalHeight) {
                regionIds.add(currentReducer);
                currentReducer += cS;
            }
        } else {
        	
        	
            currentReducer = col % cS + 1;
            for (int i = optimalHeight; i <= R; i += optimalHeight) {

                regionIds.add(currentReducer);
                currentReducer += cS;
            }
        }
        return regionIds;
    }
}