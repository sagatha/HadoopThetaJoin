package project;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Main {

    public static void main(String[] args) throws Exception
    {
         //Η ώρα που ξεκινά το project σε milliseconds
    	 long startMillis = System.currentTimeMillis();
         System.out.println("Project starts at :"+ startMillis);

         Configuration conf = new Configuration();
        
         /*Tο συγκεκριμένο attribute αφαιρεί το όριο ανάγνωσης του 1000000 γραμμών 
          * του αρχείου εισόδου.
          */
         conf.set("mapreduce.jobtracker.split.metainfo.maxsize", "-1");
        
         int [] myTable=Setup(Integer.parseInt(args[2]), Integer.parseInt(args[3]),Integer.parseInt(args[4]));
         conf.set("R",String.valueOf(myTable[4]));
         conf.set("S",String.valueOf(myTable[5]));
         conf.set("cS", String.valueOf(myTable[0]));
         conf.set("cR", String.valueOf(myTable[1]));
         conf.set("optimalHeight", String.valueOf(myTable[2]));
         conf.set("optimalWidth", String.valueOf(myTable[3]));
        
        //1o Job
        Job job = new Job(conf, "ThetaJoin");

        job.setMapperClass(project.Map.class);
        job.setReducerClass(project.Reduce.class);
        job.setJarByClass(Main.class);

        job.setNumReduceTasks(Integer.parseInt(args[4]));
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        //Αυτόματη διαγραφή του φακέλου output 
        FileSystem fs = FileSystem.get(new Configuration());
        fs.delete(new Path(args[5]), true);
        FileOutputFormat.setOutputPath(job, new Path(args[5]));

        boolean result = job.waitForCompletion(true);
        
        //2o Job
        conf = new Configuration();
        
        job = new Job(conf, "Sum");
        

        job.setJarByClass(Main.class);
        job.setMapperClass(project.Map2.class);
        job.setReducerClass(project.Reduce2.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        //Αυτόματη διαγραφή του φακέλου temp 
        FileInputFormat.addInputPath(job, new Path(args[5]));
        fs.delete(new Path(args[1]), true);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        result = job.waitForCompletion(true);
        
        //Η ώρα που τελείωνει το project σε milliseconds
        long endMillis = System.currentTimeMillis() ;
        //Η συνολική διάρκεια του project σε milliseconds
        System.out.println("Project duration is :"+ (endMillis-startMillis));

        System.exit(result ? 0 : 1);
    }
    
    /* Μέθοδος που υπολογίζει βοηθητικές μεταβλητές και δέχεται σαν παραμέτρους το inputR για το πλήθος των πλειάδων R,
     * το inputS για το πλήθος των πλειάδων S και το NumOfReducer για το πλήθος των reducers.
     * Βασίζεται στο άρθρο "Processing Theta-Join using MapReduce" των A.Okcan και M.Riedewald
     * για να χωρίσει βέλτιστα τον νοητό πίνακα RxS σε NumOfReducer τμήματα.
     * 
     */

    public static int[] Setup (int inputR, int inputS, int NumOfReducer) {

        /*
         * Αντιστοιχίζει το μικρότερο πλήθος πλειάδων στις γραμμές του πίνακα. 
         */
    	int R, S;
        int optimalWidth, optimalHeight;
        int cR, cS;
        
    	
    	if (inputR > inputS) {
            int temp = inputR;
            R = inputS;
            S = temp;
        } else {
            R = inputR;
            S = inputS;
        }
        
    	
    	/*cR πλήθος των optimal square στις γραμμές  
    	 *cS πλήθος των optimal square στις στήλες
    	 *optimalWidth το πλάτος του optimal square
    	 *optimalHeight το ύψος του optimal square 
    	 */
        
    	double d = Math.sqrt(R * S / NumOfReducer);
        optimalWidth = (int) d;
        optimalHeight = (int) d;
        cS=0;
        cR=0;
        
        /*1η περίπτωση:Το |R| και το |S|  είναι πολλαπλάσια του d.
         * Ο πίνακας χωρίζεται τέλεια και δεν περισσέουν γραμμές ή στήλες.
         */
         
        
         
        if (R % d == 0 && S % d == 0) {
            cR = R / (int) d;
            cS = S / (int) d;
        }
        
        /*2η περίπτωση : Το |R| είναι πολύ μικρότερο του |S|.
         * Ο πίνακας θα έχει 1 optimal square στις γραμμές και στις στήλες
         * θα έχει τόσα optimal squares όσα και ο αριθμός των reducers.
         * Υπάρχει πιθανότητα να περισσεύουν κάποιες στήλες.
         */
         
        
        
        else if (R < (S / NumOfReducer)) {
            optimalWidth = (int) Math.floor((double) S / (double) NumOfReducer);
            optimalHeight = R;
            cR = 1;
            cS = NumOfReducer;
        }
        
        /*3η περίπτωση : Το |R| είναι μικρότερο του |S|.
         * Ο πίνακας χωρίζεται cR optimal squares στις γραμμές, 
         * σε cS optimal squares στις στήλες και περισσεύουν κάποιες γραμμές
         * ή στήλες.
         */
        
        
        else if (R <= S) {
            cR = (int) Math.floor((double) R / (int) d);
            cS = (int) Math.floor((double) S / (int) d);
        }
        
        /*Πίνακας που αποθηκεύονται οι βοηθητικές μεταβλητές cS, cR, optimalHeight, optimalWidth,R,S.
        *Οι R και S αποθηκεύονται ξανά γιατί πλέον R είναι το πλήθος των πλειάδων με τις λιγότερες εγγραφές
        *και αντιστοιχεί στις γραμμές του νοητού πίνακα.
        */
        int[] myTable=new int[6];
        myTable[0]=cS;
        myTable[1]=cR;
        myTable[2]=optimalHeight;
        myTable[3]=optimalWidth;
        myTable[4]=R;
        myTable[5]=S;
        
        return myTable;
    }
}