package rows;

/**
 * Αναπαριστά μία γραμμή του πίνακα R.
 */

public class RRow implements Row {

    private Integer a;

    @Override
    public String getOrigin() {
        return "R";
    }

    @Override //R.a
    public Integer getA() {
        return a;
    }

    public void setA(Integer a) {
        this.a = a;
    }

    @Override
    /**
     * Δηλώνει ότι το κοινό χαρακτηριστικό δύο objects τύπου Row είναι το property a.
     */
    public int compareTo(Row row) {
        return a.compareTo(row.getA());
    }
}
