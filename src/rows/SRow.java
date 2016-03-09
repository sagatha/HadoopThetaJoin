package rows;

/**
 * Αναπαριστά μία γραμμή του πίνακα S.
 */

public class SRow implements Row {

    private Integer a, x;

    @Override
    public String getOrigin() { return "S"; }

    @Override // S.a
    public Integer getA() { return a; }

    public void setA(Integer a) { this.a = a; }

    //S.x
    public Integer getX() { return x; }

    public void setX(Integer s) { this.x = s; }

    /**
     * Δηλώνει ότι το κοινό χαρακτηριστικό δύο objects τύπου Row είναι το property a.
     */
    @Override
    public int compareTo(Row row) {
        return a.compareTo(row.getA());
    }

    /**
     *  Ελέγχει το αν δύο Objects είναι ίσα. Πιο συγκεκριμένα:
     *  1. ελέγχει αν το Object που περνάμε σαν παράμετρο είναι το ίδιο,
     *  2. ελέγχει αν το Object που περνάμε σαν παράμετρο είναι null,
     *  3. ελέγχει αν το Object που περνάμε σαν παράμετρο είναι της ίδιας κλάσης (Srow),
     *  4. ελέγχει αν τα properties α είναι ίδια.
      */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || this.getClass() != o.getClass())
        {
            return false;
        }

        SRow sRow = (SRow) o;

        if (!a.equals(sRow.a)) {
            return false;
        }
        return true;
    }

    /**
     * Δημιουργεί το hashcode του Object.
     */   
    @Override
    public int hashCode() {
        return a.hashCode();
    }
}
