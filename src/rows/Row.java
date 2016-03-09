package rows;

/**
 * Ορισμός των βασικών μεθόδων που κληρονομούν όλα τα objects τύπου Row.
 * Το interface Row αντιπροσωπεύει την κάθε γραμμή του αρχείου εισόδου είτε
 * προέρχεται από τον πίνακα R είτε από τον πίνακα S.
 */


public interface Row extends Comparable<Row> {
    String getOrigin();

    Integer getA();

    void setA(Integer a);
}
