package parser;

import rows.Row;

/**
 * Ορισμός των βασικών μεθόδων που κληρονομούν όλα τα objects τύπου RowParser.
 */

public interface RowParser {

    Row convertFromString (String inputRow);

    String convertToString (Row row);
}
