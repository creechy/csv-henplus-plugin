package org.fakebelieve.henplus.plugins.csv;

import org.supercsv.prefs.CsvPreference;
import org.supercsv.quote.AlwaysQuoteMode;
import org.supercsv.quote.NormalQuoteMode;

/**
 * Created by mock on 3/28/17.
 */
public class CsvPreferenceBuilder {

    private char delimiterChar = ',';
    private char quoteChar = '"';
    private String endOfLineSymbols = "\n";
    private String quoteMode = "normal";
    private String surroundingSpaces = "need-quotes";
    private String emptyLines = "ignore";


    public CsvPreferenceBuilder withDelimiterChar(char delimiterChar) {
        this.delimiterChar = delimiterChar;
        return this;
    }

    public CsvPreferenceBuilder withQuoteChar(char quoteChar) {
        this.quoteChar = quoteChar;
        return this;
    }

    public CsvPreferenceBuilder withQuoteMode(String quoteMode) {
        this.quoteMode = quoteMode;
        return this;
    }

    public CsvPreferenceBuilder withEndOfLineSymbols(String endOfLineSymbols) {
        this.endOfLineSymbols = endOfLineSymbols;
        return this;
    }

    public CsvPreferenceBuilder withSurroundingSpaces(String surroundingSpacesNeedQuotes) {
        this.surroundingSpaces = surroundingSpacesNeedQuotes;
        return this;
    }

    public CsvPreferenceBuilder withEmptyLines(String ignoreEmptyLines) {
        this.emptyLines = ignoreEmptyLines;
        return this;
    }

    public CsvPreference build() {
        CsvPreference.Builder builder = new CsvPreference.Builder(quoteChar, delimiterChar, endOfLineSymbols);
        builder.ignoreEmptyLines(emptyLines.equals("ignore"));
        builder.surroundingSpacesNeedQuotes(surroundingSpaces.equals("need-quotes"));
        builder.useQuoteMode(quoteMode.equals("always") ? new AlwaysQuoteMode() : new NormalQuoteMode());

        return builder.build();
    }
}
