package io.burt.athena.result.csv;

import java.io.IOException;
import java.io.Reader;
import java.text.ParseException;
import java.util.Iterator;

public class VeryBasicCsvParser implements Iterator<String[]> {
    private final Reader csv;
    private final int columnCount;
    private String[] nextRow;
    private int nextChar0;
    private int nextChar1;
    private int position;

    public VeryBasicCsvParser(Reader csv, int columnCount) {
        this.csv = csv;
        this.columnCount = columnCount;
        this.nextRow = null;
        this.nextChar0 = -1;
        this.nextChar1 = -1;
        this.position = -1;
    }

    @Override
    public String[] next() {
        if (nextRow == null) {
            loadNext();
        }
        String[] n = nextRow;
        nextRow = null;
        return n;
    }

    private boolean loadNext() {
        try {
            if (nextChar0 == -1) {
                advance();
            }
            if (nextChar0 == -1) {
                return false;
            }
            nextRow = new String[columnCount];
            for (int i = 0; i < columnCount; i++) {
                if (nextChar0 == ',' || nextChar0 == '\n') {
                    advance();
                    nextRow[i] = null;
                } else {
                    consumeQuote();
                    nextRow[i] = consumeString();
                    consumeQuote();
                    if (i == columnCount - 1) {
                        consumeNewline();
                    } else {
                        consumeComma();
                    }
                }
            }
            return true;
        } catch (IOException | ParseException e) {
            nextRow = null;
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean hasNext() {
        return nextRow != null || loadNext();
    }

    private void advance() throws IOException {
        if (nextChar0 == -1) {
            nextChar0 = csv.read();
        } else {
            nextChar0 = nextChar1;
        }
        nextChar1 = csv.read();
        position++;
    }

    private String charToString(int chr) {
        if (chr == '\n') {
            return "\\n";
        } else {
            return String.format("%c", chr);
        }
    }

    private void consumeQuote() throws IOException, ParseException {
        if (nextChar0 == '"') {
            advance();
        } else {
            throw new ParseException(String.format("Expected quote but found \"%s\"", charToString(nextChar0)), position);
        }
    }

    private void consumeComma() throws IOException, ParseException {
        if (nextChar0 == ',') {
            advance();
        } else {
            throw new ParseException(String.format("Expected comma but found \"%s\"", charToString(nextChar0)), position);
        }
    }

    private void consumeNewline() throws IOException, ParseException {
        if (nextChar0 == '\n') {
            advance();
        } else {
            throw new ParseException(String.format("Expected newline but found \"%s\"", charToString(nextChar0)), position);
        }
    }

    private String consumeString() throws IOException, ParseException {
        StringBuilder builder = new StringBuilder();
        while (nextChar0 != '"' || nextChar1 == '"') {
            if (nextChar0 == -1) {
                throw new ParseException("Unexpected end of stream", position);
            }
            if (nextChar0 == '"') {
                advance();
            }
            builder.appendCodePoint(nextChar0);
            advance();
        }
        return builder.toString();
    }
}
