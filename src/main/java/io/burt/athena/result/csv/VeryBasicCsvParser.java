package io.burt.athena.result.csv;

import java.io.IOException;
import java.io.Reader;
import java.util.Iterator;

public class VeryBasicCsvParser implements Iterator<String[]> {
    private final Reader csv;
    private final int columnCount;
    private String[] nextRow;
    private int nextChar0;
    private int nextChar1;

    public VeryBasicCsvParser(Reader csv, int columnCount) {
        this.csv = csv;
        this.columnCount = columnCount;
        this.nextRow = null;
        this.nextChar0 = -1;
        this.nextChar1 = -1;
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
        } catch (IOException | IllegalStateException e) {
            nextRow = null;
            return false;
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
    }

    private void consumeQuote() throws IOException, IllegalStateException {
        if (nextChar0 == '"') {
            advance();
        } else {
            throw new IllegalStateException(String.format("Expected quote but found %c", nextChar0));
        }
    }

    private void consumeComma() throws IOException, IllegalStateException {
        if (nextChar0 == ',') {
            advance();
        } else {
            throw new IllegalStateException(String.format("Expected comma but found %c", nextChar0));
        }
    }

    private void consumeNewline() throws IOException, IllegalStateException {
        if (nextChar0 == '\n') {
            advance();
        } else {
            throw new IllegalStateException(String.format("Expected newline but found %c", nextChar0));
        }
    }

    private String consumeString() throws IOException, IllegalStateException {
        StringBuilder builder = new StringBuilder();
        while (nextChar0 != '"' || nextChar1 == '"') {
            if (nextChar0 == -1) {
                throw new IllegalStateException("Stream ended in the middle of a string");
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
