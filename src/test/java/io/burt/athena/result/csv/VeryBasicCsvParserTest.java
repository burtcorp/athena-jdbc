package io.burt.athena.result.csv;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.Reader;
import java.io.StringReader;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
class VeryBasicCsvParserTest {
    private VeryBasicCsvParser parser;

    private Reader createInput(String[][] rows) {
        StringBuilder builder = new StringBuilder();
        for (String[] row : rows) {
            for (String value : row) {
                if (value != null) {
                    builder.append("\"").append(value).append("\"");
                }
                builder.append(",");
            }
            builder.deleteCharAt(builder.length() - 1);
            builder.append("\n");
        }
        return new StringReader(builder.toString());
    }

    @Nested
    class HasNext {
        @Nested
        class WhenTheInputIsEmpty {
            @BeforeEach
            void setUp() {
                parser = new VeryBasicCsvParser(createInput(new String[0][0]), 3);
            }

            @Test
            void returnsFalse() {
                assertFalse(parser.hasNext());
            }
        }

        @Nested
        class WhenTheInputIsNotEmpty {
            @BeforeEach
            void setUp() {
                parser = new VeryBasicCsvParser(createInput(new String[][]{
                        new String[]{"r0c0", "r0c1", "r0c2"},
                        new String[]{"r1c0", "r1c1", "r1c2"},
                        new String[]{"r2c0", "r2c1", "r2c2"}
                }), 3);
            }

            @Test
            void returnsTrue() {
                assertTrue(parser.hasNext());
            }

            @Test
            void returnsTrueWhileThereAreStillRows() {
                assertTrue(parser.hasNext());
                parser.next();
                assertTrue(parser.hasNext());
                parser.next();
                assertTrue(parser.hasNext());
                parser.next();
                assertFalse(parser.hasNext());
            }

            @Test
            void doesNotConsumeMoreThanOneRow() {
                parser.hasNext();
                parser.hasNext();
                parser.hasNext();
                assertArrayEquals(new String[]{"r0c0", "r0c1", "r0c2"}, parser.next());
                parser.hasNext();
                parser.hasNext();
                parser.hasNext();
                assertArrayEquals(new String[]{"r1c0", "r1c1", "r1c2"}, parser.next());
            }
        }
    }

    @Nested
    class Next {
        @Nested
        class WhenTheInputIsEmpty {
            @BeforeEach
            void setUp() {
                parser = new VeryBasicCsvParser(createInput(new String[0][0]), 3);
            }

            @Test
            void returnsNull() {
                assertNull(parser.next());
            }
        }

        @Nested
        class WhenTheInputIsNotEmpty {
            @BeforeEach
            void setUp() {
                parser = new VeryBasicCsvParser(createInput(new String[][]{
                        new String[]{"r0c0", "r0c1", "r0c2"},
                        new String[]{"r1c0", "r1c1", "r1c2"},
                        new String[]{"r2c0", "r2c1", "r2c2"}
                }), 3);
            }

            @Test
            void returnsTheRowsSplitIntoColumns() {
                assertArrayEquals(new String[]{"r0c0", "r0c1", "r0c2"}, parser.next());
                assertArrayEquals(new String[]{"r1c0", "r1c1", "r1c2"}, parser.next());
                assertArrayEquals(new String[]{"r2c0", "r2c1", "r2c2"}, parser.next());
            }

            @Test
            void returnsNullWhenThereAreNoMoreRows() {
                parser.next();
                parser.next();
                parser.next();
                assertNull(parser.next());
            }
        }

        @Nested
        class WithEscapedQuotes {
            @BeforeEach
            void setUp() {
                parser = new VeryBasicCsvParser(createInput(new String[][]{
                        new String[]{"r\"\"0\"\"c0", "r0c\"\"1\"\"", "r0c2"},
                        new String[]{"r1c0", "r1c1", "r1c2"},
                        new String[]{"r2c0", "r2c1", "r2c2"}
                }), 3);
            }

            @Test
            void unescapesTheQuotes() {
                assertArrayEquals(new String[]{"r\"0\"c0", "r0c\"1\"", "r0c2"}, parser.next());
            }
        }

        @Nested
        class WithNullValues {
            @BeforeEach
            void setUp() {
                parser = new VeryBasicCsvParser(createInput(new String[][]{
                        new String[]{"r0c0", null, "r0c2"},
                        new String[]{"r1c0", "r1c1", null},
                        new String[]{"r2c0", "r2c1", "r2c2"}
                }), 3);
            }

            @Test
            void returnsNulls() {
                assertArrayEquals(new String[]{"r0c0", null, "r0c2"}, parser.next());
                assertArrayEquals(new String[]{"r1c0", "r1c1", null}, parser.next());
            }
        }

        @Nested
        class WhenTheInputIsMalformed {
            @Nested
            class AndARowHAsTooFewColumns {
                @BeforeEach
                void setUp() {
                    parser = new VeryBasicCsvParser(createInput(new String[][]{
                            new String[]{"r\"\"0\"\"c0", "r0c\"\"1\"\"", "r0c2"},
                            new String[]{"r1c0", "r1c1"},
                            new String[]{"r2c0", "r2c1", "r2c2"}
                    }), 3);
                }

                @Test
                void returnsNull() {
                    parser.next();
                    assertNull(parser.next());
                }
            }

            @Nested
            class AndAQuoteIsNotEscaped {
                @BeforeEach
                void setUp() {
                    parser = new VeryBasicCsvParser(createInput(new String[][]{
                            new String[]{"r\"\"0\"\"c0", "r0c\"1\"\"", "r0c2"},
                            new String[]{"r1c0", "r1c1", "r1c2"},
                            new String[]{"r2c0", "r2c1", "r2c2"}
                    }), 3);
                }

                @Test
                void returnsNull() {
                    assertNull(parser.next());
                }
            }
        }
    }
}
