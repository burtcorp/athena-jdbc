package io.burt.athena.result.protobuf;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.InputStream;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(MockitoExtension.class)
class VeryBasicProtobufParserTest {
    private VeryBasicProtobufParser parser;

    private InputStream smallInput() {
        return getClass().getResourceAsStream("/protobuf/small.bin");
    }

    private InputStream mediumInput() {
        return getClass().getResourceAsStream("/protobuf/medium.bin");
    }

    @BeforeEach
    void setUp() {
        parser = new VeryBasicProtobufParser();
    }

    @Nested
    class Parse {
        @Nested
        class WithNoRepetitions {
            @Test
            void returnsAListOfFields() throws Exception {
                List<Field> fields = parser.parse(smallInput());
                assertEquals(2, fields.size());
                assertEquals(1, fields.get(0).getNumber());
                assertEquals(4, fields.get(1).getNumber());
                assertEquals(new BinaryField(1, "20190423_125128_00001_ehtur".getBytes()), fields.get(0));
                assertEquals(25, ((BinaryField) fields.get(1)).getContents().length);
            }
        }

        @Nested
        class WithRepetitions {
            @Test
            void returnsAListOfFieldsContainingRepeatedFields() throws Exception {
                List<Field> fields = parser.parse(mediumInput());
                assertEquals(4, fields.size());
                assertEquals(1, fields.get(0).getNumber());
                assertEquals(4, fields.get(1).getNumber());
                assertEquals(4, fields.get(1).getNumber());
                assertEquals(4, fields.get(1).getNumber());
                assertEquals(new BinaryField(1, "20190513_152236_00114_n7swu".getBytes()), fields.get(0));
                assertEquals(37, ((BinaryField) fields.get(1)).getContents().length);
                assertEquals(37, ((BinaryField) fields.get(2)).getContents().length);
                assertEquals(37, ((BinaryField) fields.get(3)).getContents().length);
            }
        }

        @Nested
        class WithDifferentTypes {
            @Test
            void returnsAListOfFieldsOfDifferentTypes() throws Exception {
                List<Field> outerFields = parser.parse(mediumInput());
                List<Field> innerFields = parser.parse(((BinaryField) outerFields.get(1)).getContents());
                assertEquals(8, innerFields.size());
                assertEquals(1, innerFields.get(0).getNumber());
                assertEquals(4, innerFields.get(1).getNumber());
                assertEquals(5, innerFields.get(2).getNumber());
                assertEquals(6, innerFields.get(3).getNumber());
                assertEquals(7, innerFields.get(4).getNumber());
                assertEquals(8, innerFields.get(5).getNumber());
                assertEquals(9, innerFields.get(6).getNumber());
                assertEquals(10, innerFields.get(7).getNumber());
                assertArrayEquals("hive".getBytes(), ((BinaryField) innerFields.get(0)).getContents());
                assertArrayEquals("_col0".getBytes(), ((BinaryField) innerFields.get(1)).getContents());
                assertArrayEquals("_col0".getBytes(), ((BinaryField) innerFields.get(2)).getContents());
                assertArrayEquals("integer".getBytes(), ((BinaryField) innerFields.get(3)).getContents());
                assertEquals(10, ((IntegerField) innerFields.get(4)).getValue());
                assertEquals(0, ((IntegerField) innerFields.get(5)).getValue());
                assertEquals(3, ((IntegerField) innerFields.get(6)).getValue());
                assertEquals(0, ((IntegerField) innerFields.get(7)).getValue());
            }
        }
    }
}
