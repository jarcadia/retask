package dev.jarcadia;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

public class RetaskJsonUnitTest {

    static ObjectMapper mapper;

    @BeforeAll
    static void init() {
        mapper = RetaskJson.decorate(new ObjectMapper());
    }

    @Test
    void serializesEmptyOptional() throws JsonProcessingException {
        assertSerialized("null", Optional.empty());
    }

    @Test
    void serializesPresentOptional() throws JsonProcessingException {
        Optional<String> opt = Optional.of("Hello World");
        assertSerialized("\"Hello World\"", opt);
    }

    @Test
    void deserializesPresentOptional() throws JsonProcessingException {
        JavaType type = mapper.getTypeFactory().constructParametricType(Optional.class, String.class);
        Optional<String> opt = mapper.readValue("\"Hello World\"", type);
        Assertions.assertTrue(opt.isPresent());
        Assertions.assertEquals("Hello World", opt.get());
    }

    @Test
    void deserializeComplexTaskFieldsBytes() throws IOException {
        JavaType optionalStringType = mapper.getTypeFactory().constructParametricType(Optional.class, String.class);
        Fields taskFields = mapper.readValue("{\"bool\": true, \"integer\": 42, \"decimal\": 42.42, \"str\": \"hello world\", \"obj\": {\"key2\": \"value\"}, \"numArray\": [1, 2, 3], \"alphaArray\": [\"hello\", \"world\"]}".getBytes(StandardCharsets.UTF_8), Fields.class);

        Assertions.assertTrue(taskFields.getFieldAs("bool", boolean.class));
        Assertions.assertEquals(42, taskFields.getFieldAs("integer", int.class));
        Assertions.assertEquals("hello world",
                taskFields.getFieldAs("str", String.class));
        Assertions.assertEquals(Optional.of("hello world"),
                taskFields.getFieldAs("str", optionalStringType));
        Assertions.assertEquals(Optional.empty(),
                taskFields.getFieldAs("missing", optionalStringType));
    }



    public void assertSerialized(String expected, Object obj) throws JsonProcessingException {
        Assertions.assertEquals(expected, mapper.writeValueAsString(obj));
    }

}
