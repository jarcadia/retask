package dev.jarcadia;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.ContextualDeserializer;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class RetaskJson {

	public static ObjectMapper decorate(ObjectMapper objectMapper) {
		SimpleModule module = new SimpleModule();
		module.addDeserializer(Fields.class, new FieldsDeserializer(objectMapper));
		module.addDeserializer(UV.class, new UVDeserializer());
		module.addSerializer(new OptionalSerializer());
		module.addDeserializer(Optional.class, new OptionalDeserializer());
		objectMapper.registerModule(module);
		return objectMapper;
	}

	private static class OptionalSerializer extends StdSerializer<Optional<?>> {

	    public OptionalSerializer() {
	        super(Optional.class, false);
	    }

		@Override
		public void serialize(Optional<?> value, JsonGenerator gen, SerializerProvider provider) throws IOException {
			if (value.isPresent()) {
				gen.writeObject(value.get());
			} else {
				gen.writeNull();
			}
		}
	}

	private static class UVDeserializer extends JsonDeserializer<UV<?>> implements ContextualDeserializer {

		private final JavaType innerType;

		UVDeserializer() {
			innerType = null;
		}

		UVDeserializer(JavaType innerType) {
			this.innerType = innerType;
		}

		@Override
		public JsonDeserializer<?> createContextual(DeserializationContext ctxt, BeanProperty property) throws JsonMappingException {
			JavaType type = ctxt.getContextualType().containedType(0);
			return new UVDeserializer(type);
		}

		@Override
		public UV<?> deserialize(JsonParser parser, DeserializationContext ctxt) throws IOException {
			if (parser.currentToken() != JsonToken.START_ARRAY) {
				throw new IllegalStateException("Expected an array");
			}

			parser.nextToken();
			Object pre = ctxt.readValue(parser, this.innerType);

			parser.nextToken();
			Object post = ctxt.readValue(parser, this.innerType);

			if (parser.nextToken() != JsonToken.END_ARRAY) {
				throw new IllegalStateException("Expected an array");
			}

			return new UV<>(pre, post);
		}
	}

	private static class FieldsDeserializer extends StdDeserializer<Fields> {

		private final ObjectMapper objectMapper;

		public FieldsDeserializer(ObjectMapper objectMapper) {
			super(Fields.class);
			this.objectMapper = objectMapper;
		}

		@Override
		public Fields deserialize(JsonParser parser, DeserializationContext deserializer) throws IOException {
			if (parser.currentToken() != JsonToken.START_OBJECT) {
				throw new IllegalStateException("Expected a JSON object");
			}
			byte[] source = (byte[]) parser.getCurrentLocation().getSourceRef();

			parser.getInputSource();


			Map<String, FieldLocation> fields = new HashMap<>();

			JsonToken token;
			String name;
			int start;
			while (parser.nextToken() != JsonToken.END_OBJECT) {
				name = parser.getText();
				token = parser.nextToken();
				start = (int) parser.getCurrentLocation().getByteOffset() - 1;
				if (token.isStructStart()) {
					parser.skipChildren();
				} else if (token == JsonToken.VALUE_STRING) {
					parser.finishToken();
				} else {
					start -= parser.getTextLength();
				}
				fields.put(name, new FieldLocation(start, (int) parser.getCurrentLocation().getByteOffset()));
			}
			return new Fields(objectMapper, source, fields);
		}
	}

	private static class OptionalDeserializer extends JsonDeserializer<Optional<?>> implements ContextualDeserializer {

		private final JavaType innerType;

		OptionalDeserializer() {
			innerType = null;
		}

		OptionalDeserializer(JavaType innerType) {
			this.innerType = innerType;
		}

		@Override
		public JsonDeserializer<?> createContextual(DeserializationContext ctxt, BeanProperty property) throws JsonMappingException {
			JavaType type = ctxt.getContextualType().containedType(0);
			return new OptionalDeserializer(type);
		}

		@Override
		public Optional<?> deserialize(JsonParser parser, DeserializationContext ctxt) throws IOException {
			// Jackson will not invoke this deserializer if the JSON is null, instead null is returned directly.
			// Therefore returned nulls for Optionals must be handled outside deserialization
			return Optional.of(ctxt.readValue(parser, innerType));
		}
	}

}
