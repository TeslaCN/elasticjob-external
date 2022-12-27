package icu.wwj.elasticjob.external.reg.kubernetes.model.json;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

public final class Base64ValueJsonSerializer extends JsonSerializer<String> {
    
    @Override
    public void serialize(final String value, final JsonGenerator gen, final SerializerProvider serializers) throws IOException {
        gen.writeString(Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8)));
    }
}
