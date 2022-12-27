package icu.wwj.elasticjob.external.reg.kubernetes.model.json;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

public final class Base64ValueJsonDeserializer extends JsonDeserializer<String> {
    
    @Override
    public String deserialize(final JsonParser p, final DeserializationContext ctxt) throws IOException {
        return new String(Base64.getDecoder().decode(p.readValueAs(String.class)), StandardCharsets.UTF_8);
    }
}
