package icu.wwj.elasticjob.external.reg.kubernetes.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

class NodeValueTest {
    
    @Test
    void testEncode() throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        NodeValue expected = new NodeValue(false, "hello", 10000L);
        String encoded = mapper.writeValueAsString(expected);
        Map<String, String> map = mapper.readValue(encoded, new TypeReference<Map<String, String>>() {
        });
        Assertions.assertEquals(Base64.getEncoder().encodeToString("hello".getBytes(StandardCharsets.UTF_8)), map.get("value"));
        NodeValue decoded = mapper.readValue(encoded, NodeValue.class);
        Assertions.assertEquals(expected, decoded);
    }
}
