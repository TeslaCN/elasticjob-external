package icu.wwj.elasticjob.external.reg.kubernetes.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import icu.wwj.elasticjob.external.reg.kubernetes.model.json.Base64ValueJsonDeserializer;
import icu.wwj.elasticjob.external.reg.kubernetes.model.json.Base64ValueJsonSerializer;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Data
public final class NodeValue {
    
    private boolean ephemeral;
    
    @JsonSerialize(using = Base64ValueJsonSerializer.class)
    @JsonDeserialize(using = Base64ValueJsonDeserializer.class)
    private String value;
    
    private long expiredAt;
    
    public NodeValue(final String value) {
        this(false, value, Long.MAX_VALUE);
    }
}
