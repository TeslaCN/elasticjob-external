package icu.wwj.elasticjob.external.reg.kubernetes.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class JsonPatch {
    
    private final String op;
    
    private final String path;
    
    private String value;
}
