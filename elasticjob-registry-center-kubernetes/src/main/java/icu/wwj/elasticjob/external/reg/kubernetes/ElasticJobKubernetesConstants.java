package icu.wwj.elasticjob.external.reg.kubernetes;

import java.util.regex.Pattern;

public class ElasticJobKubernetesConstants {
    
    public static final String LABEL_PREFIX = "elasticjob.shardingsphere.apache.org/";
    
    public static final String ELASTICJOB_NAMESPACE_LABEL = LABEL_PREFIX + "namespace";
    
    public static final String JOB_NAME_LABEL = LABEL_PREFIX + "jobName";
    
    public static final Pattern NAME_PATTERN = Pattern.compile("[a-z0-9]([-a-z0-9]*[a-z0-9])?([a-z0-9]([-a-z0-9]*[a-z0-9])?)*");
}
