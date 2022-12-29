package icu.wwj.elasticjob.external.reg.kubernetes.internal;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class Escapes {
    
    public static String getDataKey(final String key) {
        return key.substring(1).replace("@", "__at__").replace(".", "__dot__").replace('/', '.');
    }
    
    public static String revertDataKey(final String dataKey, final boolean absolutePath) {
        return (absolutePath ? '/' : "") + dataKey.replace('.', '/').replace("__dot__", ".").replace("__at__", "@");
    }
    
    public static String escapeDoubleQuote(final String value) {
        return value.replace("\"", "\\\"");
    }
}
