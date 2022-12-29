package icu.wwj.elasticjob.external.reg.kubernetes;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import icu.wwj.elasticjob.external.reg.kubernetes.internal.ConfigMapCache;
import icu.wwj.elasticjob.external.reg.kubernetes.internal.Escapes;
import icu.wwj.elasticjob.external.reg.kubernetes.model.NodeValue;
import io.kubernetes.client.custom.V1Patch;
import io.kubernetes.client.extended.leaderelection.LeaderElectionConfig;
import io.kubernetes.client.extended.leaderelection.LeaderElector;
import io.kubernetes.client.extended.leaderelection.Lock;
import io.kubernetes.client.extended.leaderelection.resourcelock.LeaseLock;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.util.Config;
import io.kubernetes.client.util.PatchUtils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.elasticjob.reg.base.CoordinatorRegistryCenter;
import org.apache.shardingsphere.elasticjob.reg.base.LeaderExecutionCallback;
import org.apache.shardingsphere.elasticjob.reg.base.transaction.TransactionOperation;
import org.apache.shardingsphere.elasticjob.reg.exception.RegException;
import org.apache.shardingsphere.elasticjob.reg.listener.ConnectionStateChangedEventListener;
import org.apache.shardingsphere.elasticjob.reg.listener.DataChangedEventListener;

import java.io.IOException;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Kubernetes registry center.
 */
@Slf4j
public final class KubernetesRegistryCenter implements CoordinatorRegistryCenter {
    
    private static final Pattern KEY_PATTERN = Pattern.compile("^/(?<name>" + ElasticJobKubernetesConstants.NAME_PATTERN.pattern() + ").*$");
    
    private final String identity = UUID.randomUUID().toString();
    
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    private final KubernetesRegistryConfiguration config;
    
    private final String namespace;
    
    private final ApiClient apiClient;
    
    private final CoreV1Api coreV1Api;
    
    private final ConfigMapCache configMapCache;
    
    public KubernetesRegistryCenter(final KubernetesRegistryConfiguration configuration) throws IOException {
        this(configuration, Config.defaultClient());
    }
    
    public KubernetesRegistryCenter(final KubernetesRegistryConfiguration configuration, final ApiClient apiClient) {
        config = configuration;
        namespace = configuration.getNamespace();
        this.apiClient = apiClient;
        coreV1Api = new CoreV1Api(apiClient);
        configMapCache = new ConfigMapCache(apiClient, namespace);
    }
    
    @Override
    public void init() {
        configMapCache.init();
    }
    
    @Override
    public void close() {
        configMapCache.destroy();
    }
    
    @Override
    public String getDirectly(final String key) {
        try {
            V1ConfigMap configMap = coreV1Api.readNamespacedConfigMap(getConfigMapName(key), namespace, null);
            String encodedValue = configMap.getData().get(Escapes.getDataKey(key));
            return null == encodedValue ? null : decodeValue(encodedValue).getValue();
        } catch (final ApiException ex) {
            throw new RegException(ex);
        }
    }
    
    @Override
    public List<String> getChildrenKeys(final String key) {
        try {
            V1ConfigMap configMap = coreV1Api.readNamespacedConfigMap(getConfigMapName(key), namespace, null);
            String prefix = Escapes.getDataKey(key) + (key.endsWith("/") ? "" : ".");
            long current = System.currentTimeMillis();
            return configMap.getData().entrySet().stream().filter(each -> each.getKey().startsWith(prefix))
                    .filter(each -> isValid(current, each.getValue()))
                    .map(each -> Escapes.revertDataKey(each.getKey().substring(prefix.length()).split("\\.", 2)[0], false))
                    .sorted(Comparator.reverseOrder()).collect(Collectors.toList());
        } catch (final ApiException ex) {
            throw new RegException(ex);
        }
    }
    
    private boolean isValid(final long currentMillis, final String encodedValue) {
        return currentMillis < decodeValue(encodedValue).getExpiredAt();
    }
    
    @Override
    public int getNumChildren(final String key) {
        return getChildrenKeys(key).size();
    }
    
    @Override
    public void persistEphemeral(final String key, final String value) {
        persist(key, new NodeValue(true, value, System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(config.getEphemeralEachRenewalSeconds())));
    }
    
    @Override
    public String persistSequential(final String key, final String value) {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public void persistEphemeralSequential(final String key) {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public void addCacheData(final String cachePath) {
        // no op
    }
    
    @Override
    public void evictCacheData(final String cachePath) {
        // no op
    }
    
    @Override
    public Object getRawCache(final String cachePath) {
        return null;
    }
    
    @Override
    public void executeInLeader(final String key, final LeaderExecutionCallback callback) {
        String leaseName = getConfigMapName(key) + '-' + Escapes.getDataKey(key);
        Lock lock = new LeaseLock(namespace, leaseName, identity, apiClient);
        LeaderElectionConfig leaderElectionConfig = new LeaderElectionConfig(lock, Duration.ofMillis(10000), Duration.ofMillis(8000), Duration.ofMillis(2000));
        try (LeaderElector leaderElector = new LeaderElector(leaderElectionConfig)) {
            leaderElector.run(() -> {
                log.debug("Leader acquired");
                callback.execute();
                leaderElector.close();
            }, () -> log.debug("Leader released"));
        }
    }
    
    @Override
    public void watch(final String key, final DataChangedEventListener listener, final Executor executor) {
        configMapCache.watch(key, listener);
    }
    
    @Override
    public void addConnectionStateChangedEventListener(final ConnectionStateChangedEventListener listener) {
    }
    
    @Override
    public void executeInTransaction(final List<TransactionOperation> transactionOperations) {
        if (transactionOperations.isEmpty()) {
            return;
        }
        StringBuilder jsonPatchStringBuilder = new StringBuilder("[");
        transactionOperations.forEach(each -> translateToJsonPatch(each).ifPresent(str -> jsonPatchStringBuilder.append(str).append(',')));
        jsonPatchStringBuilder.setCharAt(jsonPatchStringBuilder.length() - 1, ']');
        try {
            // TODO Get a proper key.
            doJsonPatch(transactionOperations.get(transactionOperations.size() - 1).getKey(), jsonPatchStringBuilder.toString());
        } catch (final ApiException ex) {
            throw new RegException(ex);
        }
    }
    
    private Optional<String> translateToJsonPatch(final TransactionOperation operation) {
        switch (operation.getType()) {
            case ADD:
                return Optional.of("{\"op\":\"add\",\"path\":\"/data/" + Escapes.getDataKey(operation.getKey()) + "\",\"value\":\"" + Escapes.escapeDoubleQuote(encodeValue(new NodeValue(operation.getValue()))) + "\"}");
            case UPDATE:
                return Optional.of("{\"op\":\"replace\",\"path\":\"/data/" + Escapes.getDataKey(operation.getKey()) + "\",\"value\":\"" + Escapes.escapeDoubleQuote(encodeValue(new NodeValue(operation.getValue()))) + "\"}");
            case DELETE:
                return Optional.of("{\"op\":\"remove\",\"path\":\"/data/" + Escapes.getDataKey(operation.getKey()) + "\"}");
            case CHECK_EXISTS:
            default:
                return Optional.empty();
        }
    }
    
    @Override
    public String get(final String key) {
        return getDirectly(key);
    }
    
    @Override
    public boolean isExisted(final String key) {
        try {
            return Optional.ofNullable(coreV1Api.readNamespacedConfigMap(getConfigMapName(key), namespace, null).getData().get(Escapes.getDataKey(key)))
                    .map(this::decodeValue).map(value -> System.currentTimeMillis() < value.getExpiredAt()).orElse(false);
        } catch (final ApiException ignored) {
            return false;
        }
    }
    
    @SneakyThrows(JsonProcessingException.class)
    private NodeValue decodeValue(final String value) {
        return objectMapper.readValue(value, NodeValue.class);
    }
    
    @Override
    public void persist(final String key, final String value) {
        persist(key, new NodeValue(value));
    }
    
    private void persist(final String key, final NodeValue nodeValue) {
        String encodedValue = encodeValue(nodeValue);
        String dataKey = Escapes.getDataKey(key);
        String jsonPatchString = "[{\"op\":\"add\",\"path\":\"/data/" + dataKey + "\",\"value\":\"" + Escapes.escapeDoubleQuote(encodedValue) + "\"}]";
        try {
            doJsonPatch(key, jsonPatchString);
        } catch (final ApiException ex) {
            if (404 != ex.getCode()) {
                throw new RegException(ex);
            }
            createConfigMap(key, encodedValue);
        }
    }
    
    private void createConfigMap(final String key, final String encodedValue) {
        String configMapName = getConfigMapName(key);
        V1ObjectMeta metadata = new V1ObjectMeta().name(configMapName).putLabelsItem(ElasticJobKubernetesConstants.JOB_NAME_LABEL, configMapName).putLabelsItem(ElasticJobKubernetesConstants.ELASTICJOB_NAMESPACE_LABEL, namespace);
        V1ConfigMap configMap = new V1ConfigMap().putDataItem(Escapes.getDataKey(key), encodedValue).metadata(metadata);
        try {
            coreV1Api.createNamespacedConfigMap(namespace, configMap, null, null, null, null);
        } catch (final ApiException ex) {
            throw new RegException(ex);
        }
    }
    
    @Override
    public void update(final String key, final String value) {
        String jsonPatchString = "[{\"op\":\"replace\",\"path\":\"/data/" + Escapes.getDataKey(key) + "\",\"value\":\"" + Escapes.escapeDoubleQuote(encodeValue(new NodeValue(value))) + "\"}]";
        try {
            doJsonPatch(key, jsonPatchString);
        } catch (final ApiException ex) {
            throw new RegException(ex);
        }
    }
    
    @SneakyThrows(JsonProcessingException.class)
    private String encodeValue(final NodeValue nodeValue) {
        return objectMapper.writeValueAsString(nodeValue);
    }
    
    @Override
    public void remove(final String key) {
        String jsonPatchString = "[{\"op\":\"remove\",\"path\":\"/data/" + Escapes.getDataKey(key) + "\"}]";
        try {
            doJsonPatch(key, jsonPatchString);
        } catch (final ApiException ex) {
            throw new RegException(ex);
        }
    }
    
    private void doJsonPatch(final String key, final String jsonPatchString) throws ApiException {
        String configMapName = getConfigMapName(key);
        V1Patch patch = new V1Patch(jsonPatchString);
        PatchUtils.patch(V1ConfigMap.class, () -> coreV1Api.patchNamespacedConfigMapCall(configMapName, namespace, patch, null, null, null, null, null, null), V1Patch.PATCH_FORMAT_JSON_PATCH, apiClient);
    }
    
    private String getConfigMapName(final String key) {
        Matcher matcher = KEY_PATTERN.matcher(key);
        if (!matcher.matches()) {
            throw new IllegalArgumentException(key);
        }
        return matcher.group("name");
    }
    
    @Override
    public long getRegistryCenterTime(final String key) {
        return 0;
    }
    
    @Override
    public Object getRawClient() {
        return apiClient;
    }
}
