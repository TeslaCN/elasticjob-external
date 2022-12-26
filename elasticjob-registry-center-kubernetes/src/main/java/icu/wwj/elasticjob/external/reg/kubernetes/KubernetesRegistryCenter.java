package icu.wwj.elasticjob.external.reg.kubernetes;

import com.google.gson.reflect.TypeToken;
import io.kubernetes.client.custom.V1Patch;
import io.kubernetes.client.extended.leaderelection.LeaderElectionConfig;
import io.kubernetes.client.extended.leaderelection.LeaderElector;
import io.kubernetes.client.extended.leaderelection.Lock;
import io.kubernetes.client.extended.leaderelection.resourcelock.LeaseLock;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoordinationV1Api;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1Lease;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.util.Config;
import io.kubernetes.client.util.PatchUtils;
import io.kubernetes.client.util.Watch;
import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.elasticjob.reg.base.CoordinatorRegistryCenter;
import org.apache.shardingsphere.elasticjob.reg.base.LeaderExecutionCallback;
import org.apache.shardingsphere.elasticjob.reg.base.transaction.TransactionOperation;
import org.apache.shardingsphere.elasticjob.reg.exception.RegException;
import org.apache.shardingsphere.elasticjob.reg.listener.ConnectionStateChangedEventListener;
import org.apache.shardingsphere.elasticjob.reg.listener.DataChangedEvent;
import org.apache.shardingsphere.elasticjob.reg.listener.DataChangedEvent.Type;
import org.apache.shardingsphere.elasticjob.reg.listener.DataChangedEventListener;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Kubernetes registry center.
 */
@Slf4j
public final class KubernetesRegistryCenter implements CoordinatorRegistryCenter {
    
    private static final String LABEL_PREFIX = "elasticjob.shardingsphere.apache.org/";
    
    private static final String JOB_NAME_LABEL = LABEL_PREFIX + "jobName";
    
    private static final String ELASTICJOB_NAMESPACE_LABEL = LABEL_PREFIX + "namespace";
    
    private static final Pattern NAME_PATTERN = Pattern.compile("[a-z0-9]([-a-z0-9]*[a-z0-9])?([a-z0-9]([-a-z0-9]*[a-z0-9])?)*");
    
    private static final Pattern KEY_PATTERN = Pattern.compile("^/(?<name>" + NAME_PATTERN.pattern() + ").*$");
    
    private final String identity = UUID.randomUUID().toString();
    
    private final String namespace;
    
    private final String namespaceLabelSelector;
    
    private final ApiClient apiClient;
    
    private final CoreV1Api coreV1Api;
    
    private final CoordinationV1Api coordinationV1Api;
    
    private final Executor eventCallbackExecutor = Executors.newCachedThreadPool();
    
    private final Map<String, Map<String, String>> cachedConfigMaps = new HashMap<>();
    
    private final Map<String, List<DataChangedEventListener>> pathToListeners = new ConcurrentHashMap<>();
    
    private final Thread configMapWatchThread = new Thread(this::watchConfigMap);
    
    private final Thread leaseWatchThread = new Thread(this::watchLease);
    
    public KubernetesRegistryCenter(final String namespace) throws IOException {
        this.namespace = namespace;
        namespaceLabelSelector = ELASTICJOB_NAMESPACE_LABEL + "=" + namespace;
        apiClient = Config.defaultClient();
        coreV1Api = new CoreV1Api(apiClient);
        coordinationV1Api = new CoordinationV1Api(apiClient);
    }
    
    @Override
    public void init() {
        configMapWatchThread.start();
        leaseWatchThread.start();
    }
    
    @Override
    public void close() {
        configMapWatchThread.interrupt();
        leaseWatchThread.interrupt();
    }
    
    private void watchConfigMap() {
        for (; ; ) {
            try {
                Watch<V1ConfigMap> watch = Watch.createWatch(apiClient,
                        coreV1Api.listNamespacedConfigMapCall(namespace, null, null, null, null, namespaceLabelSelector, 1, null, null, null, Boolean.TRUE, null),
                        new TypeToken<Watch.Response<V1ConfigMap>>() {
                        }.getType());
                for (Watch.Response<V1ConfigMap> each : watch) {
                    log.debug("ConfigMap {} {}", each.object.getMetadata().getName(), each.type);
                    String configMapName = each.object.getMetadata().getName();
                    Map<String, String> data = Collections.unmodifiableMap(Optional.ofNullable(each.object.getData()).orElseGet(Collections::emptyMap));
                    switch (each.type) {
                        case "ADDED":
                            cachedConfigMaps.put(configMapName, data);
                            handleConfigMapAddedOrDeleted(data, Type.ADDED);
                            break;
                        case "MODIFIED":
                            handleConfigMapModified(cachedConfigMaps.get(configMapName), data);
                            cachedConfigMaps.put(configMapName, data);
                            break;
                        case "DELETED":
                            handleConfigMapAddedOrDeleted(data, Type.DELETED);
                            cachedConfigMaps.remove(configMapName);
                            break;
                        case "ERROR":
                        default:
                    }
                }
            } catch (final ApiException ex) {
                log.error(ex.getResponseBody(), ex);
                
            } catch (final Exception ex) {
                log.error(ex.getMessage(), ex);
            }
        }
    }
    
    private void handleConfigMapModified(final Map<String, String> before, final Map<String, String> after) {
        Map<String, String> added = new HashMap<>(after);
        added.keySet().removeAll(before.keySet());
        handleConfigMapAddedOrDeleted(added, Type.ADDED);
        Map<String, String> deleted = new HashMap<>(before);
        deleted.keySet().removeAll(after.keySet());
        handleConfigMapAddedOrDeleted(deleted, Type.DELETED);
        Set<String> existing = new HashSet<>(after.keySet());
        existing.removeAll(added.keySet());
        for (String each : existing) {
            String valueBefore = before.get(each);
            String valueAfter = after.get(each);
            if (valueBefore == valueAfter || valueBefore.equals(valueAfter)) {
                continue;
            }
            DataChangedEvent event = null;
            for (Entry<String, List<DataChangedEventListener>> entry : pathToListeners.entrySet()) {
                if (!each.startsWith(entry.getKey())) {
                    continue;
                }
                if (null == event) {
                    event = new DataChangedEvent(Type.UPDATED, revertDataKey(each, true), valueAfter);
                }
                for (DataChangedEventListener eachListener : entry.getValue()) {
                    DataChangedEvent finalEvent = event;
                    eventCallbackExecutor.execute(() -> eachListener.onChange(finalEvent));
                }
            }
        }
    }
    
    private void handleConfigMapAddedOrDeleted(final Map<String, String> data, final Type type) {
        for (Entry<String, String> dataEntry : data.entrySet()) {
            for (Entry<String, List<DataChangedEventListener>> entry : pathToListeners.entrySet()) {
                String changedKey = dataEntry.getKey();
                if (!changedKey.startsWith(entry.getKey())) {
                    continue;
                }
                DataChangedEvent event = new DataChangedEvent(type, revertDataKey(changedKey, true), dataEntry.getValue());
                for (DataChangedEventListener eachListener : entry.getValue()) {
                    eventCallbackExecutor.execute(() -> eachListener.onChange(event));
                }
            }
        }
    }
    
    private void watchLease() {
        for (; ; ) {
            try {
                Watch<V1Lease> watch = Watch.createWatch(apiClient,
                        coordinationV1Api.listNamespacedLeaseCall(namespace, null, null, null, null, namespaceLabelSelector, null, null, null, null, Boolean.TRUE, null),
                        new TypeToken<Watch.Response<V1Lease>>() {
                        }.getType());
                for (Watch.Response<V1Lease> each : watch) {
                    switch (each.type) {
                        case "ADDED":
                        
                        case "MODIFIED":
                        
                        case "DELETED":
                        
                        case "ERROR":
                        
                        default:
                    }
                }
            } catch (final ApiException ex) {
                log.error(ex.getResponseBody(), ex);
            }
        }
    }
    
    @Override
    public String getDirectly(final String key) {
        try {
            V1ConfigMap configMap = coreV1Api.readNamespacedConfigMap(getConfigMapName(key), namespace, null);
            return unescapedValue(configMap.getData().get(getDataKey(key)));
        } catch (final ApiException ex) {
            throw new RegException(ex);
        }
    }
    
    @Override
    public List<String> getChildrenKeys(final String key) {
        try {
            V1ConfigMap configMap = coreV1Api.readNamespacedConfigMap(getConfigMapName(key), namespace, null);
            String prefix = getDataKey(key) + (key.endsWith("/") ? "" : ".");
            return configMap.getData().keySet().stream().filter(each -> each.startsWith(prefix)).map(each -> revertDataKey(each.substring(prefix.length()).split("\\.", 2)[0], false))
                    .sorted(Comparator.reverseOrder()).collect(Collectors.toList());
        } catch (final ApiException ex) {
            throw new RegException(ex);
        }
    }
    
    @Override
    public int getNumChildren(final String key) {
        return getChildrenKeys(key).size();
    }
    
    @Override
    public void persistEphemeral(final String key, final String value) {
        // TODO This is ephemeral node!
        persist(key, value);
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
        String leaseName = getConfigMapName(key) + '-' + getDataKey(key);
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
        pathToListeners.computeIfAbsent(getDataKey(key), k -> new CopyOnWriteArrayList<>()).add(listener);
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
                return Optional.of("{\"op\":\"add\",\"path\":\"/data/" + getDataKey(operation.getKey()) + "\",\"value\":\"" + escapeValue(operation.getValue()) + "\"}");
            case UPDATE:
                return Optional.of("{\"op\":\"replace\",\"path\":\"/data/" + getDataKey(operation.getKey()) + "\",\"value\":\"" + escapeValue(operation.getValue()) + "\"}");
            case DELETE:
                return Optional.of("{\"op\":\"remove\",\"path\":\"/data/" + getDataKey(operation.getKey()) + "\"}");
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
            return coreV1Api.readNamespacedConfigMap(getConfigMapName(key), namespace, null).getData().containsKey(getDataKey(key));
        } catch (final ApiException ignored) {
            return false;
        }
    }
    
    @Override
    public void persist(final String key, final String value) {
        String dataKey = getDataKey(key);
        String jsonPatchString = "[{\"op\":\"add\",\"path\":\"/data/" + dataKey + "\",\"value\":\"" + escapeValue(value) + "\"}]";
        try {
            doJsonPatch(key, jsonPatchString);
        } catch (final ApiException ex) {
            if (404 != ex.getCode()) {
                throw new RegException(ex);
            }
            createConfigMap(key, value);
        }
    }
    
    private void createConfigMap(final String key, final String value) {
        String configMapName = getConfigMapName(key);
        V1ObjectMeta metadata = new V1ObjectMeta().name(configMapName).putLabelsItem(JOB_NAME_LABEL, configMapName).putLabelsItem(ELASTICJOB_NAMESPACE_LABEL, namespace);
        V1ConfigMap configMap = new V1ConfigMap().putDataItem(getDataKey(key), escapeValue(value)).metadata(metadata);
        try {
            coreV1Api.createNamespacedConfigMap(namespace, configMap, null, null, null, null);
        } catch (final ApiException ex) {
            throw new RegException(ex);
        }
    }
    
    @Override
    public void update(final String key, final String value) {
        String jsonPatchString = "[{\"op\":\"replace\",\"path\":\"/data/" + getDataKey(key) + "\",\"value\":\"" + escapeValue(value) + "\"}]";
        try {
            doJsonPatch(key, jsonPatchString);
        } catch (final ApiException ex) {
            throw new RegException(ex);
        }
    }
    
    @Override
    public void remove(final String key) {
        String jsonPatchString = "[{\"op\":\"remove\",\"path\":\"/data/" + getDataKey(key) + "\"}]";
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
    
    private String getDataKey(final String key) {
        return key.substring(1).replace("@", "__at__").replace(".", "__dot__").replace('/', '.');
    }
    
    private String revertDataKey(final String dataKey, final boolean absolutePath) {
        return (absolutePath ? '/' : "") + dataKey.replace('.', '/').replace("__dot__", ".").replace("__at__", "@");
    }
    
    private String escapeValue(final String value) {
        return value.replace("\n", "\\n");
    }
    
    private String unescapedValue(final String escapedValue) {
        return null == escapedValue ? null : escapedValue.replace("\\n", "\n");
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