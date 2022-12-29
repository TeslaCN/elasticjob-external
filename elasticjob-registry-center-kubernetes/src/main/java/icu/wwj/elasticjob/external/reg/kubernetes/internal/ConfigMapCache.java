package icu.wwj.elasticjob.external.reg.kubernetes.internal;

import com.google.gson.reflect.TypeToken;
import icu.wwj.elasticjob.external.reg.kubernetes.ElasticJobKubernetesConstants;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.util.Watch;
import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.elasticjob.reg.listener.DataChangedEvent;
import org.apache.shardingsphere.elasticjob.reg.listener.DataChangedEventListener;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class ConfigMapCache {
    
    private final ApiClient apiClient;
    
    private final String namespace;
    
    private final CoreV1Api coreV1Api;
    
    private final String namespaceLabelSelector;
    
    private final Executor eventCallbackExecutor = Executors.newSingleThreadExecutor();
    
    private final Map<String, Map<String, String>> cachedConfigMaps = new ConcurrentHashMap<>();
    
    private final Map<String, List<DataChangedEventListener>> pathToListeners = new ConcurrentHashMap<>();
    
    private final Thread configMapWatchThread = new Thread(this::watchConfigMap);
    
    private final AtomicBoolean started = new AtomicBoolean();
    
    public ConfigMapCache(ApiClient apiClient, String namespace) {
        this.apiClient = apiClient;
        this.namespace = namespace;
        coreV1Api = new CoreV1Api(apiClient);
        namespaceLabelSelector = ElasticJobKubernetesConstants.ELASTICJOB_NAMESPACE_LABEL + "=" + namespace;
    }
    
    public void init() {
        started.set(true);
        configMapWatchThread.start();
    }
    
    private void watchConfigMap() {
        while (started.get()) {
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
                            handleConfigMapAddedOrDeleted(data, DataChangedEvent.Type.ADDED);
                            break;
                        case "MODIFIED":
                            handleConfigMapModified(cachedConfigMaps.get(configMapName), data);
                            cachedConfigMaps.put(configMapName, data);
                            break;
                        case "DELETED":
                            handleConfigMapAddedOrDeleted(data, DataChangedEvent.Type.DELETED);
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
        handleConfigMapAddedOrDeleted(added, DataChangedEvent.Type.ADDED);
        Map<String, String> deleted = new HashMap<>(before);
        deleted.keySet().removeAll(after.keySet());
        handleConfigMapAddedOrDeleted(deleted, DataChangedEvent.Type.DELETED);
        Set<String> existing = new HashSet<>(after.keySet());
        existing.removeAll(added.keySet());
        for (String each : existing) {
            String valueBefore = before.get(each);
            String valueAfter = after.get(each);
            if (valueBefore.equals(valueAfter)) {
                continue;
            }
            DataChangedEvent event = null;
            for (Map.Entry<String, List<DataChangedEventListener>> entry : pathToListeners.entrySet()) {
                if (!each.startsWith(entry.getKey())) {
                    continue;
                }
                if (null == event) {
                    event = new DataChangedEvent(DataChangedEvent.Type.UPDATED, Escapes.revertDataKey(each, true), valueAfter);
                }
                for (DataChangedEventListener eachListener : entry.getValue()) {
                    DataChangedEvent finalEvent = event;
                    eventCallbackExecutor.execute(() -> eachListener.onChange(finalEvent));
                }
            }
        }
    }
    
    private void handleConfigMapAddedOrDeleted(final Map<String, String> data, final DataChangedEvent.Type type) {
        for (Map.Entry<String, String> dataEntry : data.entrySet()) {
            for (Map.Entry<String, List<DataChangedEventListener>> entry : pathToListeners.entrySet()) {
                String changedKey = dataEntry.getKey();
                if (!changedKey.startsWith(entry.getKey())) {
                    continue;
                }
                DataChangedEvent event = new DataChangedEvent(type, Escapes.revertDataKey(changedKey, true), dataEntry.getValue());
                for (DataChangedEventListener eachListener : entry.getValue()) {
                    eventCallbackExecutor.execute(() -> eachListener.onChange(event));
                }
            }
        }
    }
    
    public void watch(final String key, final DataChangedEventListener listener) {
        pathToListeners.computeIfAbsent(Escapes.getDataKey(key), k -> new CopyOnWriteArrayList<>()).add(listener);
    }
    
    public void destroy() {
        started.set(false);
        configMapWatchThread.interrupt();
    }
}
