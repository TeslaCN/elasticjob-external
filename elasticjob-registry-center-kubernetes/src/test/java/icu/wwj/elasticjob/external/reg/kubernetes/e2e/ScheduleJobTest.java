package icu.wwj.elasticjob.external.reg.kubernetes.e2e;

import icu.wwj.elasticjob.external.reg.kubernetes.KubernetesRegistryCenter;
import icu.wwj.elasticjob.external.reg.kubernetes.KubernetesRegistryConfiguration;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Namespace;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.util.Config;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.elasticjob.api.JobConfiguration;
import org.apache.shardingsphere.elasticjob.api.ShardingContext;
import org.apache.shardingsphere.elasticjob.lite.api.bootstrap.impl.ScheduleJobBootstrap;
import org.apache.shardingsphere.elasticjob.reg.base.CoordinatorRegistryCenter;
import org.apache.shardingsphere.elasticjob.simple.job.SimpleJob;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ScheduleJobTest {
    
    private static final String NAMESPACE = "elasticjob-test";
    
    private ApiClient apiClient;
    
    @BeforeEach
    void setup() throws IOException, ApiException {
        apiClient = Config.defaultClient();
        CoreV1Api coreV1Api = new CoreV1Api(apiClient);
        try {
            coreV1Api.deleteNamespace(NAMESPACE, null, null, null, null, null, null);
        } catch (ApiException ignored) {
        }
        coreV1Api.createNamespace(new V1Namespace().metadata(new V1ObjectMeta().name(NAMESPACE)), null, null, null, null);
    }
    
    @Test
    void testJob() throws InterruptedException {
        CoordinatorRegistryCenter registryCenter = new KubernetesRegistryCenter(new KubernetesRegistryConfiguration(NAMESPACE), apiClient);
        registryCenter.init();
        JobConfiguration jobConfiguration = JobConfiguration.newBuilder("test-schedule-job", 3)
                .cron("0/3 * * * * ?").shardingItemParameters("0=foo,1=bar,2=baz").overwrite(true).build();
        CountDownLatch countDownLatch = new CountDownLatch(9);
        ScheduleJobBootstrap bootstrap = new ScheduleJobBootstrap(registryCenter, new TestJob(countDownLatch), jobConfiguration);
        bootstrap.schedule();
        Assertions.assertTrue(countDownLatch.await(15, TimeUnit.SECONDS));
        bootstrap.shutdown();
        registryCenter.close();
    }
    
    @AfterEach
    void tearDown() throws ApiException {
        CoreV1Api coreV1Api = new CoreV1Api(apiClient);
        coreV1Api.deleteNamespace(NAMESPACE, null, null, null, null, null, null);
    }
    
    @Slf4j
    @RequiredArgsConstructor
    private static class TestJob implements SimpleJob {
        
        private final CountDownLatch countDownLatch;
    
        @Override
        public void execute(final ShardingContext shardingContext) {
            log.info("Executing {}", shardingContext);
            countDownLatch.countDown();
        }
    }
}
