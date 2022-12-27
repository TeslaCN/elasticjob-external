package icu.wwj.elasticjob.external.reg.kubernetes.e2e;

import icu.wwj.elasticjob.external.reg.kubernetes.KubernetesRegistryCenter;
import icu.wwj.elasticjob.external.reg.kubernetes.KubernetesRegistryConfiguration;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.elasticjob.api.JobConfiguration;
import org.apache.shardingsphere.elasticjob.api.ShardingContext;
import org.apache.shardingsphere.elasticjob.lite.api.bootstrap.impl.ScheduleJobBootstrap;
import org.apache.shardingsphere.elasticjob.reg.base.CoordinatorRegistryCenter;
import org.apache.shardingsphere.elasticjob.simple.job.SimpleJob;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ScheduleJobTest {
    
    @Test
    void testJob() throws IOException, InterruptedException {
        CoordinatorRegistryCenter registryCenter = new KubernetesRegistryCenter(new KubernetesRegistryConfiguration("elasticjob"));
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
