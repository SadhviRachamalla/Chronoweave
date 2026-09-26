package com.chronoweave.scheduler.service;

import com.chronoweave.scheduler.domain.JobEntity;
import com.chronoweave.scheduler.repository.JobExecutionRepository;
import com.chronoweave.scheduler.repository.JobRepository;
import com.chronoweave.shared.dto.WorkerHeartbeat;
import com.chronoweave.shared.enums.JobState;
import com.chronoweave.shared.enums.JobType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@DataJpaTest
class SchedulingEngineIntegrationTest {

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private JobExecutionRepository executionRepository;

    @Autowired
    private PlatformTransactionManager transactionManager;

    private WorkerRegistryService workerRegistryService;
    private JobService jobService;
    private RedissonClient redissonClient;
    private KafkaTemplate<String, Object> kafkaTemplate;
    private SchedulingEngine schedulingEngine;

    @BeforeEach
    void setUp() throws InterruptedException {
        workerRegistryService = mock(WorkerRegistryService.class);
        jobService = mock(JobService.class);
        redissonClient = mock(RedissonClient.class);
        kafkaTemplate = mock(KafkaTemplate.class);

        RLock lock = mock(RLock.class);
        when(lock.tryLock(anyLong(), anyLong(), any(TimeUnit.class))).thenReturn(true);
        when(lock.isHeldByCurrentThread()).thenReturn(true);
        when(redissonClient.getLock(anyString())).thenReturn(lock);

        schedulingEngine = new SchedulingEngine(
            jobRepository,
            executionRepository,
            workerRegistryService,
            jobService,
            redissonClient,
            kafkaTemplate,
            transactionManager
        );
    }

    @Test
    void testPriorityAgingAcrossMultipleCycles() {
        JobEntity job = new JobEntity("job-aging-1", "Aging Job", JobType.ECHO, "{}", 10);
        job.setStateDirectly(JobState.QUEUED);
        jobRepository.saveAndFlush(job);

        assertEquals(10, jobRepository.findById("job-aging-1").get().getEffectivePriority());

        schedulingEngine.applyPriorityAging();
        JobEntity updatedCycle1 = jobRepository.findById("job-aging-1").get();
        assertEquals(11, updatedCycle1.getEffectivePriority());

        schedulingEngine.applyPriorityAging();
        JobEntity updatedCycle2 = jobRepository.findById("job-aging-1").get();
        assertEquals(12, updatedCycle2.getEffectivePriority());
    }

    @Test
    void testWorkerSlotEligibility() {
        JobEntity job = new JobEntity("job-slot-1", "Slot Job", JobType.ECHO, "{}", 5);
        job.setRequiredCapability("DEFAULT");
        job.setStateDirectly(JobState.QUEUED);
        jobRepository.saveAndFlush(job);

        // Worker has no free slots available (active = 5, max = 5)
        WorkerHeartbeat busyWorker = new WorkerHeartbeat("worker-busy", List.of("DEFAULT"), 5, 5, System.currentTimeMillis());
        when(workerRegistryService.getActiveWorkers()).thenReturn(List.of(busyWorker));

        schedulingEngine.dispatchQueuedJobs();
        assertEquals(JobState.QUEUED, jobRepository.findById("job-slot-1").get().getState());

        // Worker now has free slots available (active = 2, max = 5)
        WorkerHeartbeat availableWorker = new WorkerHeartbeat("worker-avail", List.of("DEFAULT"), 5, 2, System.currentTimeMillis());
        when(workerRegistryService.getActiveWorkers()).thenReturn(List.of(availableWorker));

        schedulingEngine.dispatchQueuedJobs();
        JobEntity dispatched = jobRepository.findById("job-slot-1").get();
        assertEquals(JobState.RUNNING, dispatched.getState());
        assertEquals("worker-avail", dispatched.getAssignedWorkerId());
        verify(kafkaTemplate).send(eq("chronoweave-dispatch"), eq("DEFAULT"), any());
    }

    @Test
    void testStaleWorkerRecovery() {
        JobEntity job = new JobEntity("job-stale-1", "Stale Worker Job", JobType.ECHO, "{}", 5);
        job.setStateDirectly(JobState.RUNNING);
        job.setAssignedWorkerId("worker-stale");
        job.setCurrentAttempt(1);
        job.setMaxAttempts(3);
        jobRepository.saveAndFlush(job);

        when(workerRegistryService.getStaleWorkerIds()).thenReturn(List.of("worker-stale"));

        schedulingEngine.recoverStaleWorkers();

        JobEntity recovered = jobRepository.findById("job-stale-1").get();
        assertEquals(JobState.QUEUED, recovered.getState());
        assertNull(recovered.getAssignedWorkerId());
        verify(workerRegistryService).unregisterWorker("worker-stale");
    }

    @Test
    void testRetryDlqBehaviorWhenAttemptsExhausted() {
        JobEntity job = new JobEntity("job-dlq-1", "Exhausted Job", JobType.ECHO, "{}", 5);
        job.setStateDirectly(JobState.RUNNING);
        job.setAssignedWorkerId("worker-dead");
        job.setCurrentAttempt(3);
        job.setMaxAttempts(3);
        jobRepository.saveAndFlush(job);

        when(workerRegistryService.getStaleWorkerIds()).thenReturn(List.of("worker-dead"));

        schedulingEngine.recoverStaleWorkers();

        JobEntity deadLettered = jobRepository.findById("job-dlq-1").get();
        assertEquals(JobState.DEAD_LETTERED, deadLettered.getState());
        verify(kafkaTemplate).send(eq("chronoweave-dlq"), eq("job-dlq-1"), eq("job-dlq-1"));
        verify(workerRegistryService).unregisterWorker("worker-dead");
    }
}
