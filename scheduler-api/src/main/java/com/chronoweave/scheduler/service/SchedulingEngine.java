package com.chronoweave.scheduler.service;

import com.chronoweave.scheduler.domain.JobEntity;
import com.chronoweave.scheduler.domain.JobExecutionEntity;
import com.chronoweave.scheduler.repository.JobExecutionRepository;
import com.chronoweave.scheduler.repository.JobRepository;
import com.chronoweave.shared.dto.JobDispatchEvent;
import com.chronoweave.shared.dto.WorkerHeartbeat;
import com.chronoweave.shared.enums.JobState;
import com.chronoweave.shared.util.BackoffUtil;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Service
public class SchedulingEngine {

    private static final Logger log = LoggerFactory.getLogger(SchedulingEngine.class);
    private static final String LEADER_LOCK_KEY = "chronoweave:leader:lock";
    private static final String JOB_LOCK_PREFIX = "chronoweave:job:lock:";
    private static final String DISPATCH_TOPIC = "chronoweave-dispatch";
    private static final String DLQ_TOPIC = "chronoweave-dlq";

    private final JobRepository jobRepository;
    private final JobExecutionRepository executionRepository;
    private final WorkerRegistryService workerRegistryService;
    private final JobService jobService;
    private final RedissonClient redissonClient;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final TransactionTemplate transactionTemplate;

    public SchedulingEngine(JobRepository jobRepository,
                            JobExecutionRepository executionRepository,
                            WorkerRegistryService workerRegistryService,
                            JobService jobService,
                            RedissonClient redissonClient,
                            KafkaTemplate<String, Object> kafkaTemplate,
                            PlatformTransactionManager transactionManager) {
        this.jobRepository = jobRepository;
        this.executionRepository = executionRepository;
        this.workerRegistryService = workerRegistryService;
        this.jobService = jobService;
        this.redissonClient = redissonClient;
        this.kafkaTemplate = kafkaTemplate;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
    }

    @Scheduled(fixedDelay = 2000)
    public void runSchedulerCycle() {
        RLock leaderLock = redissonClient.getLock(LEADER_LOCK_KEY);
        try {
            boolean isLeader = leaderLock.tryLock(100, 10000, TimeUnit.MILLISECONDS);
            if (!isLeader) {
                return;
            }

            try {
                transactionTemplate.executeWithoutResult(status -> {
                    checkPendingJobs();
                    applyPriorityAging();
                    dispatchQueuedJobs();
                    recoverStaleWorkers();
                });
            } finally {
                if (leaderLock.isHeldByCurrentThread()) {
                    leaderLock.unlock();
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.error("Error in scheduler loop", e);
        }
    }

    public void checkPendingJobs() {
        List<JobEntity> pendingJobs = jobRepository.findByState(JobState.PENDING);
        for (JobEntity job : pendingJobs) {
            if (jobService.areDependenciesSucceeded(job)) {
                job.transitionTo(JobState.QUEUED);
                jobRepository.save(job);
                log.info("Dependencies resolved for job {}, moved from PENDING to QUEUED", job.getId());
            }
        }
    }

    public void applyPriorityAging() {
        jobRepository.applyPriorityAging(1);
    }

    public void dispatchQueuedJobs() {
        Instant now = Instant.now();
        List<JobEntity> schedulable = jobRepository.findSchedulableJobs(JobState.QUEUED, now);
        List<WorkerHeartbeat> activeWorkers = workerRegistryService.getActiveWorkers();

        if (schedulable.isEmpty()) return;

        for (JobEntity job : schedulable) {
            WorkerHeartbeat selectedWorker = selectWorkerForJob(job, activeWorkers);
            if (selectedWorker == null) {
                log.debug("No available worker with capability '{}' and free slots for job {}",
                    job.getRequiredCapability(), job.getId());
                continue;
            }

            RLock jobLock = redissonClient.getLock(JOB_LOCK_PREFIX + job.getId());
            try {
                if (jobLock.tryLock(50, 5000, TimeUnit.MILLISECONDS)) {
                    try {
                        job.transitionTo(JobState.RUNNING);
                        job.setAssignedWorkerId(selectedWorker.workerId());
                        job.setCurrentAttempt(job.getCurrentAttempt() + 1);
                        jobRepository.save(job);

                        JobExecutionEntity exec = new JobExecutionEntity(
                            job.getId(), job.getCurrentAttempt(), selectedWorker.workerId(), Instant.now(), "RUNNING"
                        );
                        executionRepository.save(exec);

                        JobDispatchEvent event = new JobDispatchEvent(
                            job.getId(), job.getJobType(), job.getPayload(),
                            job.getRequiredCapability(), job.getCurrentAttempt(), job.getRetryBackoffMs()
                        );
                        kafkaTemplate.send(DISPATCH_TOPIC, job.getRequiredCapability(), event);
                        log.info("Dispatched job {} (attempt {}) to worker {}", job.getId(), job.getCurrentAttempt(), selectedWorker.workerId());

                    } finally {
                        if (jobLock.isHeldByCurrentThread()) {
                            jobLock.unlock();
                        }
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private WorkerHeartbeat selectWorkerForJob(JobEntity job, List<WorkerHeartbeat> workers) {
        return workers.stream()
            .filter(w -> w.capabilities() != null && w.capabilities().contains(job.getRequiredCapability()))
            .filter(w -> w.activeSlots() < w.maxSlots())
            .findFirst()
            .orElse(null);
    }

    public void recoverStaleWorkers() {
        List<String> staleWorkerIds = workerRegistryService.getStaleWorkerIds();
        for (String staleId : staleWorkerIds) {
            log.warn("Worker {} is stale, initiating recovery for assigned jobs", staleId);
            List<JobEntity> stuckJobs = jobRepository.findByAssignedWorkerIdAndState(staleId, JobState.RUNNING);
            for (JobEntity job : stuckJobs) {
                if (job.getCurrentAttempt() < job.getMaxAttempts()) {
                    long backoff = BackoffUtil.calculateBackoffWithJitter(job.getCurrentAttempt(), job.getRetryBackoffMs(), 2.0);
                    job.transitionTo(JobState.RETRYING);
                    job.setScheduledAt(Instant.now().plusMillis(backoff));
                    job.setAssignedWorkerId(null);
                    job.transitionTo(JobState.QUEUED);
                    jobRepository.save(job);
                    log.info("Recovered stuck job {} from stale worker {}, scheduled for retry in {} ms", job.getId(), staleId, backoff);
                } else {
                    job.transitionTo(JobState.DEAD_LETTERED);
                    jobRepository.save(job);
                    kafkaTemplate.send(DLQ_TOPIC, job.getId(), job.getId());
                    log.error("Stuck job {} on stale worker {} exceeded max attempts, moved to DLQ", job.getId(), staleId);
                }
            }
            workerRegistryService.unregisterWorker(staleId);
        }
    }
}
