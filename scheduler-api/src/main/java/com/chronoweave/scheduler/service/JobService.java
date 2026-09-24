package com.chronoweave.scheduler.service;

import com.chronoweave.scheduler.domain.JobEntity;
import com.chronoweave.scheduler.domain.JobExecutionEntity;
import com.chronoweave.scheduler.repository.JobExecutionRepository;
import com.chronoweave.scheduler.repository.JobRepository;
import com.chronoweave.shared.dto.*;
import com.chronoweave.shared.enums.JobState;
import com.chronoweave.shared.exception.ResourceNotFoundException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class JobService {

    private static final Logger log = LoggerFactory.getLogger(JobService.class);

    private final JobRepository jobRepository;
    private final JobExecutionRepository executionRepository;
    private final DagValidationService dagValidationService;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public JobService(JobRepository jobRepository,
                      JobExecutionRepository executionRepository,
                      DagValidationService dagValidationService,
                      KafkaTemplate<String, Object> kafkaTemplate,
                      ObjectMapper objectMapper) {
        this.jobRepository = jobRepository;
        this.executionRepository = executionRepository;
        this.dagValidationService = dagValidationService;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public JobResponse submitJob(JobSubmitRequest request) {
        if (request.idempotencyKey() != null && !request.idempotencyKey().isBlank()) {
            Optional<JobEntity> existing = jobRepository.findByIdempotencyKey(request.idempotencyKey());
            if (existing.isPresent()) {
                log.info("Idempotent submission detected for key: {}", request.idempotencyKey());
                return mapToResponse(existing.get());
            }
        }

        String jobId = UUID.randomUUID().toString();
        String payloadStr = null;
        if (request.payload() != null) {
            try {
                payloadStr = objectMapper.writeValueAsString(request.payload());
            } catch (JsonProcessingException e) {
                payloadStr = "{}";
            }
        }

        JobEntity job = new JobEntity(jobId, request.name(), request.jobType(), payloadStr, request.priority());
        job.setIdempotencyKey(request.idempotencyKey());
        job.setRequiredCapability(request.requiredCapability());
        job.setMaxAttempts(request.maxAttempts());
        job.setRetryBackoffMs(request.retryBackoffMs());

        if (request.dependsOnJobIds() != null && !request.dependsOnJobIds().isEmpty()) {
            Set<JobEntity> parents = new HashSet<>();
            for (String parentId : request.dependsOnJobIds()) {
                JobEntity parent = jobRepository.findById(parentId)
                    .orElseThrow(() -> new ResourceNotFoundException("Parent job not found: " + parentId));
                parents.add(parent);
            }
            dagValidationService.validateNoCycles(job, parents);
            job.setDependencies(parents);
        }

        if (areDependenciesSucceeded(job)) {
            job.transitionTo(JobState.QUEUED);
        }

        JobEntity saved = jobRepository.save(job);
        log.info("Submitted job {} with initial state {}", saved.getId(), saved.getState());
        return mapToResponse(saved);
    }

    @Transactional(readOnly = true)
    public JobResponse getJob(String id) {
        JobEntity job = jobRepository.findById(id)
            .orElseThrow(() -> new ResourceNotFoundException("Job not found: " + id));
        return mapToResponse(job);
    }

    @Transactional(readOnly = true)
    public List<JobResponse> getAllJobs() {
        return jobRepository.findAll().stream()
            .map(this::mapToResponse)
            .collect(Collectors.toList());
    }

    @Transactional(readOnly = true)
    public List<JobExecutionResponse> getJobExecutions(String jobId) {
        return executionRepository.findByJobIdOrderByAttemptAsc(jobId).stream()
            .map(e -> new JobExecutionResponse(
                e.getId(), e.getJobId(), e.getAttempt(), e.getWorkerId(),
                e.getStartedAt(), e.getCompletedAt(), e.getStatus(),
                e.getErrorMessage(), e.getOutputData()
            )).collect(Collectors.toList());
    }

    @Transactional
    public JobResponse requeueDlqJob(String jobId) {
        JobEntity job = jobRepository.findById(jobId)
            .orElseThrow(() -> new ResourceNotFoundException("Job not found: " + jobId));
        
        job.transitionTo(JobState.QUEUED);
        job.setCurrentAttempt(0);
        JobEntity updated = jobRepository.save(job);
        log.info("Requeued DLQ job {}", jobId);
        return mapToResponse(updated);
    }

    @Transactional
    public JobResponse cancelJob(String jobId) {
        JobEntity job = jobRepository.findById(jobId)
            .orElseThrow(() -> new ResourceNotFoundException("Job not found: " + jobId));
        
        job.transitionTo(JobState.CANCELLED);
        JobEntity updated = jobRepository.save(job);
        log.info("Cancelled job {}", jobId);
        return mapToResponse(updated);
    }

    public boolean areDependenciesSucceeded(JobEntity job) {
        if (job.getDependencies() == null || job.getDependencies().isEmpty()) {
            return true;
        }
        return job.getDependencies().stream()
            .allMatch(parent -> parent.getState() == JobState.SUCCEEDED);
    }

    public JobResponse mapToResponse(JobEntity entity) {
        List<String> depIds = entity.getDependencies() == null ? List.of() :
            entity.getDependencies().stream().map(JobEntity::getId).collect(Collectors.toList());

        return new JobResponse(
            entity.getId(),
            entity.getIdempotencyKey(),
            entity.getName(),
            entity.getJobType(),
            entity.getPayload(),
            entity.getPriority(),
            entity.getEffectivePriority(),
            entity.getState(),
            entity.getRequiredCapability(),
            entity.getMaxAttempts(),
            entity.getCurrentAttempt(),
            entity.getRetryBackoffMs(),
            entity.getAssignedWorkerId(),
            entity.getScheduledAt(),
            entity.getCreatedAt(),
            entity.getUpdatedAt(),
            depIds
        );
    }
}
