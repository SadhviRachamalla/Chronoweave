package com.chronoweave.scheduler.service;

import com.chronoweave.scheduler.domain.JobEntity;
import com.chronoweave.scheduler.repository.JobExecutionRepository;
import com.chronoweave.scheduler.repository.JobRepository;
import com.chronoweave.shared.dto.JobResponse;
import com.chronoweave.shared.dto.JobSubmitRequest;
import com.chronoweave.shared.enums.JobState;
import com.chronoweave.shared.enums.JobType;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class JobServiceUnitTest {

    private JobService jobService;
    private JobRepository jobRepository;
    private JobExecutionRepository executionRepository;
    private DagValidationService dagValidationService;
    private KafkaTemplate<String, Object> kafkaTemplate;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        jobRepository = mock(JobRepository.class);
        executionRepository = mock(JobExecutionRepository.class);
        dagValidationService = mock(DagValidationService.class);
        kafkaTemplate = mock(KafkaTemplate.class);
        objectMapper = new ObjectMapper();

        jobService = new JobService(jobRepository, executionRepository, dagValidationService, kafkaTemplate, objectMapper);
    }

    @Test
    void testIdempotencyReturnsExistingJob() {
        String idempotencyKey = "key-abc-123";
        JobEntity existingJob = new JobEntity("job-uuid-1", "Test Job", JobType.ECHO, "{}", 5);
        existingJob.setIdempotencyKey(idempotencyKey);
        existingJob.setStateDirectly(JobState.QUEUED);

        when(jobRepository.findByIdempotencyKey(idempotencyKey)).thenReturn(Optional.of(existingJob));

        JobSubmitRequest request = new JobSubmitRequest(idempotencyKey, "Test Job", JobType.ECHO, null, 5, "DEFAULT", 3, 1000L, null);
        JobResponse response = jobService.submitJob(request);

        assertEquals("job-uuid-1", response.id());
        assertEquals(JobState.QUEUED, response.state());
        verify(jobRepository, never()).save(any());
    }

    @Test
    void testNewJobSubmissionMovesToQueuedWhenNoDependencies() {
        when(jobRepository.findByIdempotencyKey(any())).thenReturn(Optional.empty());
        when(jobRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        JobSubmitRequest request = new JobSubmitRequest("key-new", "New Job", JobType.ECHO, null, 10, "DEFAULT", 3, 1000L, null);
        JobResponse response = jobService.submitJob(request);

        assertNotNull(response.id());
        assertEquals(JobState.QUEUED, response.state());
        assertEquals(10, response.priority());
    }

    @Test
    void testIdempotencyRaceHandlingOnDataIntegrityViolation() {
        String idempotencyKey = "key-race-123";
        JobEntity existingJob = new JobEntity("job-uuid-race", "Race Job", JobType.ECHO, "{}", 5);
        existingJob.setIdempotencyKey(idempotencyKey);
        existingJob.setStateDirectly(JobState.QUEUED);

        when(jobRepository.findByIdempotencyKey(idempotencyKey))
            .thenReturn(Optional.empty())
            .thenReturn(Optional.of(existingJob));

        when(jobRepository.save(any()))
            .thenThrow(new org.springframework.dao.DataIntegrityViolationException("Duplicate entry for idempotency key"));

        JobSubmitRequest request = new JobSubmitRequest(idempotencyKey, "Race Job", JobType.ECHO, null, 5, "DEFAULT", 3, 1000L, null);
        JobResponse response = jobService.submitJob(request);

        assertEquals("job-uuid-race", response.id());
        assertEquals(JobState.QUEUED, response.state());
        verify(jobRepository, times(2)).findByIdempotencyKey(idempotencyKey);
    }
}
