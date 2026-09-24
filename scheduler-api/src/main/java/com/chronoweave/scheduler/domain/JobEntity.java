package com.chronoweave.scheduler.domain;

import com.chronoweave.shared.enums.JobState;
import com.chronoweave.shared.enums.JobType;
import com.chronoweave.shared.exception.IllegalJobStateTransitionException;
import jakarta.persistence.*;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

@Entity
@Table(name = "jobs")
public class JobEntity {

    @Id
    private String id;

    @Column(name = "idempotency_key", unique = true)
    private String idempotencyKey;

    @Column(nullable = false)
    private String name;

    @Enumerated(EnumType.STRING)
    @Column(name = "job_type", nullable = false)
    private JobType jobType;

    @Column(columnDefinition = "TEXT")
    private String payload;

    @Column(nullable = false)
    private int priority;

    @Column(name = "effective_priority", nullable = false)
    private int effectivePriority;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private JobState state;

    @Column(name = "required_capability", nullable = false)
    private String requiredCapability = "DEFAULT";

    @Column(name = "max_attempts", nullable = false)
    private int maxAttempts = 3;

    @Column(name = "current_attempt", nullable = false)
    private int currentAttempt = 0;

    @Column(name = "retry_backoff_ms", nullable = false)
    private long retryBackoffMs = 1000L;

    @Column(name = "assigned_worker_id")
    private String assignedWorkerId;

    @Column(name = "scheduled_at")
    private Instant scheduledAt;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    @ManyToMany(fetch = FetchType.EAGER)
    @JoinTable(
        name = "job_dependencies",
        joinColumns = @JoinColumn(name = "job_id"),
        inverseJoinColumns = @JoinColumn(name = "parent_job_id")
    )
    private Set<JobEntity> dependencies = new HashSet<>();

    public JobEntity() {}

    public JobEntity(String id, String name, JobType jobType, String payload, int priority) {
        this.id = id;
        this.name = name;
        this.jobType = jobType;
        this.payload = payload;
        this.priority = priority;
        this.effectivePriority = priority;
        this.state = JobState.PENDING;
        this.createdAt = Instant.now();
        this.updatedAt = Instant.now();
    }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getIdempotencyKey() { return idempotencyKey; }
    public void setIdempotencyKey(String idempotencyKey) { this.idempotencyKey = idempotencyKey; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public JobType getJobType() { return jobType; }
    public void setJobType(JobType jobType) { this.jobType = jobType; }

    public String getPayload() { return payload; }
    public void setPayload(String payload) { this.payload = payload; }

    public int getPriority() { return priority; }
    public void setPriority(int priority) { this.priority = priority; }

    public int getEffectivePriority() { return effectivePriority; }
    public void setEffectivePriority(int effectivePriority) { this.effectivePriority = effectivePriority; }

    public JobState getState() { return state; }
    
    public void transitionTo(JobState newState) {
        if (!this.state.canTransitionTo(newState)) {
            throw new IllegalJobStateTransitionException(
                String.format("Cannot transition job %s from %s to %s", id, state, newState)
            );
        }
        this.state = newState;
        this.updatedAt = Instant.now();
    }
    
    public void setStateDirectly(JobState state) {
        this.state = state;
        this.updatedAt = Instant.now();
    }

    public String getRequiredCapability() { return requiredCapability; }
    public void setRequiredCapability(String requiredCapability) { this.requiredCapability = requiredCapability; }

    public int getMaxAttempts() { return maxAttempts; }
    public void setMaxAttempts(int maxAttempts) { this.maxAttempts = maxAttempts; }

    public int getCurrentAttempt() { return currentAttempt; }
    public void setCurrentAttempt(int currentAttempt) { this.currentAttempt = currentAttempt; }

    public long getRetryBackoffMs() { return retryBackoffMs; }
    public void setRetryBackoffMs(long retryBackoffMs) { this.retryBackoffMs = retryBackoffMs; }

    public String getAssignedWorkerId() { return assignedWorkerId; }
    public void setAssignedWorkerId(String assignedWorkerId) { this.assignedWorkerId = assignedWorkerId; }

    public Instant getScheduledAt() { return scheduledAt; }
    public void setScheduledAt(Instant scheduledAt) { this.scheduledAt = scheduledAt; }

    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }

    public Instant getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(Instant updatedAt) { this.updatedAt = updatedAt; }

    public Set<JobEntity> getDependencies() { return dependencies; }
    public void setDependencies(Set<JobEntity> dependencies) { this.dependencies = dependencies; }

    @PrePersist
    protected void onCreate() {
        if (createdAt == null) createdAt = Instant.now();
        if (updatedAt == null) updatedAt = Instant.now();
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = Instant.now();
    }
}
