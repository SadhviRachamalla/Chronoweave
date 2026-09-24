package com.chronoweave.scheduler.repository;

import com.chronoweave.scheduler.domain.JobEntity;
import com.chronoweave.shared.enums.JobState;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

@Repository
public interface JobRepository extends JpaRepository<JobEntity, String> {

    Optional<JobEntity> findByIdempotencyKey(String idempotencyKey);

    List<JobEntity> findByStateOrderByEffectivePriorityDescCreatedAtAsc(JobState state);

    @Query("SELECT j FROM JobEntity j WHERE j.state = :state AND (j.scheduledAt IS NULL OR j.scheduledAt <= :now) ORDER BY j.effectivePriority DESC, j.createdAt ASC")
    List<JobEntity> findSchedulableJobs(@Param("state") JobState state, @Param("now") Instant now);

    @Modifying
    @Query("UPDATE JobEntity j SET j.effectivePriority = j.priority + :agingBonus WHERE j.state = 'QUEUED'")
    int applyPriorityAging(@Param("agingBonus") int agingBonus);

    List<JobEntity> findByAssignedWorkerIdAndState(String workerId, JobState state);

    List<JobEntity> findByState(JobState state);
}
