package com.chronoweave.scheduler.repository;

import com.chronoweave.scheduler.domain.JobExecutionEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface JobExecutionRepository extends JpaRepository<JobExecutionEntity, Long> {
    List<JobExecutionEntity> findByJobIdOrderByAttemptAsc(String jobId);
}
