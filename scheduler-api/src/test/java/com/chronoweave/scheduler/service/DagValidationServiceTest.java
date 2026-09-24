package com.chronoweave.scheduler.service;

import com.chronoweave.scheduler.domain.JobEntity;
import com.chronoweave.shared.enums.JobType;
import com.chronoweave.shared.exception.DagCycleException;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class DagValidationServiceTest {

    private final DagValidationService service = new DagValidationService();

    @Test
    void testLinearDagValid() {
        JobEntity parent = new JobEntity("job-1", "Parent", JobType.ECHO, "{}", 5);
        JobEntity child = new JobEntity("job-2", "Child", JobType.ECHO, "{}", 5);

        assertDoesNotThrow(() -> service.validateNoCycles(child, Set.of(parent)));
    }

    @Test
    void testDirectCycleThrowsException() {
        JobEntity jobA = new JobEntity("job-A", "A", JobType.ECHO, "{}", 5);
        JobEntity jobB = new JobEntity("job-B", "B", JobType.ECHO, "{}", 5);

        jobB.setDependencies(Set.of(jobA));

        // Attempting to make A depend on B creates cycle A -> B -> A
        assertThrows(DagCycleException.class, () -> service.validateNoCycles(jobA, Set.of(jobB)));
    }
}
