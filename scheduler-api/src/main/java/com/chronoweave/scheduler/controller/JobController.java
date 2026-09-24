package com.chronoweave.scheduler.controller;

import com.chronoweave.scheduler.service.JobService;
import com.chronoweave.shared.dto.*;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/v1/jobs")
public class JobController {

    private final JobService jobService;

    public JobController(JobService jobService) {
        this.jobService = jobService;
    }

    @PostMapping
    public ResponseEntity<JobResponse> submitJob(@Valid @RequestBody JobSubmitRequest request) {
        JobResponse response = jobService.submitJob(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }

    @GetMapping("/detail/{id}")
    public ResponseEntity<JobResponse> getJob(@PathVariable("id") String id) {
        return ResponseEntity.ok(jobService.getJob(id));
    }

    @GetMapping
    public ResponseEntity<List<JobResponse>> getAllJobs() {
        return ResponseEntity.ok(jobService.getAllJobs());
    }

    @GetMapping("/detail/{id}/executions")
    public ResponseEntity<List<JobExecutionResponse>> getJobExecutions(@PathVariable("id") String id) {
        return ResponseEntity.ok(jobService.getJobExecutions(id));
    }

    @PostMapping("/detail/{id}/requeue")
    public ResponseEntity<JobResponse> requeueDlqJob(@PathVariable("id") String id) {
        return ResponseEntity.ok(jobService.requeueDlqJob(id));
    }

    @PostMapping("/detail/{id}/cancel")
    public ResponseEntity<JobResponse> cancelJob(@PathVariable("id") String id) {
        return ResponseEntity.ok(jobService.cancelJob(id));
    }
}
