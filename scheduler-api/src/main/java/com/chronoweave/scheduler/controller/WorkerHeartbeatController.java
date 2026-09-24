package com.chronoweave.scheduler.controller;

import com.chronoweave.scheduler.service.WorkerRegistryService;
import com.chronoweave.shared.dto.WorkerHeartbeat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/workers")
public class WorkerHeartbeatController {

    private final WorkerRegistryService workerRegistryService;

    public WorkerHeartbeatController(WorkerRegistryService workerRegistryService) {
        this.workerRegistryService = workerRegistryService;
    }

    @PostMapping("/heartbeat")
    public ResponseEntity<Void> receiveHeartbeat(@RequestBody WorkerHeartbeat heartbeat) {
        workerRegistryService.registerHeartbeat(heartbeat);
        return ResponseEntity.ok().build();
    }
}
