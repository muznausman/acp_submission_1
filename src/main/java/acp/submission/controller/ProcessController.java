package acp.submission.controller;

import acp.submission.dto.ProcessRequest;
import acp.submission.service.ProcessService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import com.fasterxml.jackson.databind.node.ArrayNode;

@RestController
@RequestMapping("/api/v1/acp")
public class ProcessController {

    private final ProcessService processService;

    public ProcessController(ProcessService processService) {
        this.processService = processService;
    }

    // Spec: POST process/dump
    @PostMapping("/process/dump")
    public ResponseEntity<?> dump(@RequestBody ProcessRequest req) {
        try {
            ArrayNode processed = processService.fetchAndProcess(req.getUrlPath());
            // 🔥 IMPORTANT FIX: return JSON as string
            return ResponseEntity.ok()
                    .header("Content-Type", "application/json")
                    .body(processed.toString());
        } catch (Exception e) {
            return ResponseEntity.status(404).build();
        }
    }

    // Spec: POST process/dynamo
    @PostMapping("/process/dynamo")
    public ResponseEntity<?> toDynamo(@RequestBody ProcessRequest req) {
        try {
            ArrayNode processed = processService.fetchAndProcess(req.getUrlPath());
            processService.writeToDynamo(processed);
            return ResponseEntity.ok().build();
        } catch (Exception e) {
            return ResponseEntity.status(404).build();
        }
    }

    // Spec: POST process/s3
    @PostMapping("/process/s3")
    public ResponseEntity<?> toS3(@RequestBody ProcessRequest req) {
        try {
            ArrayNode processed = processService.fetchAndProcess(req.getUrlPath());
            processService.writeToS3(processed);
            return ResponseEntity.ok().build();
        } catch (Exception e) {
            return ResponseEntity.status(404).build();
        }
    }

    // Spec: POST process/postgres/{table}
    @PostMapping("/process/postgres/{table}")
    public ResponseEntity<?> toPostgres(@PathVariable String table, @RequestBody ProcessRequest req) {
        try {
            ArrayNode processed = processService.fetchAndProcess(req.getUrlPath());
            processService.writeToPostgres(table, processed);
            return ResponseEntity.ok().build();
        } catch (Exception e) {
            return ResponseEntity.status(404).build();
        }
    }

    // Spec: POST copy-content/dynamo/{table}
    @PostMapping("/copy-content/dynamo/{table}")
    public ResponseEntity<?> copyToDynamo(@PathVariable String table) {
        try {
            processService.copyPostgresToDynamo(table);
            return ResponseEntity.ok().build();
        } catch (Exception e) {
            return ResponseEntity.status(404).build();
        }
    }

    // Spec: POST copy-content/S3/{table}
    @PostMapping("/copy-content/s3/{table}")
    public ResponseEntity<?> copyContentToS3(@PathVariable String table) {
        try {
            processService.copyPostgresToS3(table);
            return ResponseEntity.ok().build();
        } catch (Exception e) {
            return ResponseEntity.status(404).build();
        }
    }
}