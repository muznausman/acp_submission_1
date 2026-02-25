package acp.submission.controller;

import acp.submission.service.S3Service;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;

@RestController
@RequestMapping("/api/v1/acp")
public class S3Controller {

    private final S3Service s3Service;
    private final ObjectMapper mapper = new ObjectMapper();

    public S3Controller(S3Service s3Service) {
        this.s3Service = s3Service;
    }

    // Spec: GET all/s3/{bucket}
    @GetMapping("/all/s3/{bucket}")
    public ResponseEntity<?> getAll(@PathVariable String bucket) {
        try {
            List<String> objects = s3Service.readAll(bucket);

            List<Object> parsed = new ArrayList<>();
            for (String obj : objects) {
                parsed.add(mapper.readTree(obj));
            }

            return ResponseEntity.ok()
                    .header("Content-Type", "application/json")
                    .body(mapper.writeValueAsString(parsed));

        } catch (Exception e) {
            return ResponseEntity.status(404).build();
        }
    }

    // Spec: GET single/s3/{bucket}/{key}
    @GetMapping("/single/s3/{bucket}/{key}")
    public ResponseEntity<?> getSingle(@PathVariable String bucket, @PathVariable String key) {
        try {
            String object = s3Service.readOne(bucket, key);
            if (object == null) return ResponseEntity.status(404).build();

            return ResponseEntity.ok()
                    .header("Content-Type", "application/json")
                    .body(mapper.writeValueAsString(mapper.readTree(object)));

        } catch (Exception e) {
            return ResponseEntity.status(404).build();
        }
    }
}