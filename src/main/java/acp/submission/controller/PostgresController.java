package acp.submission.controller;

import acp.submission.service.PostgresService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/acp")
public class PostgresController {

    private final PostgresService postgresService;

    public PostgresController(PostgresService postgresService) {
        this.postgresService = postgresService;
    }

    // Spec: GET all/postgres/{table}
    @GetMapping("/all/postgres/{table}")
    public ResponseEntity<?> getAll(@PathVariable String table) {
        try {
            List<Map<String, Object>> items = postgresService.readAll(table);
            return ResponseEntity.ok(items);
        } catch (Exception e) {
            return ResponseEntity.status(404).build();
        }
    }
}