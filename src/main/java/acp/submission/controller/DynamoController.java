package acp.submission.controller;

import acp.submission.service.DynamoService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/acp")
public class DynamoController {

    private final DynamoService dynamoService;
    private final ObjectMapper mapper = new ObjectMapper();

    public DynamoController(DynamoService dynamoService) {
        this.dynamoService = dynamoService;
    }

    // Spec: GET all/dynamo/{table}
    @GetMapping("/all/dynamo/{table}")
    public ResponseEntity<?> getAll(@PathVariable String table) {
        try {
            Object items = dynamoService.readAll(table);
            return ResponseEntity.ok()
                    .header("Content-Type", "application/json")
                    .body(mapper.writeValueAsString(items));
        } catch (Exception e) {
            return ResponseEntity.status(404).build();
        }
    }

    // Spec: GET single/dynamo/{table}/{key}
    @GetMapping("/single/dynamo/{table}/{key}")
    public ResponseEntity<?> single(@PathVariable String table, @PathVariable String key) {
        try {
            Object item = dynamoService.readOne(table, key);
            if (item == null) return ResponseEntity.status(404).build();

            return ResponseEntity.ok()
                    .header("Content-Type", "application/json")
                    .body(mapper.writeValueAsString(item));
        } catch (Exception e) {
            return ResponseEntity.status(404).build();
        }
    }
}