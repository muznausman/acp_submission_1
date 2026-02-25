package acp.submission.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class DynamoService {

    private final DynamoDbClient dynamoDbClient;
    private final ObjectMapper mapper = new ObjectMapper();

    public DynamoService(DynamoDbClient dynamoDbClient) {
        this.dynamoDbClient = dynamoDbClient;
    }

    public List<Object> readAll(String table) {
        ScanRequest request = ScanRequest.builder()
                .tableName(table)
                .build();

        ScanResponse response = dynamoDbClient.scan(request);

        return response.items().stream().map(item -> {
            AttributeValue data = item.get("data");
            if (data != null && data.s() != null) {
                try {
                    return mapper.readTree(data.s());
                } catch (Exception ignored) {
                    // fall through
                }
            }
            return convertItem(item);
        }).collect(Collectors.toList());
    }

    /**
     * Spec: GET single/dynamo/{table}/{key}
     */
    public Object readOne(String table, String key) {
        // We assume the hash key is either "name" (process/dynamo) or "id" (copy-content/dynamo)
        // We'll try both to be robust.
        Map<String, AttributeValue> keyMap = new HashMap<>();
        keyMap.put("name", AttributeValue.builder().s(key).build());

        GetItemRequest req = GetItemRequest.builder()
                .tableName(table)
                .key(keyMap)
                .build();

        Map<String, AttributeValue> item = dynamoDbClient.getItem(req).item();
        if (item == null || item.isEmpty()) {
            // try "id" as partition key
            keyMap = new HashMap<>();
            keyMap.put("id", AttributeValue.builder().s(key).build());
            req = GetItemRequest.builder().tableName(table).key(keyMap).build();
            item = dynamoDbClient.getItem(req).item();
        }

        if (item == null || item.isEmpty()) {
            return null;
        }

        AttributeValue data = item.get("data");
        if (data != null && data.s() != null) {
            try {
                JsonNode node = mapper.readTree(data.s());
                return node;
            } catch (Exception ignored) {
            }
        }
        return convertItem(item);
    }

    private Map<String, Object> convertItem(Map<String, AttributeValue> item) {
        Map<String, Object> out = new LinkedHashMap<>();
        for (Map.Entry<String, AttributeValue> entry : item.entrySet()) {
            out.put(entry.getKey(), attrToObject(entry.getValue()));
        }
        return out;
    }

    private Object attrToObject(AttributeValue av) {
        if (av == null) return null;

        // String
        if (av.s() != null) return av.s();

        // Number (return BigDecimal so JSON becomes number)
        if (av.n() != null) return new BigDecimal(av.n());

        // Boolean
        if (av.bool() != null) return av.bool();

        // Null
        if (Boolean.TRUE.equals(av.nul())) return null;

        // Map
        if (av.m() != null && !av.m().isEmpty()) {
            Map<String, Object> m = new LinkedHashMap<>();
            for (Map.Entry<String, AttributeValue> e : av.m().entrySet()) {
                m.put(e.getKey(), attrToObject(e.getValue()));
            }
            return m;
        }

        // List
        if (av.l() != null && !av.l().isEmpty()) {
            List<Object> list = new ArrayList<>();
            for (AttributeValue v : av.l()) list.add(attrToObject(v));
            return list;
        }

        // String Set
        if (av.ss() != null && !av.ss().isEmpty()) return av.ss();

        // Number Set
        if (av.ns() != null && !av.ns().isEmpty()) {
            return av.ns().stream().map(BigDecimal::new).collect(Collectors.toList());
        }

        // Binary sets etc (rare for your assignment)
        if (av.b() != null) return av.b().asByteArray();
        if (av.bs() != null && !av.bs().isEmpty()) {
            return av.bs().stream().map(b -> b.asByteArray()).collect(Collectors.toList());
        }

        // Fallback: empty map rather than {}
        return null;
    }
}