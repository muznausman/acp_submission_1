package acp.submission.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.*;

import static acp.submission.util.SidResolver.resolve;

@Service
public class ProcessService {

    private final HttpClient httpClient = HttpClient.newHttpClient();
    private final ObjectMapper mapper = new ObjectMapper();
    private final DynamoDbClient dynamoDbClient;
    private final S3Client s3Client;
    private final JdbcTemplate jdbcTemplate;

    @Value("${acp.url.endpoint:}")
    private String baseUrl;

    @Value("${acp.sid:}")
    private String explicitSid;

    @Value("${spring.datasource.url:}")
    private String postgresUrl;

    public ProcessService(DynamoDbClient dynamoDbClient, S3Client s3Client, JdbcTemplate jdbcTemplate) {
        this.dynamoDbClient = dynamoDbClient;
        this.s3Client = s3Client;
        this.jdbcTemplate = jdbcTemplate;
    }

    public ArrayNode fetchAndProcess(String urlPath) throws Exception {
        String url = buildUrl(urlPath);

        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .GET()
                .build();

        HttpResponse<String> res = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
        if (res.statusCode() != 200) {
            throw new RuntimeException("External call failed: " + res.statusCode());
        }

        JsonNode root = mapper.readTree(res.body());
        ArrayNode arr;
        if (root.isArray()) arr = (ArrayNode) root;
        else {
            arr = mapper.createArrayNode();
            arr.add(root);
        }

        ArrayNode out = mapper.createArrayNode();
        for (JsonNode n : arr) {
            if (!n.isObject()) continue;
            ObjectNode obj = (ObjectNode) n.deepCopy();
            obj.put("costPer100Moves", computeCostPer100Moves(obj));
            out.add(obj);
        }
        return out;
    }

    public void writeToDynamo(ArrayNode processed) throws Exception {
        String sid = resolve(explicitSid, postgresUrl);
        if (sid.isBlank()) throw new RuntimeException("SID not resolved");

        for (JsonNode node : processed) {
            String name = node.path("name").asText(null);
            if (name == null || name.isBlank()) continue;
            String json = mapper.writeValueAsString(node);

            Map<String, AttributeValue> item = new HashMap<>();
            item.put("name", AttributeValue.builder().s(name).build());
            item.put("data", AttributeValue.builder().s(json).build());

            dynamoDbClient.putItem(PutItemRequest.builder()
                    .tableName(sid)
                    .item(item)
                    .build());
        }
    }

    public void writeToS3(ArrayNode processed) throws Exception {
        String sid = resolve(explicitSid, postgresUrl);
        if (sid.isBlank()) throw new RuntimeException("SID not resolved");

        String bucket = sid.toLowerCase(); // ✅ IMPORTANT: bucket names must be lowercase

        for (JsonNode node : processed) {
            String name = node.path("name").asText(null);
            if (name == null || name.isBlank()) continue;

            String json = mapper.writeValueAsString(node);

            s3Client.putObject(
                    PutObjectRequest.builder().bucket(bucket).key(name).build(),
                    RequestBody.fromString(json)
            );
        }
    }

    public void writeToPostgres(String table, ArrayNode processed) throws Exception {
        String sid = resolve(explicitSid, postgresUrl);
        if (sid.isBlank()) throw new RuntimeException("SID not resolved");

        String fqTable = sid + "." + table;

        for (JsonNode node : processed) {
            String name = node.path("name").asText(null);
            if (name == null || name.isBlank()) continue;

            String json = mapper.writeValueAsString(node);

            jdbcTemplate.update(
                    "INSERT INTO " + fqTable + " (name, data) VALUES (?, ?::jsonb) " +
                            "ON CONFLICT (name) DO UPDATE SET data = EXCLUDED.data",
                    name,
                    json
            );
        }
    }

    public void copyPostgresToDynamo(String table) throws Exception {
        String sid = resolve(explicitSid, postgresUrl);
        if (sid.isBlank()) throw new RuntimeException("SID not resolved");

        List<Map<String, Object>> rows =
                jdbcTemplate.queryForList("SELECT * FROM " + table);

        for (Map<String, Object> row : rows) {

            Object nameObj = row.get("name");
            if (nameObj == null) continue;
            String name = nameObj.toString();

            Object dataObj = row.get("data");

            JsonNode payloadNode;

            if (dataObj == null) {
                payloadNode = mapper.valueToTree(row);
            } else if (dataObj instanceof String) {

                payloadNode = mapper.readTree((String) dataObj);
            } else if (dataObj instanceof Map) {

                Object maybeValue = ((Map<?, ?>) dataObj).get("value");
                if (maybeValue != null) {
                    payloadNode = mapper.readTree(maybeValue.toString());
                } else {

                    payloadNode = mapper.valueToTree(dataObj);
                }
            } else {
                String jsonValue = null;
                try {
                    var m = dataObj.getClass().getMethod("getValue");
                    Object v = m.invoke(dataObj);
                    if (v != null) jsonValue = v.toString();
                } catch (Exception ignored) { }

                if (jsonValue != null && !jsonValue.isBlank()) {
                    payloadNode = mapper.readTree(jsonValue);
                } else {
                    payloadNode = mapper.valueToTree(dataObj);
                }
            }

            String json = mapper.writeValueAsString(payloadNode);

            Map<String, AttributeValue> item = new HashMap<>();
            item.put("name", AttributeValue.builder().s(name).build());
            item.put("data", AttributeValue.builder().s(json).build());

            dynamoDbClient.putItem(PutItemRequest.builder()
                    .tableName(sid)
                    .item(item)
                    .build());
        }
    }

    public void copyPostgresToS3(String table) throws Exception {
        String sid = resolve(explicitSid, postgresUrl);
        if (sid.isBlank()) throw new RuntimeException("SID not resolved");

        String bucket = sid.toLowerCase();

        List<Map<String, Object>> rows = jdbcTemplate.queryForList("SELECT * FROM " + table);
        for (Map<String, Object> row : rows) {
            String key = UUID.randomUUID().toString();
            String json = mapper.writeValueAsString(row);

            s3Client.putObject(
                    PutObjectRequest.builder().bucket(bucket).key(key).build(),
                    RequestBody.fromString(json)
            );
        }
    }

    private String buildUrl(String urlPath) {
        if (urlPath == null) return baseUrl;
        if (urlPath.startsWith("http://") || urlPath.startsWith("https://")) return urlPath;
        if (baseUrl == null || baseUrl.isBlank()) return urlPath;
        if (baseUrl.endsWith("/") && urlPath.startsWith("/")) return baseUrl + urlPath.substring(1);
        if (!baseUrl.endsWith("/") && !urlPath.startsWith("/")) return baseUrl + "/" + urlPath;
        return baseUrl + urlPath;
    }

    private double computeCostPer100Moves(ObjectNode obj) {
        JsonNode cap = obj.path("capability");
        double costInitial = readDouble(cap, "costInitial");
        double costFinal = readDouble(cap, "costFinal");
        double costPerMove = readDouble(cap, "costPerMove");

        double out = costInitial + costFinal + (costPerMove * 100.0);
        if (Double.isNaN(out) || Double.isInfinite(out)) return 0.0;
        return out;
    }

    private double readDouble(JsonNode parent, String field) {
        if (parent == null || parent.isMissingNode() || parent.isNull()) return 0.0;
        JsonNode n = parent.get(field);
        if (n == null || n.isNull() || n.isMissingNode()) return 0.0;
        if (n.isNumber()) {
            double d = n.asDouble();
            return (Double.isNaN(d) || Double.isInfinite(d)) ? 0.0 : d;
        }
        if (n.isTextual()) {
            try {
                double d = Double.parseDouble(n.asText());
                return (Double.isNaN(d) || Double.isInfinite(d)) ? 0.0 : d;
            } catch (Exception ignored) {
                return 0.0;
            }
        }
        return 0.0;
    }
}
