package acp.submission.service;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class PostgresService {
    private final JdbcTemplate jdbcTemplate;

    public PostgresService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public List<Map<String, Object>> readAll(String table) {
        // simple for now; we will harden later
        return jdbcTemplate.queryForList("SELECT * FROM " + table);
    }
}

