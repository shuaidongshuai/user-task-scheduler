package org.dong.demo.repo;

import org.dong.demo.domain.DemoBizTask;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public class DemoBizTaskRepository {
    private final JdbcTemplate jdbcTemplate;

    public DemoBizTaskRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void insert(String bizKey, String payload) {
        jdbcTemplate.update("""
                insert into demo_biz_task(biz_key, status, payload, create_time, update_time)
                values(?, 'SUBMIT', ?, now(), now())
                on duplicate key update payload=values(payload), update_time=now()
                """, bizKey, payload);
    }

    public void updateStatus(String bizKey, String status) {
        jdbcTemplate.update("update demo_biz_task set status=?, update_time=now() where biz_key=?", status, bizKey);
    }

    public Optional<DemoBizTask> findByBizKey(String bizKey) {
        List<DemoBizTask> list = jdbcTemplate.query("select * from demo_biz_task where biz_key=?",
                (rs, rowNum) -> {
                    DemoBizTask t = new DemoBizTask();
                    t.setId(rs.getLong("id"));
                    t.setBizKey(rs.getString("biz_key"));
                    t.setStatus(rs.getString("status"));
                    t.setPayload(rs.getString("payload"));
                    t.setCreateTime(rs.getTimestamp("create_time").toLocalDateTime());
                    t.setUpdateTime(rs.getTimestamp("update_time").toLocalDateTime());
                    return t;
                }, bizKey);
        return list.stream().findFirst();
    }
}
