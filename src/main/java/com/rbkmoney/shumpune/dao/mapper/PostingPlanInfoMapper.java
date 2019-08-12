package com.rbkmoney.shumpune.dao.mapper;

import com.rbkmoney.shumpune.constant.PostingOperation;
import com.rbkmoney.shumpune.domain.PostingPlanInfo;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import java.sql.ResultSet;
import java.sql.SQLException;

@Component
public class PostingPlanInfoMapper implements RowMapper<PostingPlanInfo> {

    @Override
    public PostingPlanInfo mapRow(ResultSet rs, int rowNum) throws SQLException {
        return PostingPlanInfo.builder()
                .id(rs.getString("plan_id"))
                .postingOperation(PostingOperation.valueOf(rs.getString("last_operation")))
                .batchId(rs.getLong("last_batch_id"))
                .clock(rs.getLong("CLOCK"))
                .build();
    }
}