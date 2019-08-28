package com.rbkmoney.shumpune.dao.mapper;

import com.rbkmoney.shumpune.constant.PlanLogFields;
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
                .id(rs.getString(PlanLogFields.PLAN_ID))
                .postingOperation(PostingOperation.valueOf(rs.getString(PlanLogFields.LAST_OPERATION)))
                .batchId(rs.getLong(PlanLogFields.LAST_BATCH_ID))
                .clock(rs.getLong(PlanLogFields.CLOCK))
                .build();
    }
}