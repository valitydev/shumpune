package com.rbkmoney.shumpune.dao.mapper;

import com.rbkmoney.shumpune.constant.PostingOperation;
import com.rbkmoney.shumpune.domain.PostingModel;
import com.rbkmoney.shumpune.utils.ResultSetMapperUtils;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import java.sql.ResultSet;
import java.sql.SQLException;

@Component
public class PostingModelMapper implements RowMapper<PostingModel> {

    @Override
    public PostingModel mapRow(ResultSet rs, int rowNum) throws SQLException {
        return PostingModel.builder()
                .planId(rs.getString("plan_id"))
                .operation(PostingOperation.valueOf(rs.getString("operation")))
                .batchId(rs.getLong("batch_id"))
                .creationTime(ResultSetMapperUtils.getIntentFromRs(rs, "creation_time"))
                .currencySymbCode(rs.getString("curr_sym_code"))
                .description(rs.getString("description"))
                .amount(rs.getLong("amount"))
                .accountFromId(rs.getLong("from_account_id"))
                .accountToId(rs.getLong("to_account_id"))
                .build();
    }
}
