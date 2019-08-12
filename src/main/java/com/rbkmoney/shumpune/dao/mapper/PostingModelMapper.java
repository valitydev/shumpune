package com.rbkmoney.shumpune.dao.mapper;

import com.rbkmoney.shumpune.constant.PostingLogFields;
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
                .planId(rs.getString(PostingLogFields.PLAN_ID))
                .operation(PostingOperation.valueOf(rs.getString(PostingLogFields.OPERATION)))
                .batchId(rs.getLong(PostingLogFields.BATCH_ID))
                .creationTime(ResultSetMapperUtils.getIntentFromRs(rs, PostingLogFields.CREATION_TIME))
                .currencySymbCode(rs.getString(PostingLogFields.CURR_SYM_CODE))
                .description(rs.getString(PostingLogFields.DESCRIPTION))
                .amount(rs.getLong(PostingLogFields.AMOUNT))
                .accountFromId(rs.getLong(PostingLogFields.FROM_ACCOUNT_ID))
                .accountToId(rs.getLong(PostingLogFields.TO_ACCOUNT_ID))
                .build();
    }
}
