package com.rbkmoney.shumpune.utils;

import com.rbkmoney.geck.common.util.TypeUtil;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class ResultSetMapperUtils {

    public static String getTimeWithConvertToString(ResultSet rs, String fieldName) throws SQLException {
        return TypeUtil.temporalToString(getIntentFromRs(rs, fieldName));
    }

    public static Instant getIntentFromRs(ResultSet rs, String fieldName) throws SQLException {
        return rs.getObject(fieldName, LocalDateTime.class).toInstant(ZoneOffset.UTC);
    }

}
