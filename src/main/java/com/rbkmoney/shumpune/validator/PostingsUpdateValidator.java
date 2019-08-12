package com.rbkmoney.shumpune.validator;


import com.rbkmoney.damsel.shumpune.PostingPlan;
import com.rbkmoney.shumpune.domain.PostingModel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class PostingsUpdateValidator {

    public void validate(PostingPlan postingPlan, Map<Long, List<PostingModel>> postingsModels) throws TException {

    }

}
