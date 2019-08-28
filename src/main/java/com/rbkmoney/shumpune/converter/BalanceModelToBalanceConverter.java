package com.rbkmoney.shumpune.converter;

import com.rbkmoney.damsel.shumpune.Balance;
import com.rbkmoney.damsel.shumpune.Clock;
import com.rbkmoney.shumpune.domain.BalanceModel;
import com.rbkmoney.shumpune.utils.VectorClockSerializer;
import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;

@Component
public class BalanceModelToBalanceConverter implements Converter<BalanceModel, Balance> {

    @Override
    public Balance convert(BalanceModel balanceModel) {
        return new Balance()
                .setId(balanceModel.getAccountId())
                .setClock(Clock.vector(VectorClockSerializer.serialize(balanceModel.getClock())))
                .setMinAvailableAmount(balanceModel.getMinAvailableAmount())
                .setMaxAvailableAmount(balanceModel.getMaxAvailableAmount())
                .setOwnAmount(balanceModel.getOwnAmount());
    }

}
