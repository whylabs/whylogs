package com.whylogs.core.metics;

import com.whylogs.core.metics.OperationResult;
import lombok.Data;

import java.util.ArrayList;
import java.util.Collections;

// TODO: lots of things to implement from supporting classes

@Data
public class IntergralMetric extends Metric{
    private MaxIntegralComponent;
    private MinIntegralComponent;
    private final String namespace = "ints";

    public OperationResult columnarUpdate(PreprocessedColumn data){
        if(data.length() == 0){
            return OperationResult.ok();
        }

        int successes = 0;
        int max_ = this.max.getValue();
        int min_ = this.min.getValue();

        // TODO: Double check we don't have anything similar to numpy here

        if(data.hasListInts()){
            ArrayList<Integer> data_list = data.getListInts()
            int l_max = Collections.max(data_list);
            int l_min = Collections.min(data_list);
            max_ = Integer.max(max_, l_max);
            min_ = Integer.min(min_, l_min);
            successes += data_list.size();
        }

        this.setMax(max_);
        this.setMin(min_);
        return OperationResult.ok(successes);
    }

    @Override
    public IntergralMetric zero(MetricConfig config){
        return IntergralMetric(MaxIntegralComponent(Integer.MIN_VALUE), MinIntegralComponent(Integer.MAX_VALUE));
    }
}
