package com.whylogs.core.metics;

import com.whylogs.core.metics.components.MaxIntegralComponent;
import com.whylogs.core.metics.components.MinIntegralComponent;
import lombok.Data;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

// TODO: lots of things to implement from supporting classes

@Data
public class IntegralMetric extends Metric{
    private MaxIntegralComponent maxComponent;
    private MinIntegralComponent minComponent;
    private final String namespace = "ints";

    public IntegralMetric() {
        // TODO: Should we initialize these to the same as zero?
        this.maxComponent = new MaxIntegralComponent(Integer.MIN_VALUE);
        this.minComponent = new MinIntegralComponent(Integer.MAX_VALUE);
    }

    public IntegralMetric(MaxIntegralComponent maxComponent, MinIntegralComponent minComponent) {
        this.maxComponent = maxComponent;
        this.minComponent = minComponent;
    }

    public OperationResult columnarUpdate(PreprocessedColumn data){
        if(data.length() == 0){
            return OperationResult.ok();
        }

        int successes = 0;
        int max_ = this.maxComponent.getValue();
        int min_ = this.minComponent.getValue();

        // TODO: Double check we don't have anything similar to numpy here

        if(data.hasListInts()){
            ArrayList<Integer> data_list = data.getListInts();
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
    public IntegralMetric zero(MetricConfig config){
        return new IntegralMetric(new MaxIntegralComponent(Integer.MIN_VALUE), new MinIntegralComponent(Integer.MAX_VALUE));
    }


    @Override
    //TODO: why would the config be passed in if we don't use it. Come back t
    public HashMap<String, Integer> toSummaryDict(SummaryConfig config){
        HashMap<String, Integer> summary = new HashMap<String, Integer>();
        summary.put("max", this.maxComponent.getValue());
        summary.put("min", this.minComponent.getValue());
        return summary;
    }

    private void setMax(int max){
        this.maxComponent = new MaxIntegralComponent(max);
    }

    private void setMin(int min){
        this.minComponent = new MinIntegralComponent(min);
    }
}
