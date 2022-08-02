package com.whylogs.core.metrics;

import com.whylogs.core.PreProcessedColumn;
import com.whylogs.core.SummaryConfig;
import com.whylogs.core.metrics.components.MaxIntegralComponent;
import com.whylogs.core.metrics.components.MinIntegralComponent;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;


@Data
@EqualsAndHashCode(callSuper=false)
public class IntegralMetric extends Metric{
    private MaxIntegralComponent maxComponent;
    private MinIntegralComponent minComponent;
    private final String namespace = "ints";

    public IntegralMetric() {
        // zeros the metrics out
        this.maxComponent = new MaxIntegralComponent(Integer.MIN_VALUE);
        this.minComponent = new MinIntegralComponent(Integer.MAX_VALUE);
    }

    public IntegralMetric(MaxIntegralComponent maxComponent, MinIntegralComponent minComponent) {
        this.maxComponent = maxComponent;
        this.minComponent = minComponent;
    }

    public OperationResult columnarUpdate(PreProcessedColumn data){
        if(data.getLength() == 0){
            return OperationResult.ok();
        }

        int successes = 0;
        int max_ = this.maxComponent.getValue();
        int min_ = this.minComponent.getValue();

        if(data.hasListInts()){
            ArrayList<Integer> data_list = data.getLists().getInts();
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
        return new IntegralMetric();
    }

    @Override
    public HashMap<String, Integer> toSummaryDict(SummaryConfig config){
        // This component does not need the config, but others do
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
