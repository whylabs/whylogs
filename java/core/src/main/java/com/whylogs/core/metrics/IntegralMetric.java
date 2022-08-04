package com.whylogs.core.metrics;

import com.whylogs.core.PreProcessedColumn;
import com.whylogs.core.SummaryConfig;
import com.whylogs.core.metrics.components.MaxIntegralComponent;
import com.whylogs.core.metrics.components.MinIntegralComponent;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

@Getter
@EqualsAndHashCode(callSuper=false)
public class IntegralMetric extends BaseMetric<IntegralMetric> {
    public static final String DEFAULT_NAMESPACE = "ints";
    private MaxIntegralComponent maxComponent;
    private MinIntegralComponent minComponent;

    public IntegralMetric(){
        this(IntegralMetric.DEFAULT_NAMESPACE);
    }

    public IntegralMetric(String namespace) {
        this(new MaxIntegralComponent(Integer.MIN_VALUE), new MinIntegralComponent(Integer.MAX_VALUE), namespace);
    }

    public IntegralMetric(MaxIntegralComponent maxComponent, MinIntegralComponent minComponent) {
        this(maxComponent, minComponent, IntegralMetric.DEFAULT_NAMESPACE);
    }

    public IntegralMetric(MaxIntegralComponent maxComponent, MinIntegralComponent minComponent, String namespace) {
        super(namespace);

        this.maxComponent = maxComponent;
        this.minComponent = minComponent;
    }

    private void setMax(int max){
        this.maxComponent = new MaxIntegralComponent(max);
    }
    private void setMin(int min){
        this.minComponent = new MinIntegralComponent(min);
    }

    @Override
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
    public HashMap<String, Object> toSummaryDict(SummaryConfig config){
        // This metric does not need the config, but others do
        HashMap<String, Object> summary = new HashMap<>();
        summary.put("max", this.maxComponent.getValue());
        summary.put("min", this.minComponent.getValue());
        return summary;
    }

    @Override
    public IntegralMetric merge(IntegralMetric other) {
        // QUESTION: Is there ever a case were we want to merge an Integral with say a Distribution Metric?

        // QUESTION: What happens if they have two different namespaces? Putting this for now
        if (!this.getNamespace().equals(other.getNamespace())) {
            throw new IllegalArgumentException("Cannot merge IntegralMetrics with different namespaces");
        }

        int max = Integer.max(this.maxComponent.getValue(), other.maxComponent.getValue());
        int min = Integer.min(this.minComponent.getValue(), other.minComponent.getValue());

        return new IntegralMetric(new MaxIntegralComponent(max), new MinIntegralComponent(min), this.getNamespace());
    }
}
