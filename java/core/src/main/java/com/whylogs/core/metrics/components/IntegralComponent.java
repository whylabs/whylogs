package com.whylogs.core.metrics.components;

import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper=false)
public class IntegralComponent extends MetricComponent<Integer> {
    // everything is the same as MetricComponent, mtype=int

    public IntegralComponent() {super(0);}
    public IntegralComponent(Integer value) {
        super(value);
    }

    @Override
    public String getTypeName() {
        return this.getClass().getSimpleName();
    }
}
