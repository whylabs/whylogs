package com.whylogs.core.metrics.components;

import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper=false)
public class IntegralComponent extends MetricComponent<Integer> {
    // everything is the same as MetricComponent, mtype=int

    public IntegralComponent() {super(0);}
    public IntegralComponent(Integer value) {
        super(value);
    }
}
