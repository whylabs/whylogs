package com.whylogs.core.metics.components;

import lombok.Data;

@Data
public class IntegralComponent extends MetricComponent<Integer> {
    // everything is the same as MetricComponent, mtype=int

    public IntegralComponent(Integer value) {
        super(value);
    }
}
