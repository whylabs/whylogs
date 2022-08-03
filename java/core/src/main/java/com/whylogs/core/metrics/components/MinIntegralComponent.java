package com.whylogs.core.metrics.components;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper=false)
public class MinIntegralComponent extends IntegralComponent {
    private final int type_id = 1;

    public MinIntegralComponent(Integer value) {
        super(value);
    }

    public static Integer min(Integer a, Integer b) {
        return Integer.min(a, b);
    }
}
