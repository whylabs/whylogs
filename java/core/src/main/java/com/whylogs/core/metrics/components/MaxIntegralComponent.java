package com.whylogs.core.metrics.components;

import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.Collection;

@Getter
@EqualsAndHashCode(callSuper=false)
public class MaxIntegralComponent extends IntegralComponent {
    private static final int TYPE_ID = 2;

    public MaxIntegralComponent() {
        super(Integer.MIN_VALUE);
    }

    public MaxIntegralComponent(Integer value) {
        super(value);
    }

    @Override
    public int getTypeId() {
        return TYPE_ID;
    }

    @Override
    public String getTypeName() {
        return this.getClass().getSimpleName();
    }

    public static MaxIntegralComponent max(Collection<? extends Integer> list){
        int max = Integer.MIN_VALUE;
        for(Integer i : list){
            max = Integer.max(max, i);
        }
        return new MaxIntegralComponent(max);
    }

    public static MaxIntegralComponent max(MaxIntegralComponent a, MaxIntegralComponent b) {
        return new MaxIntegralComponent(Integer.max(a.getValue(), b.getValue()));
    }
    public static MaxIntegralComponent max(int a, int b) {
        return new MaxIntegralComponent(Integer.max(a, b));
    }
}
