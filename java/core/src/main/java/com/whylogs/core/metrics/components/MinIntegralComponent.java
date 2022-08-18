package com.whylogs.core.metrics.components;

import java.util.Collection;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@EqualsAndHashCode(callSuper = false)
public class MinIntegralComponent extends IntegralComponent {
  private static final int TYPE_ID = 1;

  public MinIntegralComponent() {
    super(Integer.MAX_VALUE);
  }

  public MinIntegralComponent(Integer value) {
    super(value);
  }
  
  @Override
  public String getTypeName() {
    return this.getClass().getSimpleName();
  }

  @Override
  public int getTypeId() {
    return TYPE_ID;
  }
  
  public static MinIntegralComponent min(Collection<? extends Integer> list) {
    int min = Integer.MAX_VALUE;
    for (Integer i : list) {
      min = Integer.min(min, i);
    }
    return new MinIntegralComponent(min);
  }

  public static MinIntegralComponent min(MinIntegralComponent a, MinIntegralComponent b) {
    return new MinIntegralComponent(Integer.min(a.getValue(), b.getValue()));
  }

  public static MinIntegralComponent min(int a, int b) {
    return new MinIntegralComponent(Integer.min(a, b));
  }
}
