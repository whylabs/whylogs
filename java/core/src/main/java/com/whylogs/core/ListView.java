package com.whylogs.core;

import java.util.ArrayList;
import lombok.Data;

@Data
public class ListView {
  private ArrayList<Integer> ints;
  private ArrayList<String> strings;
  private ArrayList<Double> doubles;
  private ArrayList<Object> objects;

  // TODO: iterable in python is easy , but here the typing is a bit more complicated

  public ListView() {
    this.ints = new ArrayList<>();
    this.strings = new ArrayList<>();
    this.doubles = new ArrayList<>();
    this.objects = new ArrayList<>();
  }

  public void add(int i) {
    this.ints.add(i);
  }

  public void add(String s) {
    this.strings.add(s);
  }

  public void add(double d) {
    this.doubles.add(d);
  }

  public void add(Object o) {
    this.objects.add(o);
  }
}
