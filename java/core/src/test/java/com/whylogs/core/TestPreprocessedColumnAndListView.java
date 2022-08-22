package com.whylogs.core;

import java.util.ArrayList;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class TestPreprocessedColumnAndListView {

  private void assert_zero_len(List<?> values) {
    if (values == null) {
      return;
    }
    Assert.assertEquals(values.size(), 0);
  }

  @Test
  public void test_floats_ints_strings() {
    ArrayList<Object> mixed = new ArrayList<>();
    PreprocessedColumn results;

    mixed.add(1.0);
    mixed.add((float) 2.0);
    mixed.add(1);
    mixed.add(2);
    mixed.add("hello");
    mixed.add(null);

    results = PreprocessedColumn.apply(mixed);

    ArrayList<Double> double_answer = new ArrayList<>();
    double_answer.add(1.0);
    double_answer.add(2.0);

    Assert.assertEquals(results.getLists().getDoubles(), double_answer);
    Assert.assertEquals(results.getLists().getInts(), mixed.subList(2, 4));
    Assert.assertEquals(results.getLists().getStrings(), mixed.subList(4, 5));
    assert_zero_len(results.getLists().getObjects());
    Assert.assertEquals(results.getNullCount(), 1);
  }

  @Test
  public void test_bools_and_ints() {
    ArrayList<Object> mixed = new ArrayList<>();
    PreprocessedColumn results;

    mixed.add(true);
    mixed.add(false);
    mixed.add(1);
    mixed.add(2);
    mixed.add(true);

    results = PreprocessedColumn.apply(mixed);

    Assert.assertEquals(results.getBoolCount(), 3);
    Assert.assertEquals(results.getBoolCountWhereTrue(), 2);
    Assert.assertEquals(results.getLists().getInts(), mixed.subList(2, 4));
    assert_zero_len(results.getLists().getObjects());
    assert_zero_len(results.getLists().getDoubles());
    assert_zero_len(results.getLists().getStrings());
  }

  @Test
  public void test_objects_with_null() {
    ArrayList<ArrayList<Integer>> objects = new ArrayList<>();
    ArrayList<Integer> ints = new ArrayList<>();

    ints.add(1);
    ints.add(2);
    ints.add(3);

    objects.add(ints);
    objects.add(ints);
    objects.add(ints);
    objects.add(null);

    PreprocessedColumn results = PreprocessedColumn.apply(objects);
    Assert.assertEquals(results.getLists().getObjects().size(), 3);
    Assert.assertEquals(results.getNullCount(), 1);
    assert_zero_len(results.getLists().getDoubles());
    assert_zero_len(results.getLists().getInts());
    assert_zero_len(results.getLists().getStrings());
  }

  @Test
  public void test_floats_with_null() {
    ArrayList<Object> floats_with_null = new ArrayList<>();
    PreprocessedColumn results;

    floats_with_null.add(1.0);
    floats_with_null.add(null);
    floats_with_null.add(1.222);
    floats_with_null.add(null);
    floats_with_null.add(3.14);
    results = PreprocessedColumn.apply(floats_with_null);

    ArrayList<Double> floats = new ArrayList<>();
    floats.add(1.0);
    floats.add(1.222);
    floats.add(3.14);

    Assert.assertEquals(results.getLists().getDoubles(), floats);
    Assert.assertEquals(results.getNullCount(), 2);
    assert_zero_len(results.getLists().getStrings());
    assert_zero_len(results.getLists().getInts());
    assert_zero_len(results.getLists().getObjects());
  }

  @Test
  public void test_floats() {
    ArrayList<Double> floats = new ArrayList<>();
    PreprocessedColumn results;

    floats.add(1.0);
    floats.add(1.222);
    floats.add(3.14);
    results = PreprocessedColumn.apply(floats);

    Assert.assertEquals(results.getLists().getDoubles(), floats);
    Assert.assertEquals(results.getNullCount(), 0);
    assert_zero_len(results.getLists().getStrings());
    assert_zero_len(results.getLists().getInts());
    assert_zero_len(results.getLists().getObjects());
  }

  @Test
  public void test_ints() {
    ArrayList<Integer> ints = new ArrayList<>();
    PreprocessedColumn results;

    ints.add(1);
    ints.add(2);
    ints.add(3);
    results = PreprocessedColumn.apply(ints);

    Assert.assertEquals(results.getLists().getInts(), ints);
    Assert.assertEquals(results.getNullCount(), 0);
    assert_zero_len(results.getLists().getDoubles());
    assert_zero_len(results.getLists().getStrings());
    assert_zero_len(results.getLists().getObjects());
  }

  @Test
  public void test_ints_with_null() {
    ArrayList<Object> ints_with_null = new ArrayList<>();
    PreprocessedColumn results;

    ints_with_null.add(1);
    ints_with_null.add(null);
    ints_with_null.add((short) 2);
    ints_with_null.add(null);
    ints_with_null.add((byte) 3);
    results = PreprocessedColumn.apply(ints_with_null);

    ArrayList<Integer> ints = new ArrayList<>();
    ints.add(1);
    ints.add(2);
    ints.add(3);

    Assert.assertEquals(results.getLists().getInts(), ints);
    Assert.assertEquals(results.getNullCount(), 2);
    assert_zero_len(results.getLists().getDoubles());
    assert_zero_len(results.getLists().getStrings());
    assert_zero_len(results.getLists().getObjects());
  }

  @Test
  public void test_strings() {
    ArrayList<Object> strings = new ArrayList<>();
    PreprocessedColumn results;

    strings.add("hello");
    strings.add("world");
    strings.add('!');
    results = PreprocessedColumn.apply(strings);

    strings.remove(2);
    strings.add("!");

    Assert.assertEquals(results.getLists().getStrings(), strings);
    Assert.assertEquals(results.getNullCount(), 0);
    assert_zero_len(results.getLists().getDoubles());
    assert_zero_len(results.getLists().getInts());
    assert_zero_len(results.getLists().getObjects());
  }

  @Test
  public void test_strings_with_null() {
    ArrayList<Object> strings_with_null = new ArrayList<>();
    PreprocessedColumn results;

    strings_with_null.add("hello");
    strings_with_null.add(null);
    strings_with_null.add("world");
    strings_with_null.add(null);
    strings_with_null.add("!");
    results = PreprocessedColumn.apply(strings_with_null);

    ArrayList<String> strings = new ArrayList<>();
    strings.add("hello");
    strings.add("world");
    strings.add("!");

    Assert.assertEquals(results.getLists().getStrings(), strings);
    Assert.assertEquals(results.getNullCount(), 2);
    assert_zero_len(results.getLists().getDoubles());
    assert_zero_len(results.getLists().getInts());
    assert_zero_len(results.getLists().getObjects());
  }
}
