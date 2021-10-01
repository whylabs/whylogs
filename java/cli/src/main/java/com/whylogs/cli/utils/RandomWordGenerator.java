package com.whylogs.cli.utils;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;
import lombok.val;
import org.apache.commons.lang3.RandomUtils;

@UtilityClass
public class RandomWordGenerator {
  private final List<String> WORDS = loadWordList();

  public String nextWord() {
    final int randomIdx = RandomUtils.nextInt(0, WORDS.size());
    return WORDS.get(randomIdx);
  }

  @SneakyThrows
  private List<String> loadWordList() {
    @Cleanup val is = RandomWordGenerator.class.getResourceAsStream("/wordlist.small.txt");
    @Cleanup val fw = new InputStreamReader(is);
    @Cleanup val br = new BufferedReader(fw);
    return br.lines().collect(Collectors.toList());
  }
}
