package com.whylabs.logging.demo.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.val;

public class WordListGenerator {
  @SneakyThrows
  public static void main(String[] args) {
    // source: http://www-personal.umich.edu/\~jlawler/wordlist
    final String path = "/Users/andy/Workspace/whylogs-java/wordlist";
    final Pattern azOnly = Pattern.compile("[a-zA-Z]+$");
    @Cleanup val br = new BufferedReader(new FileReader(path));
    final List<String> words =
        br.lines()
            .filter(w -> azOnly.matcher(w).matches())
            .map(String::toLowerCase)
            .filter(w -> w.length() < 10)
            .filter(w -> w.length() > 5)
            .distinct()
            .collect(Collectors.toList());

    Collections.shuffle(words);

    try (val fw = new FileWriter("/Users/andy/Workspace/whylogs-java/wordlist.small.txt");
        val writer = new BufferedWriter(fw)) {

      for (int i = 0; i < words.size() && i < 1000; i++) {
        writer.write(words.get(i));
        writer.write('\n');
      }
    }
  }
}
