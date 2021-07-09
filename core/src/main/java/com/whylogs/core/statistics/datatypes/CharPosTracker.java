package com.whylogs.core.statistics.datatypes;

import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;

import com.google.common.base.Joiner;
import com.whylogs.core.SummaryConverters;
import com.whylogs.core.message.CharPosMessage;
import com.whylogs.core.message.CharPosSummary;
import com.whylogs.core.message.NumberSummary;
import com.whylogs.core.statistics.NumberTracker;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

@Slf4j
@AllArgsConstructor
public class CharPosTracker {
  private Set<Character> characterList;
  @Getter private final Map<Character, NumberTracker> charPosMap;

  CharPosTracker(Set<Character> characterList) {
    this.characterList = characterList;
    this.charPosMap = newHashMap();
  }

  CharPosTracker(String charString, Map<Character, NumberTracker> charPosMap) {
    this.characterList =
        charString
            .chars()
            .mapToObj(chr -> (char) chr) // autoboxed to Character
            .collect(Collectors.toSet());
    this.charPosMap = charPosMap;
  }

  CharPosTracker(String charString) {
    this(
        charString
            .chars()
            .mapToObj(chr -> (char) chr) // autoboxed to Character
            .collect(Collectors.toSet()));
  }

  CharPosTracker() {
    this("abcdefghijklmnopqrstuvwzyz0123456789-@!#$%^&*()[]{}");
  }

  private void update(int idx, char c) {
    // TODO synchronize access
    if (characterList != null && characterList.contains(c)) {
      if (!charPosMap.containsKey(c)) {
        charPosMap.put(c, new NumberTracker());
      }
      charPosMap.get(c).track(idx);
    } else {
      // TODO self.char_pos_map["NITL"].track(indx)
      if (!charPosMap.containsKey((char) 0)) {
        charPosMap.put((char) 0, new NumberTracker());
      }
      charPosMap.get((char) 0).track(idx);
    }
  }

  private void update(int idx, int codePoint) {
    val chars = Character.toChars(codePoint);
    if (chars.length == 1) {
      update(idx, Character.toLowerCase(chars[0]));
    } else {
      // TODO synchronize access
      if (!charPosMap.containsKey((char) 0)) {
        charPosMap.put((char) 0, new NumberTracker());
      }
      charPosMap.get((char) 0).track(idx);
    }
  }

  public void update(String value) {
    AtomicInteger i = new AtomicInteger();
    value.codePoints().forEach(cp -> update(i.getAndIncrement(), cp));
  }

  public void update(String value, String charString) {
    if (charString != null) {
      val newSet =
          charString
              .chars()
              .mapToObj(chr -> (char) chr) // autoboxed to Character
              .collect(Collectors.toSet());
      if (!characterList.equals(newSet)) {
        if (!charPosMap.isEmpty()) {
          log.warn(
              "Changing character list, a non-empty character position tracker is being reset to remove ambiguities");
        }
        characterList = newSet;
        charPosMap.clear();
      }
    }

    AtomicInteger i = new AtomicInteger();
    value.codePoints().forEach(cp -> update(i.getAndIncrement(), cp));
  }

  public CharPosTracker merge(CharPosTracker other) {
    if (characterList != other.characterList && (charPosMap == null || other.charPosMap == null)) {
      log.error("Merging two non-empty Character position tracker with different character lists");
    }

    Set<Character> newCharacterList = newHashSet();
    newCharacterList.addAll(characterList);
    newCharacterList.addAll(other.characterList);

    // merge
    Map<Character, NumberTracker> newCharPosMap = newHashMap();
    newCharacterList.forEach(
        c -> {
          val tracker = charPosMap.get(c);
          val otherTracker = other.charPosMap.get(c);

          if (tracker != null && otherTracker != null) {
            newCharPosMap.put(c, tracker.merge(otherTracker));
          } else if (tracker != null) {
            newCharPosMap.put(c, tracker);
          } else if (otherTracker != null) {
            newCharPosMap.put(c, otherTracker);
          }
        });

    // merge not in the list
    val nitlTracker = charPosMap.get((char) 0);
    val otherNitlTracker = other.charPosMap.get((char) 0);

    if (nitlTracker != null && otherNitlTracker != null) {
      newCharPosMap.put((char) 0, nitlTracker.merge(otherNitlTracker));
    } else if (nitlTracker != null) {
      newCharPosMap.put((char) 0, nitlTracker);
    } else if (otherNitlTracker != null) {
      newCharPosMap.put((char) 0, otherNitlTracker);
    }
    return new CharPosTracker(newCharacterList, newCharPosMap);
  }

  public CharPosMessage toProtobuf() {
    val mapMsg =
        charPosMap.entrySet().stream()
            .collect(
                Collectors.toMap(
                    e -> e.getKey().toString(), e -> e.getValue().toProtobuf().build()));
    return CharPosMessage.newBuilder()
        .putAllCharPosMap(mapMsg)
        // TODO changes order of characters in characterlist string
        .setCharList(Joiner.on("").join(characterList))
        .build();
  }

  public static CharPosTracker fromProtobuf(CharPosMessage msg) {
    Map<Character, NumberTracker> map = newHashMap();
    msg.getCharPosMapMap().forEach((k, v) -> map.put(k.charAt(0), NumberTracker.fromProtobuf(v)));
    return new CharPosTracker(msg.getCharList(), map);
  }

  public CharPosSummary toSummary() {
    Map<String, NumberSummary> map = newHashMap();
    val nullChar = new Character((char) 0);
    charPosMap.forEach(
        (k, v) ->
            map.put(
                k.equals(nullChar) ? "NITL" : k.toString(),
                SummaryConverters.fromNumberTracker(v)));

    return CharPosSummary.newBuilder()
        .setCharacterList(Joiner.on("").join(characterList))
        .putAllCharPosMap(map)
        .build();
  }
}
