package com.whylogs.core.views;

import java.nio.charset.StandardCharsets;
import lombok.experimental.UtilityClass;

@UtilityClass
public class WhylogsMagicUtility {
  public static final String WHYLOGS_MAGIC_HEADER = "WHY1";
  public static final int WHYLOGS_MAGIC_HEADER_LENGTH = WHYLOGS_MAGIC_HEADER.length();
  public static final byte[] WHYLOGS_MAGIC_HEADER_BYTES =
      WHYLOGS_MAGIC_HEADER.getBytes(StandardCharsets.UTF_8);;
}
