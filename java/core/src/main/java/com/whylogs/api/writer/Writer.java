package com.whylogs.api.writer;

import java.util.Optional;

// TODO: this is a temp holding class for logger that will be implmented next
public abstract class Writer {
  /*Validate an interval configuration for a given writer.
  Some writer only accepts certain interval configuration. By default, this should return True for a valid
  non-negative interval.*/
  public void check_interval(int interval_seconds) {
    // TODO: implement (not implemented in java either
  }

  public abstract void write(Writable file, Optional<String> dest);

  public abstract <T extends Writer> T option(T writer);
}
