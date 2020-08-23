package com.whylogs.cli.utils;

import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import lombok.SneakyThrows;

public class BoundedExecutor {
  private final Executor exec;
  private final Semaphore semaphore;

  public BoundedExecutor(Executor exec, int bound) {
    this.exec = exec;
    this.semaphore = new Semaphore(bound);
  }

  @SneakyThrows
  public void submitTask(final Runnable command) {
    semaphore.acquire();
    try {
      exec.execute(
          new Runnable() {
            public void run() {
              try {
                command.run();
              } finally {
                semaphore.release();
              }
            }
          });
    } catch (RejectedExecutionException e) {
      semaphore.release();
      throw e;
    }
  }
}
