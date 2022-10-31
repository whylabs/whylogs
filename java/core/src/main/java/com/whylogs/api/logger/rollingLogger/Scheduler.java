package com.whylogs.api.logger.rollingLogger;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Getter
@EqualsAndHashCode
@ToString
public class Scheduler {
  // Multithreading schedule.
  // Schedule a function to be called repeatedly based on a schedule
  private ScheduledExecutorService scheduledService;
  private float initial;
  private boolean ranInitial = false;
  private float interval;
  private Runnable func;
  private boolean isRunning = false;
  private String[] args;
  // TODO: figure out args an dkwards

  public Scheduler(float initial, float interval, Runnable func, String[] args) {
    this.initial = initial;
    this.interval = interval;
    this.func = func;
    this.args = args;
    this.start();
  }

  private void run() {
    // TODO: Looking at this I think this is wrong to have lines 35 & 36
    this.isRunning = false;
    this.start(); // Question: why do we need to start again?
    this.func.run(); // TODO: figure out args and kwargs
  }

  public void start() {
    if (this.isRunning) {
      return;
    }

    float initial = 0;
    if (!this.ranInitial) {
      initial = this.getInitial();
      this.ranInitial = true;
    }

    this.scheduledService = Executors.newSingleThreadScheduledExecutor();
    this.scheduledService.scheduleAtFixedRate(
        this::run, (long) initial, (long) this.interval, TimeUnit.SECONDS);
    this.isRunning = true;
  }

  public void stop() {
    this.scheduledService.shutdown();
    this.isRunning = false;
  }
}
