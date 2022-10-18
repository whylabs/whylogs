package com.whylogs.api.logger.rollingLogger;

import com.sun.org.apache.xpath.internal.functions.FuncFalse;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.util.Timer;

@Getter
@EqualsAndHashCode
@ToString
public class Scheduler {
    // Multithreading schedule.
    // Schedule a function to be called repeatedly based on a schedule

    private Timer timer;
    private float initial;
    private boolean ranInitial = false;
    private float interval;
    private Runnable func;
    private boolean isRunning = false;
    // TODO: figure out args an dkwards

    public Scheduler() {
        this.start();
    }

    private void run(){
        this.isRunning = false;
        this.start();
        this.func.run(); // TODO: figure out args and kwargs
    }

    public void start(){
        if (this.isRunning){
            return;
        }

        float interval = this.getInterval();
        if(!this.ranInitial){
            interval = this.getInitial();
            this.ranInitial = true;
        }
        this.isRunning = true;

        this.timer = new Timer(interval, this::run);
        this.timer.schedule(this::run, (long) this.initial, (long) this.interval);
    }
}
