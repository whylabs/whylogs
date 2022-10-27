package com.whylogs.api.logger.rollingLogger;

import com.whylogs.api.logger.Logger;
import com.whylogs.api.writer.Writer;
import com.whylogs.core.DatasetProfile;
import com.whylogs.core.schemas.DatasetSchema;
import com.whylogs.core.views.DatasetProfileView;
import org.apache.commons.lang3.NotImplementedException;

import java.lang.reflect.Array;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;

public class TimedRollingLogger extends Logger implements AutoCloseable{
    // A rolling logger that continuously rotates files based on time
    private DatasetSchema schema;
    private String baseName;
    private String fileExtension;
    private int interval;
    private Character when = 'H'; // TODO: Make the Literals of S M H D
    private boolean utc = false;
    private boolean align = true;
    private boolean skipEmpty = false;
    private String suffix;

    private DatasetProfile currentProfile;
    private Callable<Writer> callback; // TODO: this isn't the write signature
    private Scheduler scheduler;
    private int currentBatchTimestamp;

    // TODO: callback: Optional[Callable[[Writer, DatasetProfileView, str], None]]
    public TimedRollingLogger(DatasetSchema schema, String baseName, String fileExtension, int interval) {
        this(schema, baseName, fileExtension, interval, 'H', false, true, false);
    }

    public TimedRollingLogger(DatasetSchema schema, String baseName, String fileExtension, int interval, Character when) {
        this(schema, baseName, fileExtension, interval, when, false, true, false);
    }

    public TimedRollingLogger(DatasetSchema schema, String baseName, String fileExtension, int interval, Character when, boolean utc, boolean align, boolean skipEmpty) {
        super(schema);

        this.schema = schema;
        this.baseName = baseName;
        this.fileExtension = fileExtension;
        this.interval = interval;
        this.when = Character.toUpperCase(when);
        this.utc = utc;
        this.align = align;
        this.skipEmpty = skipEmpty;

        if(this.baseName == null || this.baseName.isEmpty()) {
            this.baseName = "profile";
        }
        if(this.fileExtension == null || this.fileExtension.isEmpty()) {
            this.fileExtension = ".bin"; // TODO: should we make this .whylogs?
        }

        switch(this.when) {
            case 'S':
                this.interval = 1; // one second
                this.suffix = "%Y-%m-%d_%H-%M-%S";
                break;
            case 'M':
                this.interval = 60; // one minute
                this.suffix = "%Y-%m-%d_%H-%M";
                break;
            case 'H':
                this.interval = 60 * 60; // one hour
                this.suffix = "%Y-%m-%d_%H";
                break;
            case 'D':
                this.interval = 60 * 60 * 24; // one day
                this.suffix = "%Y-%m-%d";
                break;
            default:
                throw new IllegalArgumentException("Invalid value for when: " + this.when);
        }

        this.interval = this.interval * interval; /// multiply by units requested
        this.utc = utc;

        Instant currentTime = Instant.now();
        this.currentBatchTimestamp = this.computeCurrentBatchTimestamp(currentTime.getEpochSecond());
        this.currentProfile = new DatasetProfile(schema, currentTime, currentTime);
        int initialRunAfter = (this.currentBatchTimestamp + this.interval) - (int) currentTime.getEpochSecond();
        if(initialRunAfter < 0) {
            // TODO: Add logging error as this shouldn't happen
            initialRunAfter = this.interval;
        }

        this.scheduler = new Scheduler(initialRunAfter, this.interval, this::doRollover, null);
        this.scheduler.start();

        // autocloseable closes at end
    }

    private int computeCurrentBatchTimestamp(long nowEpoch) {
        int roundedNow = (int) nowEpoch;
        if(this.align){
           return (Math.floorDiv((roundedNow - 1), this.interval)) * this.interval + this.interval;
        }
        return roundedNow;
    }

    public void checkWriter(Writer writer){
        writer.check_interval(this.interval);
    }

    private ArrayList<DatasetProfile> getMatchingProfiles(){
        ArrayList<DatasetProfile> matchingProfiles = new ArrayList<>();
        matchingProfiles.add(this.currentProfile);
        return matchingProfiles;
    }

    @Override
    protected ArrayList<DatasetProfile> getMatchingProfiles(Object data) {
        return this.getMatchingProfiles();
    }

    @Override
    protected <O> ArrayList<DatasetProfile> getMatchingProfiles(Map<String, O> data) {
        return this.getMatchingProfiles();
    }

    private void doRollover() {
        if(this.isClosed()) {
            return;
        }

        DatasetProfile oldProfile = this.currentProfile;
        Instant currentTime = Instant.now();
        this.currentBatchTimestamp = this.computeCurrentBatchTimestamp(currentTime.getEpochSecond());
        this.currentProfile = new DatasetProfile(schema, currentTime, currentTime);

        this.flush(oldProfile);
    }

    private void flush(DatasetProfile profile) {
        if (profile == null) {
            return;
        } else if (this.skipEmpty && profile.isEmpty()) {
            // set logger logger.debug("skip_empty is set. Skipping empty profiles")
            return;
        }

        // get time to get name
        String timedFileName = this.baseName + "_" + this.currentBatchTimestamp + this.fileExtension;

        // Sleep while the profile is active?
        // TODO: this is where we call the store list.write
        // TODO: go through through the writers
    }

    public void close() {
        // TODO log that we are closing the writer
        if(!this.isClosed()) {
            // Autoclose handles the isCLosed()
            this.scheduler.stop();
            this.flush(this.currentProfile);
        }
    }
}
