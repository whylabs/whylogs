package com.whylogs.api.logger;

import com.whylogs.api.logger.resultsets.ProfileResultSet;
import com.whylogs.api.logger.resultsets.ResultSet;
import com.whylogs.api.writer.Writer;
import com.whylogs.api.writer.WritersRegistry;
import com.whylogs.core.DatasetProfile;
import com.whylogs.core.schemas.DatasetSchema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

@NoArgsConstructor
@Getter
@EqualsAndHashCode
@ToString
public abstract class Logger implements AutoCloseable {
    private boolean isClosed = false;
    private DatasetSchema schema = new DatasetSchema();
    private ArrayList<Writer> writers = new ArrayList<>();

    public Logger(DatasetSchema schema) {
        this.schema = schema;
    }

    public <T extends Writer> void checkWriter(T Writer) {
        // Checks if a writer is configured correctly for this class
        // Question: why is this empty but not an abstract?
    }

    public void appendWriter(String name){
        if(name == null || name.isEmpty()){
            throw new IllegalArgumentException("Writer name cannot be empty");
        }

        Writer writer = WritersRegistry.get(name);
        if(writer == null){
            throw new IllegalArgumentException("Writer " + name + " is not registered");
        }

        appendWriter(writer);
    }

    public void appendWriter(Writer writer){
        if(writer == null){
            throw new IllegalArgumentException("Writer cannot be null");
        }

        checkWriter(writer);
        writers.add(writer);
    }

    protected abstract ArrayList<DatasetProfile> getMatchingProfiles(Object data);
    protected abstract <O> ArrayList<DatasetProfile> getMatchingProfiles(Map<String, O> data);

    @Override
    public void close(){
        isClosed = true;
    }

    public ResultSet log(HashMap<String, Object> data){
        // What type of data is the object? Right now we don't process that in track.
        if(isClosed){
            throw new IllegalStateException("Logger is closed");
        } else if(data == null){
            throw new IllegalArgumentException("Data cannot be null");
        }

        // TODO: implement segment processing here

        ArrayList<DatasetProfile> profiles = getMatchingProfiles(data);
        for(DatasetProfile profile : profiles){
            profile.track(data);
        }

        // Question: Why does this only return the first profile? IS this
        // getting ready for multiple profiles later on?
        return new ProfileResultSet(profiles.get(0));
    }
}
