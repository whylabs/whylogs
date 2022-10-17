package com.whylogs.api.logger;

import com.whylogs.api.writer.Writer;
import com.whylogs.api.writer.WritersRegistry;
import com.whylogs.core.schemas.DatasetSchema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.ArrayList;

@NoArgsConstructor
@Getter
@EqualsAndHashCode
public abstract class Logger implements AutoCloseable {
    private boolean isClosed = false;
    private DatasetSchema schema;
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

}
