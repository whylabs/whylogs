package com.whylogs.api.writer;

import java.io.File;
import java.io.FileWriter;

public interface Writable {

    static FileWriter safeOpenWrite(String path) {
        // Open 'path' for writing, creating any parent directories as needed
        File file = new File(path);
        FileWriter writer = null;
        try {
            writer = new FileWriter(file, true);
        } catch (Exception e) {
            System.out.println("Error: " + e);
            e.printStackTrace();
        }

        // this close happens latter on
        return writer;
    }
}
