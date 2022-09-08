package com.whylogs.core.views;

import com.whylogs.core.errors.DeserializationError;
import com.whylogs.core.metrics.components.MetricComponent;
import com.whylogs.core.utils.ProtobufUtil;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import whylogs.core.message.*;

import java.io.*;
import java.lang.reflect.Type;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.util.*;
import java.util.stream.Collectors;

import static com.whylogs.core.utils.ProtobufUtil.writeDelimitedProtobuf;
import static java.util.stream.Collectors.toMap;

// TODO: extend writable
@AllArgsConstructor
@Getter
@ToString
public class DatasetProfileView {
    private HashMap<String, ColumnProfileView> columns;
    private Date datasetTimestamp;
    private Date creationTimestamp;

    public DatasetProfileView merge(DatasetProfileView otherView) {
        HashMap<String, ColumnProfileView> mergedColumns = new HashMap<>();
        HashSet<String> allNames = new HashSet<>();
        allNames.addAll(this.columns.keySet());
        allNames.addAll(otherView.columns.keySet());

        for (String columnName : allNames) {
            ColumnProfileView thisColumn = this.columns.get(columnName);
            ColumnProfileView otherColumn = otherView.columns.get(columnName);

            ColumnProfileView result = thisColumn;

            if (thisColumn != null && otherColumn != null) {
                result = thisColumn.merge(otherColumn);
            } else if (otherColumn != null) {
                result = otherColumn;
            }

            mergedColumns.put(columnName, result);
        }

        return new DatasetProfileView(mergedColumns, this.datasetTimestamp, this.creationTimestamp);
    }

    public Optional<ColumnProfileView> getColumn(String columnName) {
        return Optional.ofNullable(this.columns.get(columnName));
    }

    public HashMap<String, ColumnProfileView> getColumns(Optional<ArrayList<String>> colNames) {
        if (colNames.isPresent()) {
            HashMap<String, ColumnProfileView> result = new HashMap<>();
            for (String colName : colNames.get()) {
                result.put(colName, this.columns.get(colName));
            }
            return result;
        } else {
            return this.columns;
        }
    }

    public String getDefaultPath() {
        return "profile_" + this.creationTimestamp + ".bin";
    }

    // TODO: we need get components
    public void write(Optional<String> path) {
        HashSet<String> allComponentNames = new HashSet<>();
        HashMap<String, Integer> metricNameToIndex = new HashMap<>();
        HashMap<Integer, String> indexToMetricName = new HashMap<>();
        HashMap<String, ChunkOffsets> columnChunkOffsets = new HashMap<>();
        String pathName = path.orElseGet(this::getDefaultPath);

        for (String colName : this.columns.keySet()) {
            ColumnProfileView column = this.columns.get(colName);
            allComponentNames.addAll(column.getComponents().keySet());
        }
        allComponentNames.stream().sorted().forEach(name -> {
            int index = metricNameToIndex.size();
            metricNameToIndex.put(name, index);
            indexToMetricName.put(index, name);
        });

        String tempPath = System.getProperty("java.io.tmpdir") + File.separator + "whylogs" + File.separator + "temp_" + this.creationTimestamp + ".bin";
        try (RandomAccessFile file = new RandomAccessFile(tempPath, "rw")) {
            OutputStream outputStream = Channels.newOutputStream(file.getChannel());
            for (String colName : this.columns.keySet().stream().sorted().collect(Collectors.toList())) {
                ColumnProfileView currentColumn = this.columns.get(colName);
                columnChunkOffsets.put(colName, ChunkOffsets.newBuilder().addOffsets(file.getFilePointer()).build());

                // Chunk the column
                HashMap<Integer, MetricComponentMessage> indexComponentMetric = new HashMap<>();
                Map<String, MetricComponentMessage> metricComponentMap = currentColumn.toProtobuf().getMetricComponentsMap();

                for (String metricName : metricComponentMap.keySet()) {
                    if (metricNameToIndex.containsKey(metricName)) {
                        indexComponentMetric.put(metricNameToIndex.get(metricName), metricComponentMap.get(metricName));
                    } else {
                        throw new InputMismatchException("Missing metric from index map. Metric name: " + metricName);
                    }
                }

                ChunkMessage chunkMsg = ChunkMessage.newBuilder().putAllMetricComponents(indexComponentMetric).build();
                ChunkHeader chunkHeader = ChunkHeader.newBuilder().setType(ChunkHeader.ChunkType.COLUMN).setLength(chunkMsg.getSerializedSize()).build();
                writeDelimitedProtobuf(outputStream, chunkHeader);
                outputStream.write(chunkMsg.toByteArray());
            }

            long totalLength = file.getFilePointer();

            DatasetProperties datasetProperties = DatasetProperties.newBuilder()
                    .setDatasetTimestamp(this.datasetTimestamp.getTime())
                    .setCreationTimestamp(this.creationTimestamp.getTime())
                    .build();

            DatasetProfileHeader header = DatasetProfileHeader.newBuilder()
                    .setProperties(datasetProperties)
                    .setLength(totalLength)
                    .putAllColumnOffsets(columnChunkOffsets)
                    .putAllIndexedMetricPaths(indexToMetricName)
                    .build();

            DatasetSegmentHeader segmentHeader = DatasetSegmentHeader.newBuilder()
                    .setHasSegments(false)
                    .build();

            try (RandomAccessFile outFile = new RandomAccessFile(pathName, "rw")) {
                file.seek(0);
                InputStream inputFromTemp = Channels.newInputStream(file.getChannel());
                OutputStream writeToFile = Channels.newOutputStream(outFile.getChannel());
                outFile.write(WhylogsMagicUtility.WHYLOGS_MAGIC_HEADER_BYTES);
                writeDelimitedProtobuf(writeToFile, segmentHeader);
                writeDelimitedProtobuf(writeToFile, header);

                int bufferSize = 1024;
                int bytesRead = 0;
                while (file.getFilePointer() < totalLength) {
                    byte[] buffer = new byte[bufferSize];
                    bytesRead = inputFromTemp.read(buffer, bytesRead, bytesRead+ bufferSize);
                    writeToFile.write(buffer, 0, bytesRead); // TODO: this offset doesn't seem write. Test
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                Files.deleteIfExists(new File(tempPath).toPath());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static DatasetProfileView read(String path) throws FileNotFoundException {
        try (RandomAccessFile file = new RandomAccessFile(path, "r")) {
            byte[] buffer = new byte[WhylogsMagicUtility.WHYLOGS_MAGIC_HEADER_LENGTH];
            file.read(buffer);

            String decodedHeader;
            try{
                decodedHeader = new String(buffer, "UTF-8");
            } catch(Exception e){
                throw new DeserializationError("Invalid magic header. Decoder error: " + e.getMessage());
            }

            if (!WhylogsMagicUtility.WHYLOGS_MAGIC_HEADER.equals(decodedHeader)) {
                throw new DeserializationError("Invalid magic header. Expected: " + WhylogsMagicUtility.WHYLOGS_MAGIC_HEADER + " Got: " + decodedHeader);
            }

            InputStream inputStream = Channels.newInputStream(file.getChannel());
            DatasetSegmentHeader segmentHeader = DatasetSegmentHeader.parseDelimitedFrom(inputStream);
            if(segmentHeader.getHasSegments()){
                throw new DeserializationError("Dataset profile has segments. This is not supported yet.");
            }

            DatasetProfileHeader header = DatasetProfileHeader.parseDelimitedFrom(inputStream);
            if(header.getSerializedSize() == 0){
                throw new DeserializationError("Missing valid dataset profile header");
            }

            Date datasetTimestamp = new Date(header.getProperties().getDatasetTimestamp());
            Date creationTimestamp = new Date(header.getProperties().getCreationTimestamp());
            Map<Integer,String> indexedMetricPath = header.getIndexedMetricPathsMap();

            // TODO; Log warning if it's less than 1 "Name index in the header is empty. Possible data corruption"
            long startOffset = file.getFilePointer();

            HashMap<String, ColumnProfileView> columns = new HashMap<>();
            ArrayList<String> sortedColNames = new ArrayList<>(header.getColumnOffsetsMap().keySet());
            sortedColNames.sort(Comparator.naturalOrder());
            for(String colName:  sortedColNames){

            }

        } catch (IOException | DeserializationError e) {
            e.printStackTrace();
        }
    }
}