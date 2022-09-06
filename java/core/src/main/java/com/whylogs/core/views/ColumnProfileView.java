package com.whylogs.core.views;

import com.google.protobuf.InvalidProtocolBufferException;
import com.whylogs.core.SummaryConfig;
import com.whylogs.core.errors.UnsupportedError;
import com.whylogs.core.metrics.Metric;
import lombok.Getter;
import whylogs.core.message.ColumnMessage;
import whylogs.core.message.MetricComponentMessage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Optional;

@Getter
public class ColumnProfileView{
    private HashMap<String, Metric> metrics;
    private int successes = 0;
    private int failures = 0;

    public ColumnProfileView(HashMap<String, Metric> metrics) {
        this.metrics = metrics;
    }

    public ColumnProfileView(HashMap<String, Metric> metrics, int successes, int failures) {
        this.metrics = metrics;
        this.successes = successes;
        this.failures = failures;
    }

    // TODO: this needs the Metric Merge fixed
    public ColumnProfileView merge(ColumnProfileView otherView){
        HashSet<String> allMetricNames = new HashSet<>();
        allMetricNames.addAll(this.metrics.keySet());
        allMetricNames.addAll(otherView.metrics.keySet());

        HashMap<String, Metric> mergedMetrics = new HashMap<>();
        for(String metricName : allMetricNames){
            Metric thisMetric = this.metrics.get(metricName);
            Metric otherMetric = otherView.metrics.get(metricName);

            Metric result = thisMetric;

            if(thisMetric != null && otherMetric != null){
                result = thisMetric.merge(otherMetric);
            } else if (otherMetric != null){
                result = otherMetric;
            }

            mergedMetrics.put(metricName, result);
        }

        return new ColumnProfileView(mergedMetrics,
                this.successes + otherView.successes,
                this.failures + otherView.failures);
    }

    public byte[] serialize(){
        return this.toProtobuf().toByteArray();
    }

    public static ColumnProfileView deserialize(byte[] data) throws InvalidProtocolBufferException {
        ColumnMessage columnMessage = ColumnMessage.parseFrom(data);
        return ColumnProfileView.fromProtobuf(columnMessage);
    }

    public Optional<Metric> getMetric(String metricName){
        return Optional.ofNullable(this.metrics.get(metricName));
    }

    // TODO: needs to have getComponents added to Metric
    public ColumnMessage toProtobuf(){
        HashMap<String, MetricComponentMessage> metricMessages = new HashMap<>();
        for(String metricName : this.metrics.keySet()){
            for(String componentName : this.metrics.get(metricName).getComponents().keySet()){
                MetricComponentMessage componentMessage = this.metrics.get(metricName).getComponents().get(componentName).toProtobuf();
                metricMessages.put(metricName + "/" + componentName, componentMessage);
            }
        }
        return ColumnMessage.newBuilder().putAllMetricComponents(metricMessages).build();
    }

    public static ColumnProfileView zero(){
        return new ColumnProfileView(new HashMap<>());
    }

    public static ColumnProfileView fromProtobuf(ColumnMessage columnMessage){
        HashMap<String, Metric> resultMetrics = new HashMap<>();
        HashMap<String, HashMap<String, MetricComponentMessage>> metricMessages = new HashMap<>();

        for(String path : columnMessage.getMetricComponentsMap().keySet()){
            String metricName = path.split("/")[0];
            HashMap<String, MetricComponentMessage> metricComponents = new HashMap<>();

            if(metricMessages.containsKey(metricName)){
                metricComponents = metricMessages.get(metricName);
                metricMessages.put(metricName, metricComponents);
            } else {
                metricMessages.put(metricName, new HashMap<String, MetricComponentMessage>());
            }

            // TODO: get the path from the first / on
            String componentKey = path.substring(path.indexOf("/") + 1);
            metricComponents.put(componentKey, columnMessage.getMetricComponentsMap().get(path));
        }

        // TODO: turn metric into type
            // was from StandardMetric
            // then Registry
            // then Metric.fromProtobuf

        return new ColumnProfileView(resultMetrics);
    }

    public static ColumnProfileView fromBytes(byte[] data) throws InvalidProtocolBufferException {
        ColumnMessage message = ColumnMessage.parseFrom(data);
        return ColumnProfileView.fromProtobuf(message);
    }

    // TODO: metric needs a getComponentPath
    public ArrayList<String> getMetricComponentPaths(){
        ArrayList<String> paths = new ArrayList<>();
        for(String metricName : this.getMetricNames()){
            for(String componentName :this.getMetric(metricName).getCompnentPaths()){
                paths.add(metricName + "/" + componentName);
            }
        }
        return paths;
    }

    public ArrayList<String> getMetricNames(){
        return new ArrayList<>(this.getMetrics().keySet());
    }

    public HashMap<String, Object> toSummaryDict(Optional<String> columnMetric, Optional<SummaryConfig> config) throws UnsupportedError {
        SummaryConfig summaryConfig = config.orElse(new SummaryConfig());
        HashMap<String, Object> summary = new HashMap<>();

        if(columnMetric.isPresent()){
            summary.putAll(getMetricSummaryHelper(summaryConfig, this.getMetric(columnMetric.get())));
        } else {
            for(String metricName : this.getMetricNames()){
                summary.putAll(getMetricSummaryHelper(summaryConfig, this.getMetric(metricName)));
            }
        }

        // TODO: there was a logger for when a ssummary couldn't be implmented for a metric

        if(columnMetric.isPresent() && columnMetric.get().length() == 0){
            throw new UnsupportedError("No metric available for requested column metric: " + columnMetric.get());
        }

        return summary;
    }

    private HashMap<String, Object> getMetricSummaryHelper(SummaryConfig summaryConfig,
                                                           Optional<Metric> maybeMetric) {
        HashMap<String, Object> result = new HashMap<>();
        Metric metric;
        if(maybeMetric.isPresent()){
            metric = maybeMetric.get();
            HashMap<String, Object> metricSummary = metric.toSummaryDict(summaryConfig);
            for (String componentName : metricSummary.keySet()) {
                String fullName = metric.getNamespace() + "/" + componentName;
                result.put(fullName, metricSummary.get(componentName));
            }
        }
        return result;
    }
}
