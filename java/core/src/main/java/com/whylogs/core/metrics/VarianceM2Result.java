package com.whylogs.core.metrics;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Getter
public class VarianceM2Result {
    private final double n;
    private final double mean;
    private final double m2;

    public static VarianceM2Result parrallelVarianceM2(VarianceM2Result first, VarianceM2Result second){
        double n = first.getN() + second.getN();
        double delta = second.getMean() - first.getMean();
        double mean = (first.getMean() * first.getN() + second.getMean() * second.getN()) / n;
        double m2 = first.getM2() + second.getM2() + Math.pow(delta, 2) * first.getN() * second.getN() / n;

        return new VarianceM2Result(n, mean, m2);
    }

    public static VarianceM2Result welfordOnlineVarianceM2(VarianceM2Result existing, double new_value){
        double n = existing.getN() + 1;
        double delta = new_value - existing.getMean();
        double mean = existing.getMean() + delta / n;
        double delta2 = new_value - mean;
        double m2 = existing.getM2() + delta * delta2;

        return new VarianceM2Result(n, mean, m2);
    }
}
