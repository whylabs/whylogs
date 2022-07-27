package com.whylogs.core.metics;

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
        double delta = first.getMean() - second.getMean();
        double m2 = first.getM2() + second.getM2() + Math.pow(delta, 2) * first.getN() * second.getN() / n;

        return new VarianceM2Result(n, delta, m2);
    }

    public static VarianceM2Result welfordOnlineVarianceM2(VarianceM2Result previous, double value){
        double n = previous.getN() + 1;
        double delta = value - previous.getMean();
        double mean = previous.getMean() + delta / n;
        double delta2 = value - mean;
        double m2 = previous.getM2() + delta * delta2;

        return new VarianceM2Result(n, delta, m2);
    }
}
