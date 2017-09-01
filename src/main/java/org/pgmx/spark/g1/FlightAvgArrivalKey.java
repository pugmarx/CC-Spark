package org.pgmx.spark.g1;

import java.io.Serializable;

public class FlightAvgArrivalKey implements Comparable<FlightAvgArrivalKey>, Serializable {
    private String fltCode;
    private Float avg;


    public String getFltCode() {
        return fltCode;
    }

    public FlightAvgArrivalKey(String airportCode, Float avgArrivalDelay) {
        this.fltCode = airportCode;
        this.avg = avgArrivalDelay;
    }

    @Override
    public int compareTo(FlightAvgArrivalKey o) {
        int cmp = this.avg.compareTo(o.avg);
        if (cmp != 0) {
            return cmp;
        }
        return this.fltCode.compareTo(o.fltCode);
    }

    @Override
    public String toString() {
        return fltCode + "," + avg;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof FlightAvgArrivalKey)) return false;

        FlightAvgArrivalKey that = (FlightAvgArrivalKey) o;

        return getFltCode().equals(that.getFltCode());
    }

    @Override
    public int hashCode() {
        return getFltCode().hashCode();
    }
}