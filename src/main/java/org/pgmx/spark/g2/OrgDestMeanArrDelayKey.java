package org.pgmx.spark.g2;

import java.io.Serializable;

public class OrgDestMeanArrDelayKey implements Comparable<OrgDestMeanArrDelayKey>, Serializable {
    private String origin;
    private String destination;
    private Float avgArrivalDelay;


    public String getDestination() {
        return destination;
    }

    public OrgDestMeanArrDelayKey(String origin, String destination, Float avgArrDelay) {
        this.origin = origin;
        this.destination = destination;
        this.avgArrivalDelay = avgArrDelay;
    }

    @Override
    public int compareTo(OrgDestMeanArrDelayKey o) {
        return this.avgArrivalDelay.compareTo(o.avgArrivalDelay);
    }

    @Override
    public String toString() {
        return origin + "," + destination + "," + avgArrivalDelay;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof OrgDestMeanArrDelayKey)) return false;

        OrgDestMeanArrDelayKey that = (OrgDestMeanArrDelayKey) o;

        return getDestination().equals(that.getDestination());
    }

    @Override
    public int hashCode() {
        return getDestination().hashCode();
    }
}