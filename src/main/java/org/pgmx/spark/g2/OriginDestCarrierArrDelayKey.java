package org.pgmx.spark.g2;

import java.io.Serializable;

public class OriginDestCarrierArrDelayKey implements Comparable<OriginDestCarrierArrDelayKey>, Serializable {
    private String origin;
    private String destination;
    private String airline;
    private Float avgArrivalDelay;


    public String getDestination() {
        return destination;
    }

    public OriginDestCarrierArrDelayKey(String origin, String destination, String airline, Float avgArrDelay) {
        this.origin = origin;
        this.destination = destination;
        this.airline = airline;
        this.avgArrivalDelay = avgArrDelay;
    }

    @Override
    public int compareTo(OriginDestCarrierArrDelayKey o) {
        return this.avgArrivalDelay.compareTo(o.avgArrivalDelay);
    }

    @Override
    public String toString() {
        return origin + "," + destination + "," + airline + "," + avgArrivalDelay;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof OriginDestCarrierArrDelayKey)) return false;
        OriginDestCarrierArrDelayKey that = (OriginDestCarrierArrDelayKey) o;
        return getDestination().equals(that.getDestination());
    }

    @Override
    public int hashCode() {
        return getDestination().hashCode();
    }
}