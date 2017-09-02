package org.pgmx.spark.g2;

import java.io.Serializable;

public class OriginDestDepDelayKey implements Comparable<OriginDestDepDelayKey>, Serializable {
    private String origin;

    public String getOrigin() {
        return origin;
    }

    public void setOrigin(String origin) {
        this.origin = origin;
    }

    public Float getAvgDepDelay() {
        return avgDepDelay;
    }

    public void setAvgDepDelay(Float avgDepDelay) {
        this.avgDepDelay = avgDepDelay;
    }

    private String destination;
    private Float avgDepDelay;


    public String getDestination() {
        return destination;
    }

    public OriginDestDepDelayKey(String origin, String destination, Float avgDepDelay) {
        this.origin = origin;
        this.destination = destination;
        this.avgDepDelay = avgDepDelay;
    }

    @Override
    public int compareTo(OriginDestDepDelayKey o) {
        int cmp = this.avgDepDelay.compareTo(o.avgDepDelay);
        if (cmp != 0) {
            return cmp;
        }
        return this.destination.compareTo(o.destination);
    }

    @Override
    public String toString() {
        return origin + "," + destination + "," + avgDepDelay;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof OriginDestDepDelayKey)) return false;

        OriginDestDepDelayKey that = (OriginDestDepDelayKey) o;

        return getDestination().equals(that.getDestination());
    }

    @Override
    public int hashCode() {
        return getDestination().hashCode();
    }
}