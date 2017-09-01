package org.pgmx.spark.g3;

import java.io.Serializable;

public class FlightLegKey implements Serializable, Comparable<FlightLegKey> {
    private String flightLeg;
    private String origin;

    public String getFlightLeg() {
        return flightLeg;
    }

    public void setFlightLeg(String flightLeg) {
        this.flightLeg = flightLeg;
    }

    public String getOrigin() {
        return origin;
    }

    public void setOrigin(String origin) {
        this.origin = origin;
    }

    public String getDest() {
        return dest;
    }

    public void setDest(String dest) {
        this.dest = dest;
    }

    public String getFltDate() {
        return fltDate;
    }

    public void setFltDate(String fltDate) {
        this.fltDate = fltDate;
    }

    public String getAirline() {
        return airline;
    }

    public void setAirline(String airline) {
        this.airline = airline;
    }

    public String getFltNum() {
        return fltNum;
    }

    public void setFltNum(String fltNum) {
        this.fltNum = fltNum;
    }

    public String getDepTime() {
        return depTime;
    }

    public void setDepTime(String depTime) {
        this.depTime = depTime;
    }

    private String dest;
    private String fltDate;
    private String airline;
    private String fltNum;
    private String depTime;
    private Float avg;
    private G3Q2.AvgCount avgCount;

    public void setAvgCount(G3Q2.AvgCount avgCount) {
        this.avgCount = avgCount;
    }

    @Override
    public int compareTo(FlightLegKey that) {
        return this.avgCount.avg().compareTo(that.avgCount.avg());
    }


    public FlightLegKey(String flightLeg, String origin, String dest, String fltDate, String airline,
                        String fltNum, String depTime) {
        this.flightLeg = flightLeg;
        this.origin = origin;
        this.dest = dest;
        this.fltDate = fltDate;
        this.airline = airline;
        this.fltNum = fltNum;
        this.depTime = depTime;
    }

    public Float getAvg() {
        return avgCount.avg();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof FlightLegKey)) return false;

        FlightLegKey that = (FlightLegKey) o;

        if (!origin.equals(that.origin)) return false;
        if (!dest.equals(that.dest)) return false;
        if (!fltDate.equals(that.fltDate)) return false;
        if (!airline.equals(that.airline)) return false;
        return fltNum.equals(that.fltNum);
    }

    @Override
    public int hashCode() {
        int result = origin.hashCode();
        result = 31 * result + dest.hashCode();
        result = 31 * result + fltDate.hashCode();
        result = 31 * result + airline.hashCode();
        result = 31 * result + fltNum.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return flightLeg + "," +
                origin + ',' +
                dest + ',' +
                fltDate + ',' +
                depTime + ',' +
                airline + ',' +
                fltNum + ',' +
                (null == avgCount ? "" : avgCount.avg().toString());
    }
}