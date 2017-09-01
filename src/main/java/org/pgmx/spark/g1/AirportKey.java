package org.pgmx.spark.g1;

import java.io.Serializable;

public class AirportKey implements Comparable<AirportKey>, Serializable {
    private String airportCode;
    private Integer flightCount;


    public String getAirportCode() {
        return airportCode;
    }

    public Integer getFlightCount() {
        return flightCount;
    }

    public AirportKey(String airportCode, Integer flightCount) {
        this.airportCode = airportCode;
        this.flightCount = flightCount;
    }

    @Override
    public int compareTo(AirportKey o) {
        return this.flightCount.compareTo(o.getFlightCount()); // reverse sort
    }

    @Override
    public String toString() {
        return airportCode + ", " + flightCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AirportKey)) return false;

        AirportKey that = (AirportKey) o;

        return getAirportCode().equals(that.getAirportCode());
    }

    @Override
    public int hashCode() {
        return getAirportCode().hashCode();
    }
}
