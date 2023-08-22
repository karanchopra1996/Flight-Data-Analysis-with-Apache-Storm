package utility;

import java.io.Serializable;

public class AirportInformation implements Serializable {
    private static final long serialVersionUID = 1L;
    private String airportCity;
    private String airportCode;
    private double latitude;
    private double longitude;

    public AirportInformation(String airportCity, String airportCode, double latitude, double longitude) {
        this.airportCity = airportCity;
        this.airportCode = airportCode;
        this.latitude = latitude;
        this.longitude = longitude;
    }

    public AirportInformation() {
    }

    public String getAirportCity() {
        return airportCity;
    }

    public void setAirportCity(String airportCity) {
        this.airportCity = airportCity;
    }

    public String getAirportCode() {
        return airportCode;
    }

    public void setAirportCode(String airportCode) {
        this.airportCode = airportCode;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    @Override
    public String toString() {
        return "AirportInformation{" +
                "airportCity='" + airportCity + '\'' +
                ", airportCode='" + airportCode + '\'' +
                ", latitude=" + latitude +
                ", longitude=" + longitude +
                '}';
    }
}
