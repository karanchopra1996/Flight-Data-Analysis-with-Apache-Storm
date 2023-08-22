package utility;

import java.io.Serializable;

public class FlightInformation implements Serializable {

    private static final long serialVersionUID = 1L;
    private String transponderAddress;
    private String callSign;
    private String originCountry;
    private String startTimestamp;
    private String lastTimestamp;
    private String longitude;
    private String latitude;
    private String altitude;
    private String isSurface;
    private String velocity;
    private String degree;
    private String verticalRate;
    private String altitudeGeometric;
    private String sensors;
    private String transponderCode;
    private String isSpecialPurpose;
    private String origin;
    public FlightInformation(String transponderAddress, String callSign, String originCountry, String startTimestamp, String lastTimestamp, String longitude, String latitude, String altitude, String isSurface, String velocity, String degree, String verticalRate, String altitudeGeometric, String sensors, String transponderCode, String isSpecialPurpose, String origin) {
        this.transponderAddress = transponderAddress;
        this.callSign = callSign;
        this.originCountry = originCountry;
        this.startTimestamp = startTimestamp;
        this.lastTimestamp = lastTimestamp;
        this.longitude = longitude;
        this.latitude = latitude;
        this.altitude = altitude;
        this.isSurface = isSurface;
        this.velocity = velocity;
        this.degree = degree;
        this.verticalRate = verticalRate;
        this.altitudeGeometric = altitudeGeometric;
        this.sensors = sensors;
        this.transponderCode = transponderCode;
        this.isSpecialPurpose = isSpecialPurpose;
        this.origin = origin;
    }

    public String getTransponderAddress() {
        return transponderAddress;
    }
    public void setTransponderAddress(String transponderAddress) {
        this.transponderAddress = transponderAddress;
    }
    public String getCallSign() {
        return callSign;
    }
    public void setCallSign(String callSign) {
        this.callSign = callSign;
    }
    public String getOriginCountry() {
        return originCountry;
    }
    public void setOriginCountry(String originCountry) {
        this.originCountry = originCountry;
    }
    public String getStartTimestamp() {
        return startTimestamp;
    }
    public void setStartTimestamp(String startTimestamp) {
        this.startTimestamp = startTimestamp;
    }
    public String getLastTimestamp() {
        return lastTimestamp;
    }
    public void setLastTimestamp(String lastTimestamp) {
        this.lastTimestamp = lastTimestamp;
    }
    public String getLongitude() {
        return longitude;
    }
    public void setLongitude(String longitude) {
        this.longitude = longitude;
    }
    public String getLatitude() {
        return latitude;
    }
    public void setLatitude(String latitude) {
        this.latitude = latitude;
    }
    public String getAltitude() {
        return altitude;
    }
    public void setAltitude(String altitude) {
        this.altitude = altitude;
    }
    public String getIsSurface() {
        return isSurface;
    }
    public void setIsSurface(String isSurface) {
        this.isSurface = isSurface;
    }
    public String getVelocity() {
        return velocity;
    }
    public void setVelocity(String velocity) {
        this.velocity = velocity;
    }
    public String getDegree() {
        return degree;
    }
    public void setDegree(String degree) {
        this.degree = degree;
    }
    public String getVerticalRate() {
        return verticalRate;
    }
    public void setVerticalRate(String verticalRate) {
        this.verticalRate = verticalRate;
    }
    public String getAltitudeGeometric() {
        return altitudeGeometric;
    }
    public void setAltitudeGeometric(String altitudeGeometric) {
        this.altitudeGeometric = altitudeGeometric;
    }
    public String getSensors() {
        return sensors;
    }
    public void setSensors(String sensors) {
        this.sensors = sensors;
    }
    public String getTransponderCode() {
        return transponderCode;
    }
    public void setTransponderCode(String transponderCode) {
        this.transponderCode = transponderCode;
    }
    public String getIsSpecialPurpose() {
        return isSpecialPurpose;
    }
    public void setIsSpecialPurpose(String isSpecialPurpose) {
        this.isSpecialPurpose = isSpecialPurpose;
    }
    public String getOrigin() {
        return origin;
    }
    public void setOrigin(String origin) {
        this.origin = origin;
    }
}
