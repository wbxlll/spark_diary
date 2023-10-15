package com.wbx.entity;

public class CallHistory {

    private String impiFrom;

    private String impiTo;

    private long callTime;

    private int callDuration;

    private String impiFromLocation = "HENAN";

    public String getImpiFrom() {
        return impiFrom;
    }

    public void setImpiFrom(String impiFrom) {
        this.impiFrom = impiFrom;
    }

    public String getImpiTo() {
        return impiTo;
    }

    public void setImpiTo(String impiTo) {
        this.impiTo = impiTo;
    }

    public long getCallTime() {
        return callTime;
    }

    public void setCallTime(long callTime) {
        this.callTime = callTime;
    }

    public int getCallDuration() {
        return callDuration;
    }

    public void setCallDuration(int callDuration) {
        this.callDuration = callDuration;
    }

    public String getImpiFromLocation() {
        return impiFromLocation;
    }

    public void setImpiFromLocation(String impiFromLocation) {
        this.impiFromLocation = impiFromLocation;
    }
}
