package com.esri.hadoop.hive;

/**
 * Created by SuchinskyMSI on 2020/5/20.
 */
public class ZCPoint {
    double lng;
    double lat;

    public ZCPoint(double lng, double lat) {
        this.lng = lng;
        this.lat = lat;
        System.out.print("(" + lng + "," + lat + ") ");
    }

    public static ZCPoint instance(double lng, double lat) {
        return new ZCPoint(lng, lat);
    }
}
