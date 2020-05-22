package com.esri.hadoop.hive;

import com.esri.core.geometry.*;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.hadoop.hive.ST_Point;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.udf.UDFType;

import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;


import com.esri.core.geometry.MultiPath;
import com.esri.core.geometry.MultiPoint;
import com.esri.core.geometry.Polygon;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.ogc.OGCGeometry;

import static org.apache.hadoop.io.retry.RetryUtils.LOG;

/**
 * Created by SuchinskyMSI on 2020/5/22.
 */
@Description(
        name = "RoadDirection",
        value = "_FUNC_(geometry1) - return true if geometry1 contains geometry2",
        extended = "Example:\n" +
                "SELECT _FUNC_(string, st_point(2, 3) from src LIMIT 1;  -- return true\n" +
                "SELECT _FUNC_(string, st_point(8, 8) from src LIMIT 1;  -- return false"
)
public class RoadDirection extends UDF {
    private transient HiveGeometryOIHelper geomHelper1;

    protected OperatorSimpleRelation getRelationOperator() {
        return OperatorContains.local();
    }

    public double evaluate(Text SuchPolygon) {
        double sum_xy = 0;
        double sum_x = 0;
        double sum_y = 0;
        double sum_x2 = 0;
        double k;


        MapGeometry mygeo=GeometryEngine.jsonToGeometry(SuchPolygon.toString());


//        ST_Polygon my111 = new ST_Polygon();
//        BytesWritable poly = null;
//        try {
//            poly = my111.evaluate(SuchPolygon);
//        } catch (UDFArgumentException e) {
//            e.printStackTrace();
//        }
//        //OGCGeometry ogcGeometry = GeometryUtils.geometryFromEsriShape(poly);
//        //Polygon polygon = (Polygon)(ogcGeometry);
//
//        OGCGeometry ogcGeometry = GeometryUtils.geometryFromEsriShape(poly);
//        if (ogcGeometry == null){
//            LogUtils.Log_ArgumentsNull(LOG);
//            return sum_xy;
//        }
//
//        Geometry esriGeom = ogcGeometry.getEsriGeometry();
        Polygon polygon = (Polygon)(mygeo.getGeometry());


        int myPointCounts = polygon.getPointCount();

        for (int i = 0; i < myPointCounts; i++) {
            sum_x += polygon.getPoint(i).getX();
            sum_x2 += polygon.getPoint(i).getX() * polygon.getPoint(i).getX();
            sum_xy += polygon.getPoint(i).getX() * polygon.getPoint(i).getY();
            sum_y += polygon.getPoint(i).getY();
        }
        k = (sum_xy - sum_x * sum_y / myPointCounts) / (sum_x2 - sum_x * sum_x / myPointCounts);

        if (k > 0) {
            return Math.toDegrees(Math.atan(k));
        } else if (Math.abs(k) < 10e-5) {
            return 90.0;
        } else {
            return 180 + Math.toDegrees(Math.atan(k));
        }

    }


}
