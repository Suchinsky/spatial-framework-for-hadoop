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

        import java.util.ArrayList;
        import java.util.List;

/**
 * Created by SuchinskyMSI on 2020/5/20.
 */
@Description(
        name = "DouglasPeucker",
        value = "_FUNC_(geometry1, geometry2) - return true if geometry1 contains geometry2",
        extended = "Example:\n" +
                "SELECT _FUNC_(string, st_point(2, 3) from src LIMIT 1;  -- return true\n" +
                "SELECT _FUNC_(string, st_point(8, 8) from src LIMIT 1;  -- return false"
)
public class DouglasPeucker extends UDF {
    private transient HiveGeometryOIHelper geomHelper1;

    protected OperatorSimpleRelation getRelationOperator() {
        return OperatorContains.local();
    }

    public  String  evaluate(Text trajectory_points){
        List<ZCPoint>  points = GetPoint(trajectory_points);
        List<ZCPoint>  result = DoDouglasPeucker(points, 1);
        return GetResString(result);

    }

    public  String  evaluate(Text trajectory_points,String epsilon){
        List<ZCPoint>  points = GetPoint(trajectory_points);
        List<ZCPoint>  result = DoDouglasPeucker(points, Integer.parseInt(epsilon));
        return GetResString(result);

    }




    public List<ZCPoint> DoDouglasPeucker(List<ZCPoint> points, int epsilon) {
        // 找到最大阈值点，即操作（1）
        double maxH = 0;
        int index = 0;
        int end = points.size();
        for (int i = 1; i < end - 1; i++) {
            double h = distToSegment(points.get(i), points.get(0), points.get(end - 1));
            if (h > maxH) {
                maxH = h;
                index = i;
            }
        }

        // 如果存在最大阈值点，就进行递归遍历出所有最大阈值点
        List<ZCPoint> result = new ArrayList<ZCPoint>();
        if (maxH > epsilon) {
            List<ZCPoint> leftPoints = new ArrayList<ZCPoint>();// 左曲线
            List<ZCPoint> rightPoints = new ArrayList<ZCPoint>();// 右曲线
            // 分别提取出左曲线和右曲线的坐标点
            for (int i = 0; i < end; i++) {
                if (i <= index) {
                    leftPoints.add(points.get(i));
                    if (i == index)
                        rightPoints.add(points.get(i));
                } else {
                    rightPoints.add(points.get(i));
                }
            }

            // 分别保存两边遍历的结果
            List<ZCPoint> leftResult = new ArrayList<ZCPoint>();
            List<ZCPoint> rightResult = new ArrayList<ZCPoint>();
            leftResult = DoDouglasPeucker(leftPoints, epsilon);
            rightResult = DoDouglasPeucker(rightPoints, epsilon);

            // 将两边的结果整合
            rightResult.remove(0);//移除重复点
            leftResult.addAll(rightResult);
            result = leftResult;
        } else {// 如果不存在最大阈值点则返回当前遍历的子曲线的起始点
            result.add(points.get(0));
            result.add(points.get(end - 1));
        }
        return result;
    }


    public  List<ZCPoint> GetPoint(Text trajectory_points){
        String tPoints = trajectory_points.toString();
        tPoints=tPoints.replace(" ", "");
        String[] strArr = tPoints.split(",");

        List<ZCPoint> origins = new ArrayList<ZCPoint>();

        for(int i=0; i<strArr.length; ++i){
            String[] strPoints = strArr[i].split("\\^");
            double lng = Double.valueOf(strPoints[0].toString());
            double lat = Double.valueOf(strPoints[1].toString());
            origins.add(ZCPoint.instance(lng, lat));
        }

        return origins;
    }

    public String GetResString(List<ZCPoint> results){

        StringBuilder strRes =  new StringBuilder();

        char seperator = ',';
        for(int i=0; i<results.size(); i++){
            if (i==results.size()-1){
                strRes.append(results.get(i).lng+"^"+ results.get(i).lat);
            }
            else{
                strRes.append(results.get(i).lng+"^"+ results.get(i).lat);
                strRes.append(seperator);

            }
        }
        return strRes.toString();

    }

    public  double distToSegment(ZCPoint p, ZCPoint s, ZCPoint e){
        double AB = distance(s,e);
        double CB = distance(p,e);
        double CA = distance(p,s);

        // Helen Methods
        double c = (CB + CA + AB) / 2;
        double S = Math.sqrt(c * (c - CB) * (c - CA) * (c - AB));

        return 2*S/AB;

    }


    public double distance(ZCPoint p1, ZCPoint p2){
        double lat1 = p1.lat;
        double lng1 = p1.lng;

        double lat2 = p2.lat;
        double lng2 = p2.lng;

        double radLat1 = lat1 * Math.PI / 180.0;
        double radLat2 = lat2 * Math.PI / 180.0;
        double a = radLat1 - radLat2;
        double b = (lng1 * Math.PI / 180.0) - (lng2 * Math.PI / 180.0);
        double s = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(a / 2), 2)
                + Math.cos(radLat1) * Math.cos(radLat2) * Math.pow(Math.sin(b / 2), 2)));
        return s * 6378137.0;

    }


}