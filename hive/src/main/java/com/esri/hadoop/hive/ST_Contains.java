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

@Description(
		name = "ST_Contains",
		value = "_FUNC_(geometry1, geometry2) - return true if geometry1 contains geometry2",
		extended = "Example:\n" + 
		"SELECT _FUNC_(st_polygon(1,1, 1,4, 4,4, 4,1), st_point(2, 3) from src LIMIT 1;  -- return true\n" + 
		"SELECT _FUNC_(st_polygon(1,1, 1,4, 4,4, 4,1), st_point(8, 8) from src LIMIT 1;  -- return false"	
		)
public class ST_Contains extends UDF {

	private transient HiveGeometryOIHelper geomHelper1;

	protected OperatorSimpleRelation getRelationOperator() {
		return OperatorContains.local();
	}
	

	public String getDisplayString(String[] args) {
		return String.format("returns true if %s contains %s", args[0], args[1]);
	}

	public boolean evaluate(Text SuchPolygon, Text SuchPoint) throws UDFArgumentException  {
		//Polygon poly = new Polygon(SuchPolygon);
		ST_Polygon my=new  ST_Polygon();
		BytesWritable poly =my.evaluate(SuchPolygon);
		ST_Point my2=new ST_Point();
		BytesWritable point =my2.suchGetPoint(SuchPoint);
		OGCGeometry ogcGeometryPoly = GeometryUtils.geometryFromEsriShape(poly);
		OGCGeometry ogcGeometryPoint = GeometryUtils.geometryFromEsriShape(point);
		return geometryContains(ogcGeometryPoly.getEsriGeometry(),ogcGeometryPoint.getEsriGeometry());
	}

	public boolean evaluate(BytesWritable SuchPolygon, BytesWritable SuchPoint) throws UDFArgumentException  {

		OGCGeometry ogcGeometryPoly = GeometryUtils.geometryFromEsriShape(SuchPolygon);
		OGCGeometry ogcGeometryPoint = GeometryUtils.geometryFromEsriShape(SuchPoint);
		return geometryContains(ogcGeometryPoly.getEsriGeometry(),ogcGeometryPoint.getEsriGeometry());
	}

	public boolean evaluate(Text SuchPolygon, Double x, Double y) throws UDFArgumentException  {
		//ST_Polygon my=new  ST_Polygon();
		//BytesWritable poly =my.evaluate(SuchPolygon);
		//OGCGeometry ogcGeometryPoly = GeometryUtils.geometryFromEsriShape(poly);

		MapGeometry mygeo=GeometryEngine.jsonToGeometry(SuchPolygon.toString());


		Point stPt = new Point(x, y);
		//OGCGeometry ogcGeometryPoint = OGCGeometry.createFromEsriGeometry(stPt, null);
		//String wkt = SuchPolygon.toString();
		//OGCGeometry ogcObj = OGCGeometry.fromText(wkt);
		//ogcObj.setSpatialReference(null);
		return geometryContains(mygeo.getGeometry(),stPt);
	}


	private boolean geometryContains(Geometry geometryA, Geometry geometryB)
	{
		//boolean contains = OperatorContains.local().execute(geometryA, geometryB, null, null);
		boolean contains =GeometryEngine.contains(geometryA,geometryB,null);

		return contains;
	}
}

