package com.esri.hadoop.hive;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.log4j.Logger;

import com.esri.core.geometry.OperatorContains;
import com.esri.core.geometry.OperatorSimpleRelation;
import com.esri.core.geometry.Geometry.GeometryAccelerationDegree;
import com.esri.core.geometry.ogc.OGCGeometry;
import org.apache.hadoop.hive.ql.exec.UDF;
/**
 * Abstract class that all simple relational tests (contains, touches, ...) extend from
 *
 */
public abstract class ST_GeometryRelational extends UDF {
	private static Logger LOG = Logger.getLogger(ST_GeometryRelational.class);
	
	private static final int NUM_ARGS = 2;
	private static final int GEOM_1 = 0;
	private static final int GEOM_2 = 1;
	
	private transient HiveGeometryOIHelper geomHelper1;
	private transient HiveGeometryOIHelper geomHelper2;
	
	private transient OperatorSimpleRelation opSimpleRelation;
	private transient boolean firstRun = true;
	
	private transient boolean geom1IsAccelerated = false;

	/**
	 * Operators that extend this should return an instance of
	 * <code>OperatorSimpleRelation</code>
	 * 
	 * @return operator for simple relationship tests
	 */
	protected abstract OperatorSimpleRelation getRelationOperator();
	

	public ObjectInspector initialize(ObjectInspector[] OIs)
			throws UDFArgumentException {

		opSimpleRelation = getRelationOperator();
		
		if (OIs.length != NUM_ARGS) {
			throw new UDFArgumentException("The " + opSimpleRelation.getType().toString().toLowerCase() + " relationship operator takes exactly two arguments");
		}

		geomHelper1 = HiveGeometryOIHelper.create(OIs[GEOM_1], GEOM_1);
		geomHelper2 = HiveGeometryOIHelper.create(OIs[GEOM_2], GEOM_2);
		
		if (LOG.isDebugEnabled()) {
			LOG.debug("OI[0]=" + geomHelper1);
			LOG.debug("OI[1]=" + geomHelper2);
		}

		firstRun = true;
		geom1IsAccelerated = false;
		
		return PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
	}
	




	public void close() {
		if (geom1IsAccelerated && geomHelper1 != null && geomHelper1.getConstantGeometry() != null) {
			OperatorContains.deaccelerateGeometry(geomHelper1.getConstantGeometry().getEsriGeometry());
		}
	}
}
