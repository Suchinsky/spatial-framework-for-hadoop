package com.esri.hadoop.hive;

import static org.junit.Assert.*;
import org.junit.Test;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;

// select ST_GeometryType(ST_Point(0, 0)) from onerow;
// select ST_GeometryType(ST_Point('point (10.02 20.01)')) from onerow;
// select ST_GeometryType(ST_Point('point z (10.02 20.01 2)')) from onerow;

public class TestStPoint {

	@Test
	public void TestStPoint() throws Exception {
		ST_GeometryType typer = new ST_GeometryType();
		ST_X stX = new ST_X();
		ST_Y stY = new ST_Y();
		ST_Point stPt = new ST_Point();
		ST_Polygon myPo=new ST_Polygon();
		BytesWritable bwGeom = stPt.evaluate(new DoubleWritable(1.2),
											 new DoubleWritable(3.4));


		String xx="120.053048^30.851565, 120.052926^30.851528, 120.052926^30.851528, 120.052926^30.851528, 120.053015^30.851549, 120.052926^30.851528, 120.053015^30.851534, 120.052926^30.851528, 120.052926^30.851528, 120.052926^30.851528, 120.052926^30.851528, 120.053015^30.849916, 120.053493^30.849423, 120.055196^30.850135, 120.05682^30.850868, 120.057001^30.849856, 120.05713^30.84661";

		String a ="{\"rings\":[[[121.84554066317729,29.943621026512226],[121.84816460317998,29.942900289522555],[121.84756792774174,29.941137538316227],[121.84497084322214,29.941816484681695],[121.84554066317729,29.943621026512226]]],\"spatialReference\":{\"wkid\":4326}}";

		//String a ="{\"rings\":[[[121.84554066317729,29.943621026512226],[121.84816460317998,29.942900289522555],[121.84756792774174,29.941137538316227],[121.84497084322214,29.941816484681695],[121.84554066317729,29.943621026512226]]]}";

		//String a="polygon ((0 0, 8 0, 0 8, 0 0), (1 1, 1 5, 5 1, 1 1))";
		Text my111=new Text(a);
		BytesWritable my2222=new BytesWritable(a.getBytes());


		//ST_Contains suchTest=new ST_Contains();
		//suchTest.evaluate(my111,121.8460,29.9430);

		//DouglasPeucker suchTest=new DouglasPeucker();
		//String my=suchTest.evaluate(my111,"2");

		RoadDirection suchTest=new RoadDirection();
		Double my=suchTest.evaluate(my111);

		DoubleWritable dwx = stX.evaluate(bwGeom);
		DoubleWritable dwy = stY.evaluate(bwGeom);
		assertEquals(1.2, dwx.get(), .000001);
		assertEquals(3.4, dwy.get(), .000001);
		Text gty = typer.evaluate(bwGeom);
		assertEquals("ST_POINT", gty.toString());
		bwGeom = stPt.evaluate(new DoubleWritable(6.5),
							   new DoubleWritable(4.3),
							   new DoubleWritable(2.1));
		gty = typer.evaluate(bwGeom);
		assertEquals("ST_POINT", gty.toString());
		bwGeom = stPt.evaluate(new Text("point (10.02 20.01)"));
		gty = typer.evaluate(bwGeom);
		assertEquals("ST_POINT", gty.toString());
		bwGeom = stPt.evaluate(new Text("point z (10.02 20.01 2)"));
		gty = typer.evaluate(bwGeom);
		assertEquals("ST_POINT", gty.toString());
	}

}

