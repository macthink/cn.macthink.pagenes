package cn.macthink.pagenes;

import org.apache.mahout.common.distance.CosineDistanceMeasure;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.junit.Test;

public class CosineDistanceMeasureTest {

	@Test
	public void test() {

		CosineDistanceMeasure measure = new CosineDistanceMeasure();

		double[] d1 = { 1, 2, 3, 4, 5, 6 };
		Vector v1 = new DenseVector(d1);

		double[] d2 = { 4, 2, 1, 9, 4, 10 };
		Vector v2 = new DenseVector(d2);
		System.out.println(measure.distance(v1, v2));

		double[] d3 = { 1, 2, 3, 4, 5, 6 };
		Vector v3 = new DenseVector(d3);
		System.out.println(measure.distance(v1, v3));

		double[] d4 = { -1, -2, -3, -4, -5, -6 };
		Vector v4 = new DenseVector(d4);
		System.out.println(measure.distance(v1, v4));

		double[] d5 = { -1, 2, -3, -4, -5, 6 };
		Vector v5 = new DenseVector(d5);
		System.out.println(measure.distance(v1, v5));

	}
}
