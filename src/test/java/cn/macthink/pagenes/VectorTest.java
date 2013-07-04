package cn.macthink.pagenes;

import org.apache.mahout.math.ConstantVector;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.function.Functions;
import org.junit.Test;

public class VectorTest {

	@Test
	public void test() {
		Vector vector1 = new ConstantVector(1, 5);
		Vector vector2 = new ConstantVector(1, 5);
		System.out.println(vector1.plus(vector2));

		Vector vector3 = new DenseVector(5);
		System.out.println(vector3);
		Vector vector4 = new DenseVector(5);

		vector3.assign(1);
		System.out.println(vector3);
		vector3.assign(Functions.COS);
		System.out.println(vector3);

		vector3.assign(5);
		vector3.assign(Functions.DIV, 2);
		System.out.println(vector3);

		vector3.assign(5);
		vector4.assign(10);
		vector4.assign(vector3, Functions.MINUS);
		System.out.println(vector4);

	}

}
