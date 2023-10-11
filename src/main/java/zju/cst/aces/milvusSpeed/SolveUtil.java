package zju.cst.aces.milvusSpeed;

import org.openlca.npy.Array2d;
import org.openlca.npy.Npy;
import org.openlca.npy.NpyFloatArray;

import java.io.File;

import static java.lang.Math.random;

public class SolveUtil {
    public static void buildData(String loc) {
        File file = new File(loc);

        NpyFloatArray npyArray = Npy.read(file).asFloatArray();
        for(int i = 0; i < npyArray.shape()[0]; i++){
            float[] vector = Array2d.getRow(npyArray, i);
            MilvusUtil.insertData(vector);
        }
    }
    public static void testData() {
        float[] vector = new float[768];
        long times = 0;
        for(int cnt = 0; cnt < 1000; cnt++){
            for(int i = 0; i < 768; i++){
                vector[i] = (float) random();
            }
            times += MilvusUtil.search(vector);
        }
        System.out.println("cnt: 100, avg: " + times / 1000.0);
    }
}
