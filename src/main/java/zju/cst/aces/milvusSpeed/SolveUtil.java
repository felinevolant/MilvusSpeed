package zju.cst.aces.milvusSpeed;

import org.openlca.npy.Array2d;
import org.openlca.npy.Npy;
import org.openlca.npy.NpyFloatArray;

import java.io.File;

public class SolveUtil {
    public static void buildData(String loc) {
        File file = new File(loc);

        NpyFloatArray npyArray = Npy.read(file).asFloatArray();
        for (int i = 0; i < npyArray.shape()[0]; i++) {
            float[] vector = Array2d.getRow(npyArray, i);
            MilvusUtil.insertData(vector);
        }
    }

    public static void testData(String loc) {
        File file = new File(loc);
        long times = 0;
        NpyFloatArray npyArray = Npy.read(file).asFloatArray();
        for (int i = 0; i < npyArray.shape()[0]; i++) {
            float[] vector = Array2d.getRow(npyArray, i);
            times += MilvusUtil.search(vector);
        }
        System.out.println("cnt: " + npyArray.shape()[0] + ", avg: " + times * 1.0 / npyArray.shape()[0]);
    }
}
