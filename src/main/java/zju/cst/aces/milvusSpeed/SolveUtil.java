package zju.cst.aces.milvusSpeed;

import org.openlca.npy.Array2d;
import org.openlca.npy.Npy;
import org.openlca.npy.NpyFloatArray;

import java.io.File;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

public class SolveUtil {
    public static void buildData(String dataPath, String testPath) {
        File file = new File(dataPath);
        int cnt = 0;
        NpyFloatArray npyArray = Npy.read(file).asFloatArray();
        List<List<Float>> data = new ArrayList<>();
        for (int i = 0; i < npyArray.shape()[0]; i++) {
            float[] vector = Array2d.getRow(npyArray, i);
            data.add(floatToList(vector));
            if (data.size() == 10000) {
                cnt++;
                MilvusUtil.insertData(data);
                data = new ArrayList<>();
                switch (cnt) {
                    case 1:
                    case 10:
                    case 100:
                    case 1000:
                    case 10000:
                    case 100000:
                        testData(testPath);
                }
            }
        }

    }

    private static List<Float> floatToList(float[] vector) {
        List<Float> floats = new ArrayList<>();
        for (float v : vector) {
            floats.add(v);
        }
        return floats;
    }

    public static void testData(String loc) {
        File file = new File(loc);
        long times = 0;
        NpyFloatArray npyArray = Npy.read(file).asFloatArray();
        for (int i = 0; i < 100; i++) {
            float[] vector = Array2d.getRow(npyArray, i);
            times += MilvusUtil.search(vector);
        }
        double avgTime = times * 1.0 / 100 / 1000000000;
        System.out.println("num: " + MilvusUtil.getVectorNum() + ", avg: " + new BigDecimal(Double.toString(avgTime)) + "s");
    }
}
