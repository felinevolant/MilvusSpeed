package zju.cst.aces.milvusSpeed;

import org.openlca.npy.Array2d;
import org.openlca.npy.Npy;
import org.openlca.npy.NpyFloatArray;

import java.io.File;

public class BuildData {
    public static void buildData(String loc) {
        File file = new File(loc);

        NpyFloatArray npyArray = Npy.read(file).asFloatArray();
        for(int i = 0; i < npyArray.shape()[0]; i++){
            Array2d.getRow(npyArray, i);
        }
        System.out.println(npyArray);
    }
}
