package zju.cst.aces.milvusSpeed;

public class Main {
    public static void main(String[] args) {
        if (args.length == 0)
            return;
        int vectorDimension= 2;
        switch (args[0]) {
            case "insert-data":
                SolveUtil.buildData(args[1], args[2]);
                System.out.println("insert over");
                break;
            case "rebuild-data":
                MilvusUtil.rebuild();
                System.out.println("rebuild over");
                break;
            case "get-data-num":
                System.out.println(MilvusUtil.getVectorNum());
                break;
            case "flush-data":
                MilvusUtil.flushData();
                System.out.println("flush over");
                break;
            case "generate-random-data":
                int numVectors = Integer.parseInt(args[1]);
                vectorDimension = Integer.parseInt(args[2]);
                MilvusUtil.generateAndInsertRandomData(numVectors,vectorDimension);
                System.out.println("random data generation and insertion over");
                break;
            case "search-data":
                int searchCount = Integer.parseInt(args[1]);
                vectorDimension = Integer.parseInt(args[2]);
                MilvusUtil.testData(searchCount,vectorDimension);
                System.out.println("search data over");
                break;
        }
    }
}