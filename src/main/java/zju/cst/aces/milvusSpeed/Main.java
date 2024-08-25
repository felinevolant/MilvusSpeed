package zju.cst.aces.milvusSpeed;

public class Main {
    public static void main(String[] args) {
        if (args.length == 0)
            return;
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
                //java Main generate-random-data 10000
                int numVectors = Integer.parseInt(args[1]);
                //int vectorDimension = Integer.parseInt(args[2]);
                MilvusUtil.generateAndInsertRandomData(numVectors);
                System.out.println("random data generation and insertion over");
                break;
            case "search-data":
                int searchCount = Integer.parseInt(args[1]);
                MilvusUtil.testData(searchCount);
                System.out.println("search data over");
                break;
            /*
            case "search-data":
                //java Main generate-random-data 10000
                //long numVectorsToSearch = Long.parseLong(args[1]);
                int searchCount = Integer.parseInt(args[1]);
                double totalAvgTime = 0;
                for (int i = 0; i < searchCount; i++){
                    totalAvgTime += MilvusUtil.testData();
                }
                double avgTime = totalAvgTime / searchCount;
                System.out.println(searchCount+"次查询时间: " + avgTime + "s");
                System.out.println("search data over");
                break;

             */
        }
    }
}