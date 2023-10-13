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
        }
    }
}