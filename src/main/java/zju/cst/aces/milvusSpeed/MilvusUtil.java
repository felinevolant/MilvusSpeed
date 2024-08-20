package zju.cst.aces.milvusSpeed;

import io.milvus.client.MilvusServiceClient;
import io.milvus.common.clientenum.ConsistencyLevelEnum;
import io.milvus.grpc.DataType;
import io.milvus.grpc.GetCollectionStatisticsResponse;
import io.milvus.param.ConnectParam;
import io.milvus.param.IndexType;
import io.milvus.param.MetricType;
import io.milvus.param.R;
import io.milvus.param.collection.*;
import io.milvus.param.dml.InsertParam;
import io.milvus.param.dml.InsertParam.Field;
import io.milvus.param.dml.SearchParam;
import io.milvus.param.index.CreateIndexParam;
import io.milvus.response.GetCollStatResponseWrapper;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.*;
import java.util.logging.Formatter;

public class MilvusUtil {
    private static final String MILVUS_COLLECTION = "collection";
    private static final Integer SEARCH_K = 10;
    private static MilvusServiceClient milvusClient;
    private static final int BATCH_SIZE = 50000;
    private static final int THREAD_COUNT = 10;
    private static final long[] MILESTONES = {10000, 100000, 1000000, 10000000, 100000000, 1000000000};
    private static final Map<Long, Long> milestoneTimes = new HashMap<>();
    private static final Logger logger = Logger.getLogger(MilvusUtil.class.getName());
    private static List<List<Float>> insertedData = new ArrayList<>(); //全局变量 insertedData 来存储插入的数据

    static {
        try {
            FileHandler fileHandler = new FileHandler("application.log", true);
            fileHandler.setFormatter(new CustomFormatter());
            logger.addHandler(fileHandler);
        } catch (IOException e) {
            logger.severe("Failed to initialize log file handler: " + e.getMessage());
        }
    }

    private static class CustomFormatter extends Formatter {
        private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        @Override
        public String format(LogRecord record) {
            StringBuilder builder = new StringBuilder();
            builder.append(dateFormat.format(new Date(record.getMillis())))
                    .append(" ")
                    .append(record.getLevel())
                    .append(": ")
                    .append(formatMessage(record))
                    .append(System.lineSeparator());
            return builder.toString();
        }
    }

    static {
        milvusClient = new MilvusServiceClient(
                ConnectParam.newBuilder()
                        .withHost("localhost")
                        .withPort(19530)
                        .build()
        );
        R<Boolean> respHasCollection;
        respHasCollection = milvusClient.hasCollection(
                HasCollectionParam.newBuilder()
                        .withCollectionName(MILVUS_COLLECTION)
                        .build()
        );
        if (respHasCollection.getData() == Boolean.TRUE) {
            milvusClient.loadCollection(
                    LoadCollectionParam.newBuilder()
                            .withCollectionName(MILVUS_COLLECTION)
                            .build()
            );
        } else {
            initCollection();
        }

    }

    public static boolean checkDisconnect() {
        return milvusClient.showCollections(ShowCollectionsParam.newBuilder().build()).getStatus() != 0;
    }


    public static long getVectorNum() {
        R<GetCollectionStatisticsResponse> respCollectionStatistics = milvusClient.getCollectionStatistics(
                // Return the statistics information of the collection.
                GetCollectionStatisticsParam.newBuilder()
                        .withCollectionName(MILVUS_COLLECTION)
                        .build()
        );
        GetCollStatResponseWrapper wrapperCollectionStatistics = new GetCollStatResponseWrapper(respCollectionStatistics.getData());
        return wrapperCollectionStatistics.getRowCount();
    }

    public static void rebuild() {
        if (checkDisconnect()) {
            initConnect();
        }
        milvusClient.dropCollection(
                DropCollectionParam.newBuilder()
                        .withCollectionName(MILVUS_COLLECTION)
                        .build()
        );
        initCollection();

    }


    private static void initConnect() {
        milvusClient = new MilvusServiceClient(ConnectParam.newBuilder()
                .withHost("localhost")
                .withPort(19530)
                .build());
        if (milvusClient.showCollections(ShowCollectionsParam.newBuilder().build()).getStatus() != 0) {
            System.out.println("Milvus Connect Error!");
        }
    }


    private static void insert(List<Field> fields) {
        if (checkDisconnect()) {
            initConnect();
        }
        InsertParam insertParam;
        insertParam = InsertParam.newBuilder()
                .withCollectionName(MilvusUtil.MILVUS_COLLECTION)
                .withFields(fields)
                .build();
        milvusClient.insert(insertParam);
    }

    public static void flushData() {
        if (checkDisconnect()) {
            initConnect();
        }

        milvusClient.flush(FlushParam.newBuilder()
                .addCollectionName(MILVUS_COLLECTION) // 添加要刷新的集合名称
                .withSyncFlush(true) // 启用同步刷新模式
                .build());
    }

    public static void insertData(List<List<Float>> data) {
        insertedData.addAll(data); // 这里将数据添加到全局变量 insertedData 中
        List<Field> fields = new ArrayList<>();
        fields.add(new InsertParam.Field("vector", data));
        insert(fields);
    }


    private static void initCollection() {
        FieldType codeID = FieldType.newBuilder()
                .withName("codeID")
                .withDataType(DataType.Int64)
                .withPrimaryKey(true)
                .withAutoID(true)
                .build();
        FieldType vector = FieldType.newBuilder()
                .withName("vector")
                .withDataType(DataType.FloatVector)
                .withDimension(2)
                .build();

        CreateCollectionParam createCollectionReq = CreateCollectionParam.newBuilder()
                .withCollectionName(MILVUS_COLLECTION)
                .withDescription("test")
                .withShardsNum(2)
                .addFieldType(codeID)
                .addFieldType(vector)
                .build();
        init(createCollectionReq);
    }

    private static void init(CreateCollectionParam createCollectionReq) {
        String collectionName = createCollectionReq.getCollectionName();
        milvusClient.createCollection(createCollectionReq);
        final IndexType INDEX_TYPE = IndexType.IVF_SQ8;
        final String INDEX_PARAM = "{\"nlist\":16384}";
        milvusClient.createIndex(
                CreateIndexParam.newBuilder()
                        .withCollectionName(collectionName)
                        .withFieldName("vector")
                        .withIndexType(INDEX_TYPE)
                        .withMetricType(MetricType.L2)
                        .withExtraParam(INDEX_PARAM)
                        .withSyncMode(Boolean.FALSE)
                        .build()
        );
        milvusClient.loadCollection(
                LoadCollectionParam.newBuilder()
                        .withCollectionName(collectionName)
                        .build()
        );
    }

    private static List<List<Float>> floatToList(float[] vector) {
        List<List<Float>> vectorArray = new ArrayList<>();
        List<Float> floats = new ArrayList<>();
        for (float v : vector) {
            floats.add(v);
        }
        vectorArray.add(floats);
        return vectorArray;
    }

    public static long search(float[] vector) {
        final String SEARCH_PARAM = "{\"nprobe\":10}";
        SearchParam searchParam;
        List<String> search_output_fields = List.of("codeID");
        searchParam = SearchParam.newBuilder()
                .withCollectionName(MILVUS_COLLECTION)
                .withConsistencyLevel(ConsistencyLevelEnum.EVENTUALLY)
                .withMetricType(MetricType.L2)
                .withOutFields(search_output_fields)
                .withTopK(SEARCH_K)
                .withVectors(floatToList(vector))
                .withVectorFieldName("vector")
                .withParams(SEARCH_PARAM)
                .build();
        long startTime = System.nanoTime();
        milvusClient.search(searchParam);
        long endTime = System.nanoTime();
        return endTime - startTime;
    }
    // 简单的for循环方法
    public static void generateAndInsertRandomData1(int numVectors, int vectorDimension) {
        Random random = new Random();
        List<List<Float>> data = new ArrayList<>();
        for (int i = 0; i < numVectors; i++) {
            List<Float> vector = new ArrayList<>();
            for(int j = 0; j < vectorDimension; j++) {
                vector.add(random.nextFloat());
            }
            data.add(vector);
            if (data.size() == 50000) {
                insertData(data);
                data.clear();
            }
        }
        if (!data.isEmpty()) {
            insertData(data);
        }
    }
    //使用线程池加速
    public static void generateAndInsertRandomData(int numVectors, int vectorDimension) {
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        int batches = (numVectors + BATCH_SIZE - 1) / BATCH_SIZE;
        long totalInserted = 0;

        for (int i = 0; i < batches; i++) {
            int start = i * BATCH_SIZE;
            int end = Math.min(start + BATCH_SIZE, numVectors);
            executor.submit(() -> {
                List<List<Float>> data = new ArrayList<>();
                Random random = new Random();
                for (int j = start; j < end; j++) {
                    List<Float> vector = new ArrayList<>();
                    for (int k = 0; k < vectorDimension; k++) {
                        vector.add(random.nextFloat());
                    }
                    data.add(vector);
                }
                insertData(data);
            });

            totalInserted += (end - start);
            checkAndLogMilestones(totalInserted);
        }

        executor.shutdown();
        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void checkAndLogMilestones(long totalInserted) {
        for (long milestone : MILESTONES) {
            if (totalInserted >= milestone && !milestoneTimes.containsKey(milestone)) {
                // 从插入的数据中随机选择一个向量进行搜索
                float[] vector = getRandomVector();
                long searchTime = search(vector);
                milestoneTimes.put(milestone, searchTime);
                logger.info("Milestone " + milestone + " reached. Search time: " + searchTime + " ns");
            }
        }
    }

    private static float[] getRandomVector() {
        Random random = new Random();
        int randomIndex = random.nextInt(insertedData.size());
        List<Float> randomVector = insertedData.get(randomIndex);
        float[] vector = new float[randomVector.size()];
        for (int i = 0; i < randomVector.size(); i++) {
            vector[i] = randomVector.get(i);
        }
        return vector;
    }

}