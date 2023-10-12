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

import java.util.ArrayList;
import java.util.List;

public class MilvusUtil {
    private static final String MILVUS_COLLECTION = "collection";
    private static final Integer SEARCH_K = 10;
    private static MilvusServiceClient milvusClient;


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

    public static void insertData(float[] vector) {

        List<Field> fields = new ArrayList<>();
        fields.add(new InsertParam.Field("vector", floatToList(vector)));
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
                .withDimension(128)
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
}