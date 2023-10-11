package zju.cst.aces.simDetector.common.util;

import com.alibaba.fastjson2.JSONArray;
import io.milvus.client.MilvusServiceClient;
import io.milvus.common.clientenum.ConsistencyLevelEnum;
import io.milvus.grpc.DataType;
import io.milvus.grpc.FieldData;
import io.milvus.grpc.GetCollectionStatisticsResponse;
import io.milvus.grpc.SearchResults;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zju.cst.aces.simDetector.common.config.PropertiesConfig;
import zju.cst.aces.simDetector.dto.milvus.CVEFunction;
import zju.cst.aces.simDetector.dto.milvus.CodeFragment;
import zju.cst.aces.simDetector.dto.milvus.MilvusCVEFunctionResult;
import zju.cst.aces.simDetector.dto.milvus.MilvusCodeFragmentResult;

import java.nio.ByteBuffer;
import java.util.*;

public class MilvusUtil {
    private static final Logger logger = LogManager.getLogger("rootLogger");
    private static final String MILVUS_COL_CODE_FRAGMENT = "CodeFragment";
    private static final String MILVUS_COL_CVE_FRAGMENT = "CVEFunction";
    private static final int SEARCH_K = 10;
    private static MilvusServiceClient milvusClient;


    static {
        String host = PropertiesConfig.getHost();
        milvusClient = new MilvusServiceClient(
                ConnectParam.newBuilder()
                        .withHost(host)
                        .withPort(19530)
                        .build()
        );
        R<Boolean> respHasCollection;
        respHasCollection = milvusClient.hasCollection(
                HasCollectionParam.newBuilder()
                        .withCollectionName(MILVUS_COL_CVE_FRAGMENT)
                        .build()
        );
        if (respHasCollection.getData() == Boolean.TRUE) {
            milvusClient.loadCollection(
                    LoadCollectionParam.newBuilder()
                            .withCollectionName(MILVUS_COL_CVE_FRAGMENT)
                            .build()
            );
        } else {
            initCVEFunctionCollection();
        }

        respHasCollection = milvusClient.hasCollection(
                HasCollectionParam.newBuilder()
                        .withCollectionName(MILVUS_COL_CODE_FRAGMENT)
                        .build()
        );
        if (respHasCollection.getData() == Boolean.TRUE) {
            milvusClient.loadCollection(
                    LoadCollectionParam.newBuilder()
                            .withCollectionName(MILVUS_COL_CODE_FRAGMENT)
                            .build()
            );
        } else {
            initCodeFragmentCollection();
        }

    }

    public static boolean checkDisconnect() {
        return milvusClient.showCollections(ShowCollectionsParam.newBuilder().build()).getStatus() != 0;
    }

    public static boolean checkCollection() {
        return milvusClient.hasCollection(HasCollectionParam.newBuilder()
                .withCollectionName(MILVUS_COL_CODE_FRAGMENT)
                .build()).getData()
                && milvusClient.hasCollection(HasCollectionParam.newBuilder()
                .withCollectionName(MILVUS_COL_CVE_FRAGMENT)
                .build()).getData();
    }

    public static long getCodeFragmentNum() {
        R<GetCollectionStatisticsResponse> respCollectionStatistics = milvusClient.getCollectionStatistics(
                // Return the statistics information of the collection.
                GetCollectionStatisticsParam.newBuilder()
                        .withCollectionName(MILVUS_COL_CODE_FRAGMENT)
                        .build()
        );
        GetCollStatResponseWrapper wrapperCollectionStatistics = new GetCollStatResponseWrapper(respCollectionStatistics.getData());
        return wrapperCollectionStatistics.getRowCount();
    }

    public static void rebuildCodeFragment() {
        if (checkDisconnect()) {
            initConnect();
        }
        milvusClient.dropCollection(
                DropCollectionParam.newBuilder()
                        .withCollectionName(MILVUS_COL_CODE_FRAGMENT)
                        .build()
        );
        initCodeFragmentCollection();

    }

    public static void rebuildCVEFunction() {
        if (checkDisconnect()) {
            initConnect();
        }
        milvusClient.dropCollection(
                DropCollectionParam.newBuilder()
                        .withCollectionName(MILVUS_COL_CVE_FRAGMENT)
                        .build()
        );
        initCVEFunctionCollection();

    }

    private static void initConnect() {
        String host = PropertiesConfig.getHost();
        milvusClient = new MilvusServiceClient(ConnectParam.newBuilder()
                .withHost(host)
                .withPort(19530)
                .build());
        if (milvusClient.showCollections(ShowCollectionsParam.newBuilder().build()).getStatus() != 0) {
            logger.error("Milvus Connect Error!");
        }
    }


    private static void insert(List<Field> fields, String collection) {
        if (checkDisconnect()) {
            initConnect();
        }
        InsertParam insertParam;
        if (fields.size() >= 65500) {
            logger.warn("this file has above 65500 : " + fields.size() + ", once insertJavaCVE should below 65536");
            insert(fields.subList(65500, fields.size()), collection);
            fields = fields.subList(0, 65500);
        }
        insertParam = InsertParam.newBuilder()
                .withCollectionName(collection)
                .withFields(fields)
                .build();
        milvusClient.insert(insertParam);

    }

    public static void flushData() {
        if (checkDisconnect()) {
            initConnect();
        }

        milvusClient.flush(FlushParam.newBuilder()
                .addCollectionName(MILVUS_COL_CODE_FRAGMENT) // 添加要刷新的集合名称
                .withSyncFlush(true) // 启用同步刷新模式
                .build());
        milvusClient.flush(FlushParam.newBuilder()
                .addCollectionName(MILVUS_COL_CVE_FRAGMENT) // 添加要刷新的集合名称
                .withSyncFlush(true) // 启用同步刷新模式
                .build());
    }

    public static void insertCodeFragmentData(List<CodeFragment> codeFragments, String uuid) {
        if (codeFragments.isEmpty())
            return;
        List<String> codeIDArray = new ArrayList<>();
        List<String> hubTagInfoUUIDArray = new ArrayList<>();
        List<Integer> startLineArray = new ArrayList<>();
        List<Integer> endLineArray = new ArrayList<>();
        List<String> locationArray = new ArrayList<>();
        List<Integer> linesArray = new ArrayList<>();
        List<ByteBuffer> simHashCodeArray = new ArrayList<>();
        for (CodeFragment codeFragment : codeFragments) {
            String codeID = UUID.randomUUID().toString();
            codeIDArray.add(codeID);
            hubTagInfoUUIDArray.add(uuid);
            startLineArray.add(codeFragment.getStartLine());
            endLineArray.add(codeFragment.getEndLine());
            locationArray.add(codeFragment.getLocation());
            linesArray.add(codeFragment.getLines());
            simHashCodeArray.add(codeFragment.getSimHashCode());
            FileUtil.mkdir("./data/hub/" + uuid + "/");
            FileUtil.write(JSONArray.toJSONString(codeFragment.getTokenStringList())
                    , "./data/hub/" + uuid + "/" + codeID + ".json");
        }

        List<Field> fields = new ArrayList<>();
        fields.add(new InsertParam.Field("codeID", codeIDArray));
        fields.add(new InsertParam.Field("hubTagInfoUUID", hubTagInfoUUIDArray));
        fields.add(new InsertParam.Field("startLine", startLineArray));
        fields.add(new InsertParam.Field("endLine", endLineArray));
        fields.add(new InsertParam.Field("location", locationArray));
        fields.add(new InsertParam.Field("lines", linesArray));
        fields.add(new InsertParam.Field("simHashCode", simHashCodeArray));
        if (checkDisconnect()) {
            initConnect();
        }
        insert(fields, MILVUS_COL_CODE_FRAGMENT);
    }

    public static void insertCVEFunctionData(List<CVEFunction> cveFunctions) {
        if (cveFunctions.isEmpty())
            return;
        List<String> codeIDArray = new ArrayList<>();
        List<String> CVEArray = new ArrayList<>();
        List<Integer> linesArray = new ArrayList<>();
        List<ByteBuffer> simHashCodeArray = new ArrayList<>();
        for (CVEFunction cveFunction : cveFunctions) {
            codeIDArray.add(cveFunction.getUuid());
            CVEArray.add(cveFunction.getCVE());
            linesArray.add(cveFunction.getLines());
            simHashCodeArray.add(cveFunction.getSimHashCode());
            FileUtil.write(JSONArray.toJSONString(cveFunction.getTokenStringList())
                    , "./data/CVE/" + cveFunction.getCVE() + "/" + cveFunction.getUuid() + ".json");
        }
        List<Field> fields = new ArrayList<>();
        fields.add(new InsertParam.Field("codeID", codeIDArray));
        fields.add(new InsertParam.Field("CVE", CVEArray));
        fields.add(new InsertParam.Field("lines", linesArray));
        fields.add(new InsertParam.Field("simHashCode", simHashCodeArray));
        if (checkDisconnect()) {
            initConnect();
        }
        insert(fields, MILVUS_COL_CVE_FRAGMENT);
    }


    private static void initCodeFragmentCollection() {
        FieldType codeID = FieldType.newBuilder()
                .withName("codeID")
                .withDataType(DataType.VarChar)
                .withMaxLength(64)
                .withPrimaryKey(true)
                .build();
        FieldType hubTagInfoUUID = FieldType.newBuilder()
                .withName("hubTagInfoUUID")
                .withDataType(DataType.VarChar)
                .withMaxLength(64)
                .build();
        FieldType startLine = FieldType.newBuilder()
                .withName("startLine")
                .withDataType(DataType.Int32)
                .build();
        FieldType endLine = FieldType.newBuilder()
                .withName("endLine")
                .withDataType(DataType.Int32)
                .build();
        FieldType location = FieldType.newBuilder()
                .withName("location")
                .withDataType(DataType.VarChar)
                .withMaxLength(256)
                .build();
        FieldType lines = FieldType.newBuilder()
                .withName("lines")
                .withDataType(DataType.Int32)
                .build();
        FieldType simHashCode = FieldType.newBuilder()
                .withName("simHashCode")
                .withDataType(DataType.BinaryVector)
                .withDimension(128)
                .build();

        CreateCollectionParam createCollectionReq = CreateCollectionParam.newBuilder()
                .withCollectionName(MILVUS_COL_CODE_FRAGMENT)
                .withDescription("Code Fragment search")
                .withShardsNum(2)
                .addFieldType(codeID)
                .addFieldType(hubTagInfoUUID)
                .addFieldType(startLine)
                .addFieldType(endLine)
                .addFieldType(location)
                .addFieldType(lines)
                .addFieldType(simHashCode)
                .build();
        init(createCollectionReq);

    }

    private static void initCVEFunctionCollection() {
        FieldType codeID = FieldType.newBuilder()
                .withName("codeID")
                .withDataType(DataType.VarChar)
                .withMaxLength(64)
                .withPrimaryKey(true)
                .build();
        FieldType CVE = FieldType.newBuilder()
                .withName("CVE")
                .withDataType(DataType.VarChar)
                .withMaxLength(64)
                .build();
        FieldType lines = FieldType.newBuilder()
                .withName("lines")
                .withDataType(DataType.Int32)
                .build();
        FieldType simHashCode = FieldType.newBuilder()
                .withName("simHashCode")
                .withDataType(DataType.BinaryVector)
                .withDimension(128)
                .build();

        CreateCollectionParam createCollectionReq = CreateCollectionParam.newBuilder()
                .withCollectionName(MILVUS_COL_CVE_FRAGMENT)
                .withDescription("CVE Function search")
                .withShardsNum(2)
                .addFieldType(codeID)
                .addFieldType(CVE)
                .addFieldType(lines)
                .addFieldType(simHashCode)
                .build();
        init(createCollectionReq);
    }
    private static void init(CreateCollectionParam createCollectionReq){
        String collectionName = createCollectionReq.getCollectionName();
        milvusClient.createCollection(createCollectionReq);
        final IndexType INDEX_TYPE = IndexType.BIN_FLAT;
        final String INDEX_PARAM = "{\"nlist\":1024}";
        milvusClient.createIndex(
                CreateIndexParam.newBuilder()
                        .withCollectionName(collectionName)
                        .withFieldName("simHashCode")
                        .withIndexType(INDEX_TYPE)
                        .withMetricType(MetricType.HAMMING)
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

    private static int getDeviationValue(int line, int simThreshold1) {
        int deviation = 0;

        switch (simThreshold1) {

            case 6:
                ///simThreshold2 = 5;
                break;

            case 7:
                if (line < 6) {
                    deviation = -1;
                } else if (line < 8) {
                    deviation = -1;
                }
                ///simThreshold2 = 6;
                break;

            case 8:
                if (line < 6) {
                    deviation = -2;
                } else if (line < 8) {
                    deviation = -1;
                }
                ///simThreshold2 = 7;
                break;

            case 9:
                if (line < 6) {
                    deviation = -3;
                } else if (line < 8) {
                    deviation = -2;
                } else if (line < 10) {
                    deviation = -1;
                }
                ///simThreshold2 = 8;
                break;

            case 10:
                if (line < 6) {
                    deviation = -3;
                } else if (line < 8) {
                    deviation = -2;
                } else if (line < 10) {
                    deviation = -2;
                } else if (line < 20) {
                    deviation = -1;
                }
                ///simThreshold2 = 8;
                break;

            case 11:
                if (line < 6) {
                    deviation = -4;
                } else if (line < 8) {
                    deviation = -3;
                } else if (line < 10) {
                    deviation = -2;
                } else if (line < 20) {
                    deviation = -1;
                }
                ///simThreshold2 = 9;
                break;

            case 12:
            case 13:
                if (line < 6) {
                    deviation = -4;
                } else if (line < 8) {
                    deviation = -3;
                } else if (line < 10) {
                    deviation = -3;
                } else if (line < 20) {
                    deviation = -2;
                } else if (line < 30) {
                    deviation = -1;
                }
                ///simThreshold2 = 12;
                break;
        }
        return deviation;
    }

    public static R<SearchResults> searchCodeFragmentData(CodeFragment codeFragment) {
        final String SEARCH_PARAM = "{\"nprobe\":10}";
        List<String> search_output_fields;
        List<ByteBuffer> search_vectors;
        SearchParam searchParam;

        search_output_fields = Arrays.asList("codeID", "hubTagInfoUUID", "startLine", "endLine", "location");
        search_vectors = Collections.singletonList(codeFragment.getSimHashCode());
        searchParam = SearchParam.newBuilder()
                .withCollectionName(MILVUS_COL_CODE_FRAGMENT)
                .withConsistencyLevel(ConsistencyLevelEnum.EVENTUALLY)
                .withMetricType(MetricType.HAMMING)
                .withOutFields(search_output_fields)
                .withTopK(SEARCH_K)
                .withVectors(search_vectors)
                .withVectorFieldName("simHashCode")
                .withParams(SEARCH_PARAM)
                .build();
        return milvusClient.search(searchParam);
    }

    public static R<SearchResults> searchCVEFunctionData(CodeFragment codeFragment) {
        final String SEARCH_PARAM = "{\"nprobe\":10}";
        List<String> search_output_fields;
        List<ByteBuffer> search_vectors;
        SearchParam searchParam;

        search_output_fields = Arrays.asList("codeID", "CVE", "lines");
        search_vectors = Collections.singletonList(codeFragment.getSimHashCode());
        searchParam = SearchParam.newBuilder()
                .withCollectionName(MILVUS_COL_CVE_FRAGMENT)
                .withConsistencyLevel(ConsistencyLevelEnum.EVENTUALLY)
                .withMetricType(MetricType.HAMMING)
                .withOutFields(search_output_fields)
                .withTopK(SEARCH_K)
                .withVectors(search_vectors)
                .withVectorFieldName("simHashCode")
                .withParams(SEARCH_PARAM)
                .build();
        return milvusClient.search(searchParam);
    }

    public static List<MilvusCodeFragmentResult> findCodeFragmentGroup(CodeFragment codeFragment) {
        List<MilvusCodeFragmentResult> neighbors = new ArrayList<>();
        int deviation;
        int simThreshold = 13;
        int dynamicSimThreshold;// = simThreshold + deviation;
        deviation = getDeviationValue(codeFragment.getLines(), simThreshold);
        dynamicSimThreshold = simThreshold + deviation;
        R<SearchResults> res = searchCodeFragmentData(codeFragment);
        List<FieldData> collectionLists = res.getData().getResults().getFieldsDataList();
        if (collectionLists.isEmpty())
            return neighbors;
        for (int i = 0; i < SEARCH_K; i++) {
            if (res.getData().getResults().getScoresCount() <= i)
                break;
            MilvusCodeFragmentResult milvusCodeFragmentResult = new MilvusCodeFragmentResult();
            for (FieldData collectionList : collectionLists) {
                switch (collectionList.getFieldName()) {
                    case "codeID": {
                        milvusCodeFragmentResult.setCodeID(collectionList.getScalars().getStringData().getData(i));
                        break;
                    }
                    case "hubTagInfoUUID": {
                        milvusCodeFragmentResult.setHubTagInfoUUID(collectionList.getScalars().getStringData().getData(i));
                        break;
                    }
                    case "startLine": {
                        milvusCodeFragmentResult.setStartLine(collectionList.getScalars().getIntData().getData(i));
                        break;
                    }
                    case "endLine": {
                        milvusCodeFragmentResult.setEndLine(collectionList.getScalars().getIntData().getData(i));
                        break;
                    }
                    case "location": {
                        milvusCodeFragmentResult.setLocation(collectionList.getScalars().getStringData().getData(i));
                        break;
                    }
                }
            }
            milvusCodeFragmentResult.setHammingDistance((int) res.getData().getResults().getScores(i));
            if (milvusCodeFragmentResult.getHammingDistance() <= dynamicSimThreshold)
                neighbors.add(milvusCodeFragmentResult);
        }
        return neighbors;
    }

    public static List<MilvusCVEFunctionResult> findCVEFunctionGroup(CodeFragment codeFragment) {
        List<MilvusCVEFunctionResult> neighbors = new ArrayList<>();
        int simThreshold = 13;
        int deviation = getDeviationValue(codeFragment.getLines(), simThreshold);
        int dynamicSimThreshold = simThreshold + deviation;
        R<SearchResults> res = searchCVEFunctionData(codeFragment);
        List<FieldData> collectionLists = res.getData().getResults().getFieldsDataList();
        if (collectionLists.isEmpty())
            return neighbors;
        for (int i = 0; i < SEARCH_K; i++) {
            MilvusCVEFunctionResult milvusResult = new MilvusCVEFunctionResult();
            if (collectionLists.get(0).getScalars().getStringData().getDataCount() <= i)
                break;
            for (FieldData collectionList : collectionLists) {
                switch (collectionList.getFieldName()) {
                    case "codeID": {
                        milvusResult.setCodeID(collectionList.getScalars().getStringData().getData(i));
                        break;
                    }
                    case "CVE": {
                        milvusResult.setCVE(collectionList.getScalars().getStringData().getData(i));
                        break;
                    }
                    case "lines": {
                        milvusResult.setLines(collectionList.getScalars().getIntData().getData(i));
                        break;
                    }
                }
            }
            milvusResult.setHammingDistance((int) res.getData().getResults().getScores(i));
            if (milvusResult.getHammingDistance() <= dynamicSimThreshold)
                neighbors.add(milvusResult);
        }
        return neighbors;
    }
}
