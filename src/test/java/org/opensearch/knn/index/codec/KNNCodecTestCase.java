/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.codec;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.join.BitSetProducer;
import org.mockito.MockedStatic;
import org.opensearch.Version;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.knn.KNNTestCase;
import org.opensearch.knn.common.KNNConstants;
import org.opensearch.knn.index.engine.KNNMethodConfigContext;
import org.opensearch.knn.index.engine.KNNMethodContext;
import org.opensearch.knn.index.KNNSettings;
import org.opensearch.knn.index.engine.MethodComponentContext;
import org.opensearch.knn.index.SpaceType;
import org.opensearch.knn.index.VectorDataType;
import org.opensearch.knn.index.VectorField;
import org.opensearch.knn.index.mapper.CompressionLevel;
import org.opensearch.knn.index.mapper.KNNVectorFieldType;
import org.opensearch.knn.index.mapper.Mode;
import org.opensearch.knn.index.query.BaseQueryFactory;
import org.opensearch.knn.index.query.KNNQueryFactory;
import org.opensearch.knn.jni.JNICommons;
import org.opensearch.knn.jni.JNIService;
import org.opensearch.knn.index.query.KNNQuery;
import org.opensearch.knn.index.mapper.KNNVectorFieldMapper;
import org.opensearch.knn.index.query.KNNWeight;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.knn.index.memory.NativeMemoryLoadStrategy;
import org.opensearch.knn.index.engine.KNNEngine;
import org.opensearch.knn.indices.Model;
import org.opensearch.knn.indices.ModelCache;
import org.opensearch.knn.indices.ModelDao;
import org.opensearch.knn.indices.ModelMetadata;
import org.opensearch.knn.indices.ModelState;
import org.opensearch.watcher.ResourceWatcherService;
import org.mockito.Mockito;

import java.io.IOException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.Version.CURRENT;
import static org.opensearch.knn.common.KNNConstants.DEFAULT_VECTOR_DATA_TYPE_FIELD;
import static org.opensearch.knn.common.KNNConstants.HNSW_ALGO_EF_CONSTRUCTION;
import static org.opensearch.knn.common.KNNConstants.HNSW_ALGO_M;
import static org.opensearch.knn.common.KNNConstants.INDEX_DESCRIPTION_PARAMETER;
import static org.opensearch.knn.common.KNNConstants.METHOD_HNSW;
import static org.opensearch.knn.common.KNNConstants.METHOD_PARAMETER_EF_CONSTRUCTION;
import static org.opensearch.knn.common.KNNConstants.METHOD_PARAMETER_M;
import static org.opensearch.knn.common.KNNConstants.SPACE_TYPE;
import static org.opensearch.knn.index.KNNSettings.MODEL_CACHE_SIZE_LIMIT_SETTING;

/**
 * Test used for testing Codecs
 */
public class KNNCodecTestCase extends KNNTestCase {
    private static final FieldType sampleFieldType;
    static {
        KNNMethodConfigContext knnMethodConfigContext = KNNMethodConfigContext.builder()
            .versionCreated(CURRENT)
            .vectorDataType(VectorDataType.DEFAULT)
            .build();
        KNNMethodContext knnMethodContext = new KNNMethodContext(
            KNNEngine.NMSLIB,
            SpaceType.DEFAULT,
            new MethodComponentContext(METHOD_HNSW, ImmutableMap.of(METHOD_PARAMETER_M, 16, METHOD_PARAMETER_EF_CONSTRUCTION, 512))
        );
        String parameterString;
        try {
            parameterString = XContentFactory.jsonBuilder()
                .map(
                    knnMethodContext.getKnnEngine()
                        .getKNNLibraryIndexingContext(knnMethodContext, knnMethodConfigContext)
                        .getLibraryParameters()
                )
                .toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        sampleFieldType = new FieldType(KNNVectorFieldMapper.Defaults.FIELD_TYPE);
        sampleFieldType.setDocValuesType(DocValuesType.BINARY);
        sampleFieldType.putAttribute(KNNVectorFieldMapper.KNN_FIELD, "true");
        sampleFieldType.putAttribute(KNNConstants.KNN_ENGINE, knnMethodContext.getKnnEngine().getName());
        sampleFieldType.putAttribute(KNNConstants.SPACE_TYPE, knnMethodContext.getSpaceType().getValue());
        sampleFieldType.putAttribute(KNNConstants.PARAMETERS, parameterString);
        sampleFieldType.freeze();
    }
    private static final String FIELD_NAME_ONE = "test_vector_one";
    private static final String FIELD_NAME_TWO = "test_vector_two";

    protected void setUpMockClusterService() {
        ClusterService clusterService = mock(ClusterService.class, RETURNS_DEEP_STUBS);
        Settings settings = Settings.Builder.EMPTY_SETTINGS;
        when(clusterService.state().getMetadata().index(Mockito.anyString()).getSettings()).thenReturn(settings);
        Set<Setting<?>> defaultClusterSettings = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        defaultClusterSettings.addAll(
            KNNSettings.state()
                .getSettings()
                .stream()
                .filter(s -> s.getProperties().contains(Setting.Property.NodeScope))
                .collect(Collectors.toList())
        );
        when(clusterService.getClusterSettings()).thenReturn(new ClusterSettings(Settings.EMPTY, defaultClusterSettings));
        KNNSettings.state().setClusterService(clusterService);
    }

    protected ResourceWatcherService createDisabledResourceWatcherService() {
        final Settings settings = Settings.builder().put("resource.reload.enabled", false).build();
        return new ResourceWatcherService(settings, null);
    }

    public void testMultiFieldsKnnIndex(Codec codec) throws Exception {
        setUpMockClusterService();
        try (Directory dir = newFSDirectory(createTempDir())) {
            IndexWriterConfig iwc = newIndexWriterConfig();
            iwc.setMergeScheduler(new SerialMergeScheduler());
            iwc.setCodec(codec);
            // Set merge policy to no merges so that we create a predictable number of segments.
            iwc.setMergePolicy(NoMergePolicy.INSTANCE);

            /**
             * Add doc with field "test_vector"
             */
            float[] array = { 1.0f, 3.0f, 4.0f };
            VectorField vectorField = new VectorField("test_vector", array, sampleFieldType);
            RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);
            Document doc = new Document();
            doc.add(vectorField);
            writer.addDocument(doc);
            // ensuring the refresh happens, to create the segment and hnsw file
            writer.flush();

            /**
             * Add doc with field "my_vector"
             */
            float[] array1 = { 6.0f, 14.0f };
            VectorField vectorField1 = new VectorField("my_vector", array1, sampleFieldType);
            Document doc1 = new Document();
            doc1.add(vectorField1);
            writer.addDocument(doc1);
            // ensuring the refresh happens, to create the segment and hnsw file
            writer.flush();
            IndexReader reader = writer.getReader();
            writer.close();
            List<String> hnswfiles = Arrays.stream(dir.listAll()).filter(x -> x.contains("hnsw")).collect(Collectors.toList());

            // there should be 2 hnsw index files created. one for test_vector and one for my_vector
            assertEquals(2, hnswfiles.size());
            assertEquals(hnswfiles.stream().filter(x -> x.contains("test_vector")).collect(Collectors.toList()).size(), 1);
            assertEquals(hnswfiles.stream().filter(x -> x.contains("my_vector")).collect(Collectors.toList()).size(), 1);

            // query to verify distance for each of the field
            IndexSearcher searcher = new IndexSearcher(reader);
            float score = searcher.search(
                new KNNQuery("test_vector", new float[] { 1.0f, 0.0f, 0.0f }, 1, "dummy", (BitSetProducer) null),
                10
            ).scoreDocs[0].score;
            float score1 = searcher.search(
                new KNNQuery("my_vector", new float[] { 1.0f, 2.0f }, 1, "dummy", (BitSetProducer) null),
                10
            ).scoreDocs[0].score;
            assertEquals(1.0f / (1 + 25), score, 0.01f);
            assertEquals(1.0f / (1 + 169), score1, 0.01f);

            // query to determine the hits
            assertEquals(
                1,
                searcher.count(new KNNQuery("test_vector", new float[] { 1.0f, 0.0f, 0.0f }, 1, "dummy", (BitSetProducer) null))
            );
            assertEquals(1, searcher.count(new KNNQuery("my_vector", new float[] { 1.0f, 1.0f }, 1, "dummy", (BitSetProducer) null)));

            reader.close();
            NativeMemoryLoadStrategy.IndexLoadStrategy.getInstance().close();
        }
    }

    public void testBuildFromModelTemplate(Codec codec) throws IOException, ExecutionException, InterruptedException {
        // Setup model params
        String modelId = "test-model";
        KNNEngine knnEngine = KNNEngine.FAISS;
        SpaceType spaceType = SpaceType.L2;
        int dimension = 3;

        // "Train" a faiss flat index - this really just creates an empty index that does brute force k-NN
        long vectorsPointer = JNICommons.storeVectorData(0, new float[0][0], 0);
        byte[] modelBlob = JNIService.trainIndex(
            ImmutableMap.of(INDEX_DESCRIPTION_PARAMETER, "Flat", SPACE_TYPE, spaceType.getValue()),
            dimension,
            vectorsPointer,
            KNNEngine.FAISS
        );

        // Setup model cache
        try (MockedStatic<ModelDao.OpenSearchKNNModelDao> modelDaoMockedStatic = Mockito.mockStatic(ModelDao.OpenSearchKNNModelDao.class)) {
            ModelDao.OpenSearchKNNModelDao modelDao = mock(ModelDao.OpenSearchKNNModelDao.class);
            modelDaoMockedStatic.when(ModelDao.OpenSearchKNNModelDao::getInstance).thenReturn(modelDao);

            // Set model state to created
            ModelMetadata modelMetadata1 = new ModelMetadata(
                knnEngine,
                spaceType,
                dimension,
                ModelState.CREATED,
                ZonedDateTime.now(ZoneOffset.UTC).toString(),
                "",
                "",
                "",
                MethodComponentContext.EMPTY,
                VectorDataType.FLOAT,
                Mode.NOT_CONFIGURED,
                CompressionLevel.NOT_CONFIGURED,
                Version.V_EMPTY
            );

            Model mockModel = new Model(modelMetadata1, modelBlob, modelId);
            when(modelDao.get(modelId)).thenReturn(mockModel);
            when(modelDao.getMetadata(modelId)).thenReturn(modelMetadata1);

            Settings settings = settings(CURRENT).put(MODEL_CACHE_SIZE_LIMIT_SETTING.getKey(), "10%").build();
            ClusterSettings clusterSettings = new ClusterSettings(settings, ImmutableSet.of(MODEL_CACHE_SIZE_LIMIT_SETTING));

            ClusterService clusterService = mock(ClusterService.class);
            when(clusterService.getSettings()).thenReturn(settings);
            when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

            ModelCache.initialize(modelDao, clusterService);
            ModelCache.getInstance().removeAll();

            // Setup Lucene
            setUpMockClusterService();
            Directory dir = newFSDirectory(createTempDir());
            IndexWriterConfig iwc = newIndexWriterConfig();
            iwc.setMergeScheduler(new SerialMergeScheduler());
            iwc.setCodec(codec);

            FieldType fieldType = new FieldType(KNNVectorFieldMapper.Defaults.FIELD_TYPE);
            fieldType.setDocValuesType(DocValuesType.BINARY);
            fieldType.putAttribute(KNNConstants.MODEL_ID, modelId);
            fieldType.freeze();

            // Add the documents to the index
            float[][] arrays = { { 1.0f, 3.0f, 4.0f }, { 2.0f, 5.0f, 8.0f }, { 3.0f, 6.0f, 9.0f }, { 4.0f, 7.0f, 10.0f } };

            RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);
            String fieldName = "test_vector";
            for (float[] array : arrays) {
                VectorField vectorField = new VectorField(fieldName, array, fieldType);
                Document doc = new Document();
                doc.add(vectorField);
                writer.addDocument(doc);
            }

            IndexReader reader = writer.getReader();
            writer.close();

            // Make sure that search returns the correct results
            KNNWeight.initialize(modelDao);
            float[] query = { 10.0f, 10.0f, 10.0f };
            IndexSearcher searcher = new IndexSearcher(reader);
            TopDocs topDocs = searcher.search(new KNNQuery(fieldName, query, 4, "dummy", (BitSetProducer) null), 10);

            assertEquals(3, topDocs.scoreDocs[0].doc);
            assertEquals(2, topDocs.scoreDocs[1].doc);
            assertEquals(1, topDocs.scoreDocs[2].doc);
            assertEquals(0, topDocs.scoreDocs[3].doc);

            reader.close();
            dir.close();
            NativeMemoryLoadStrategy.IndexLoadStrategy.getInstance().close();
        }
    }

    public void testWriteByOldCodec(Codec codec) throws IOException {
        setUpMockClusterService();
        Directory dir = newFSDirectory(createTempDir());
        IndexWriterConfig iwc = newIndexWriterConfig();
        iwc.setMergeScheduler(new SerialMergeScheduler());
        iwc.setCodec(codec);

        /**
         * Add doc with field "test_vector", expect it to fail
         */
        float[] array = { 1.0f, 3.0f, 4.0f };
        VectorField vectorField = new VectorField("test_vector", array, sampleFieldType);
        try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc)) {
            Document doc = new Document();
            doc.add(vectorField);
            expectThrows(UnsupportedOperationException.class, () -> writer.addDocument(doc));
        }

        dir.close();
        NativeMemoryLoadStrategy.IndexLoadStrategy.getInstance().close();
    }

    public void testKnnVectorIndex(
        final Function<PerFieldKnnVectorsFormat, Codec> codecProvider,
        final Function<MapperService, PerFieldKnnVectorsFormat> perFieldKnnVectorsFormatProvider
    ) throws Exception {
        final MapperService mapperService = mock(MapperService.class);
        final KNNMethodContext knnMethodContext = new KNNMethodContext(
            KNNEngine.LUCENE,
            SpaceType.L2,
            new MethodComponentContext(METHOD_HNSW, Map.of(HNSW_ALGO_M, 16, HNSW_ALGO_EF_CONSTRUCTION, 256))
        );

        final KNNVectorFieldType mappedFieldType1 = new KNNVectorFieldType(
            "test",
            Collections.emptyMap(),
            VectorDataType.FLOAT,
            getMappingConfigForMethodMapping(knnMethodContext, 3)
        );
        final KNNVectorFieldType mappedFieldType2 = new KNNVectorFieldType(
            "test",
            Collections.emptyMap(),
            VectorDataType.FLOAT,
            getMappingConfigForMethodMapping(knnMethodContext, 2)
        );
        when(mapperService.fieldType(eq(FIELD_NAME_ONE))).thenReturn(mappedFieldType1);
        when(mapperService.fieldType(eq(FIELD_NAME_TWO))).thenReturn(mappedFieldType2);

        var perFieldKnnVectorsFormatSpy = spy(perFieldKnnVectorsFormatProvider.apply(mapperService));
        final Codec codec = codecProvider.apply(perFieldKnnVectorsFormatSpy);

        setUpMockClusterService();
        Directory dir = newFSDirectory(createTempDir());
        IndexWriterConfig iwc = newIndexWriterConfig();
        iwc.setMergeScheduler(new SerialMergeScheduler());
        iwc.setCodec(codec);

        /**
         * Add doc with field "test_vector_one"
         */
        final FieldType luceneFieldType = KnnFloatVectorField.createFieldType(3, VectorSimilarityFunction.EUCLIDEAN);
        float[] array = { 1.0f, 3.0f, 4.0f };
        KnnFloatVectorField vectorField = new KnnFloatVectorField(FIELD_NAME_ONE, array, luceneFieldType);
        RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);
        Document doc = new Document();
        doc.add(vectorField);
        writer.addDocument(doc);
        writer.commit();
        IndexReader reader = writer.getReader();
        writer.close();

        verify(perFieldKnnVectorsFormatSpy, atLeastOnce()).getKnnVectorsFormatForField(eq(FIELD_NAME_ONE));
        verify(perFieldKnnVectorsFormatSpy, atLeastOnce()).getMaxDimensions(eq(FIELD_NAME_ONE));

        IndexSearcher searcher = new IndexSearcher(reader);

        Query query = KNNQueryFactory.create(
            BaseQueryFactory.CreateQueryRequest.builder()
                .knnEngine(KNNEngine.LUCENE)
                .indexName("dummy")
                .fieldName(FIELD_NAME_ONE)
                .vector(new float[] { 1.0f, 0.0f, 0.0f })
                .k(1)
                .vectorDataType(DEFAULT_VECTOR_DATA_TYPE_FIELD)
                .build()
        );

        assertEquals(1, searcher.count(query));

        reader.close();

        /**
         * Add doc with field "test_vector_two"
         */
        IndexWriterConfig iwc1 = newIndexWriterConfig();
        iwc1.setMergeScheduler(new SerialMergeScheduler());
        iwc1.setCodec(codec);
        writer = new RandomIndexWriter(random(), dir, iwc1);
        final FieldType luceneFieldType1 = KnnFloatVectorField.createFieldType(2, VectorSimilarityFunction.EUCLIDEAN);
        float[] array1 = { 6.0f, 14.0f };
        KnnFloatVectorField vectorField1 = new KnnFloatVectorField(FIELD_NAME_TWO, array1, luceneFieldType1);
        Document doc1 = new Document();
        doc1.add(vectorField1);
        writer.addDocument(doc1);
        IndexReader reader1 = writer.getReader();
        writer.close();

        verify(perFieldKnnVectorsFormatSpy, atLeastOnce()).getKnnVectorsFormatForField(eq(FIELD_NAME_TWO));
        verify(perFieldKnnVectorsFormatSpy, atLeastOnce()).getMaxDimensions(eq(FIELD_NAME_TWO));

        IndexSearcher searcher1 = new IndexSearcher(reader1);
        Query query1 = KNNQueryFactory.create(
            BaseQueryFactory.CreateQueryRequest.builder()
                .knnEngine(KNNEngine.LUCENE)
                .indexName("dummy")
                .fieldName(FIELD_NAME_TWO)
                .vector(new float[] { 1.0f, 0.0f })
                .k(1)
                .vectorDataType(DEFAULT_VECTOR_DATA_TYPE_FIELD)
                .build()
        );

        assertEquals(1, searcher1.count(query1));

        reader1.close();
        dir.close();
        NativeMemoryLoadStrategy.IndexLoadStrategy.getInstance().close();
    }
}
