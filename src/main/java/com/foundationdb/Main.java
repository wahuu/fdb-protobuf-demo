package com.foundationdb;

import java.util.List;
import java.util.function.Function;

import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Message;

public class Main {

    public static void main(String[] args) {
        FDBDatabase db = FDBDatabaseFactory.instance().getDatabase();

        // Define the keyspace for our application
        KeySpace keySpace = new KeySpace(
                new KeySpaceDirectory("record-layer-demo", KeySpaceDirectory.KeyType.STRING, "record-layer-demo"));
        // Get the path where our record store will be rooted
        KeySpacePath path = keySpace.path("record-layer-demo");

        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder()
                .setRecords(RecordLayerDemoProto.getDescriptor());

        metaDataBuilder
                .getRecordType("Order")
                .setPrimaryKey(Key.Expressions.field("order_id"));

        metaDataBuilder
                .addIndex("Order", new Index("priceIndex", Key.Expressions.field("price")));

        RecordMetaData recordMetaData = metaDataBuilder.build();

        Function<FDBRecordContext, FDBRecordStore> recordStoreProvider = context -> FDBRecordStore.newBuilder()
                .setMetaDataProvider(recordMetaData)
                .setContext(context)
                .setKeySpacePath(path)
                .createOrOpen();

        /*db.run(context -> {
            FDBRecordStore recordStore = recordStoreProvider.apply(context);
            recordStore.saveRecord(RecordLayerDemoProto.Order.newBuilder()
                    .setOrderId(1)
                    .setPrice(123)
                    .setFlower(buildFlower(FlowerType.ROSE, RecordLayerDemoProto.Color.RED))
                    .build());
            recordStore.saveRecord(RecordLayerDemoProto.Order.newBuilder()
                    .setOrderId(23)
                    .setPrice(34)
                    .setFlower(buildFlower(FlowerType.ROSE, RecordLayerDemoProto.Color.PINK))
                    .build());
            recordStore.saveRecord(RecordLayerDemoProto.Order.newBuilder()
                    .setOrderId(3)
                    .setPrice(55)
                    .setFlower(buildFlower(FlowerType.TULIP, RecordLayerDemoProto.Color.YELLOW))
                    .build());
            return null;
        });*/

        FDBStoredRecord<Message> storedRecord = db
                .run(context -> recordStoreProvider.apply(context).loadRecord(Tuple.from(1)));

        assert storedRecord != null;

        FDBStoredRecord<Message> notExistingRecord = db
                .run(context -> recordStoreProvider.apply(context).loadRecord(Tuple.from(133)));
        assert notExistingRecord == null;

        RecordLayerDemoProto.Order order = RecordLayerDemoProto.Order.newBuilder()
                .mergeFrom(storedRecord.getRecord())
                .build();
        System.out.println(order);

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("Order")
                .setFilter(Query.and(
                        Query.field("price").lessThan(50),
                        Query.field("flower").matches(Query.field("type").equalsValue(FlowerType.ROSE.name()))))
                .build();

        List<RecordLayerDemoProto.Order> orders = db.run(context -> {
            FDBRecordStore recordStore = recordStoreProvider.apply(context);

            RecordCursor<FDBQueriedRecord<Message>> asyncCursor = recordStore.executeQuery(query);
            return asyncCursor
                    .map(queriedRecord -> RecordLayerDemoProto.Order.newBuilder()
                            .mergeFrom(queriedRecord.getRecord()).build())
                    .asList().join();
        });
        orders.forEach(System.out::println);
    }

    private enum FlowerType {
        ROSE,
        TULIP,
        LILY,
    }

    private static RecordLayerDemoProto.Flower buildFlower(FlowerType type, RecordLayerDemoProto.Color color) {
        return RecordLayerDemoProto.Flower.newBuilder().setType(type.name()).setColor(color).build();
    }
}
