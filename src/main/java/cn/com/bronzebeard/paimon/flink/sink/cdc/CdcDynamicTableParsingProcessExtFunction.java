/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.com.bronzebeard.paimon.flink.sink.cdc;

import cn.com.bronzebeard.paimon.flink.common.util.Asset;
import cn.com.bronzebeard.paimon.flink.mysql.format.PatternMatchedToTargetMapper;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.sink.cdc.CdcMultiplexRecord;
import org.apache.paimon.flink.sink.cdc.CdcRecord;
import org.apache.paimon.flink.sink.cdc.EventParser;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.types.DataField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * A {@link ProcessFunction} to parse CDC change event to either a list of {@link DataField}s or
 * {@link CdcRecord} and send them to different side outputs according to table name. This process
 * function will capture newly added tables when syncing entire database and in cases where the
 * newly added tables are including by attesting table filters.
 *
 * <p>This {@link ProcessFunction} can handle records for different tables at the same time.
 *
 * @param <T> CDC change event type
 */
public class CdcDynamicTableParsingProcessExtFunction<T> extends ProcessFunction<T, Void> {

    private static final Logger LOG =
            LoggerFactory.getLogger(CdcDynamicTableParsingProcessExtFunction.class);

    public static final OutputTag<CdcMultiplexRecord> DYNAMIC_OUTPUT_TAG =
            new OutputTag<>("paimon-dynamic-table", TypeInformation.of(CdcMultiplexRecord.class));

    public static final OutputTag<Tuple2<Identifier, List<DataField>>>
            DYNAMIC_SCHEMA_CHANGE_OUTPUT_TAG =
            new OutputTag<>(
                    "paimon-dynamic-table-schema-change",
                    TypeInformation.of(
                            new TypeHint<Tuple2<Identifier, List<DataField>>>() {
                            }));

    private final EventParser.Factory<T> parserFactory;
    private final Catalog.Loader catalogLoader;

    private transient EventParser<T> parser;
    private transient Catalog catalog;

    PatternMatchedToTargetMapper<Identifier> referenceTableNameConverter;

    public CdcDynamicTableParsingProcessExtFunction(
            Catalog.Loader catalogLoader, EventParser.Factory<T> parserFactory) {
        this.catalogLoader = catalogLoader;
        this.parserFactory = parserFactory;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        parser = parserFactory.create();
        catalog = catalogLoader.load();
    }

    @Override
    public void processElement(T raw, Context context, Collector<Void> collector) throws Exception {
        RichCdcMultiplexRecord multiplexRecord = (RichCdcMultiplexRecord) raw;
        String fullSrcTableName = String.format("%s.%s", multiplexRecord.databaseName(), multiplexRecord.tableName());
        parser.setRawEvent(raw);

        // CDC Ingestion only supports single database at this time being.
        //    In the future, there will be a mapping between source databases
        //    and target paimon databases


        // check for newly added table
        parser.parseNewTable()
                .ifPresent(
                        schema -> {
                            if (referenceTableNameConverter.addNewMatchedToMapper(fullSrcTableName)) {
                                LOG.info("Newly added table {} is included in the table list, ", fullSrcTableName);
                            } else {
                                LOG.warn("Newly added table {} is not included in the table list" +
                                                ", please check your table list configuration."
                                        , fullSrcTableName);
                            }
                        });
        List<Identifier> targetTableList = referenceTableNameConverter.getTargetByMatched(fullSrcTableName);
        Asset.isNotEmpty(targetTableList, "Cannot find target table for table %s ", fullSrcTableName);
        List<DataField> schemaChange = parser.parseSchemaChange();
        if (!schemaChange.isEmpty()) {
            targetTableList.forEach(identifier -> {
                context.output(
                        DYNAMIC_SCHEMA_CHANGE_OUTPUT_TAG,
                        Tuple2.of(Identifier.create(identifier.getDatabaseName(), identifier.getObjectName()), schemaChange));

            });
        }

        parser.parseRecords()
                .stream().map(record -> (CdcRecordWithParseInfo) record)
                .forEach(
                        record -> context.output(
                                DYNAMIC_OUTPUT_TAG,
                                wrapRecord(record.getParseInfo().getTgtDb(), record.getParseInfo().getTgtTable()
                                        , record.rawRecord()))
                );
    }

    private CdcMultiplexRecord wrapRecord(String databaseName, String tableName, CdcRecord record) {
        return CdcMultiplexRecord.fromCdcRecord(databaseName, tableName, record);
    }


    public void setReferenceTableNameConverter(PatternMatchedToTargetMapper<Identifier> referenceTableNameConverter) {
        this.referenceTableNameConverter = referenceTableNameConverter;
    }
}
