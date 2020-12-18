package org.goldengate.delivery.handler.testing;

import oracle.goldengate.datasource.*;
import oracle.goldengate.datasource.meta.*;
import oracle.goldengate.delivery.handler.marklogic.MarkLogicHandler;
import oracle.goldengate.util.DateString;
import oracle.goldengate.util.DsMetric;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class GGInputBuilder {
    protected MarkLogicHandler marklogicHandler;
    protected String schemaName;
    protected String tableName;
    protected Long scn;
    protected DsOperation.OpType opType;
    protected GGDataSource.Status addStatus;
    protected GGDataSource.Status commitStatus;

    protected ArrayList<ColumnMetaData> columnMetaData = new ArrayList<>();
    protected List<DsColumn> columns = new ArrayList<>();
    protected short currentKeyColumnIndex;
    protected boolean built = false;

    private GGInputBuilder(DsOperation.OpType opType, MarkLogicHandler marklogicHandler) {
        this.opType = opType;
        this.marklogicHandler = marklogicHandler;
        this.currentKeyColumnIndex = 0;
    }

    public static GGInputBuilder newUpdate(MarkLogicHandler marklogicHandler) {
        return new GGInputBuilder(DsOperation.OpType.DO_UPDATE, marklogicHandler);
    }

    public static GGInputBuilder newPkUpdate(MarkLogicHandler marklogicHandler) {
        return new GGInputBuilder(DsOperation.OpType.DO_UNIFIED_PK_UPDATE_VAL, marklogicHandler);
    }

    public static GGInputBuilder newInsert(MarkLogicHandler marklogicHandler) {
        return new GGInputBuilder(DsOperation.OpType.DO_INSERT, marklogicHandler);
    }

    public static GGInputBuilder newDelete(MarkLogicHandler marklogicHandler) {
        return new GGInputBuilder(DsOperation.OpType.DO_DELETE, marklogicHandler);
    }

    protected void verifyNotCommitted() throws IllegalStateException {
        if(this.built) {
            throw new IllegalStateException("GGInputBuilder has already been committed.");
        }
    }

    public GGInputBuilder withScn(Long scn) {
        this.scn = scn;
        return this;
    }

    public GGInputBuilder withSchema(String schemaName) {
        verifyNotCommitted();
        this.schemaName = schemaName;
        return this;
    }

    public GGInputBuilder withTable(String tableName) {
        verifyNotCommitted();
        this.tableName = tableName;
        return this;
    }


    protected DsColumnBeforeValue beforeColumn(Object value) {
        if(value == null) {
            return new DsColumnBeforeValue(null, null);
        } else if (value instanceof String) {
            return new DsColumnBeforeValue((String)value, ((String)value).getBytes());
        } else if (value instanceof byte[]) {
            return new DsColumnBeforeValue(null, (byte[])value);
        } else if (value instanceof DateString) {
            return new DsColumnBeforeValue((DateString)value);
        } else {
            String stringValue = value == null ? null : value.toString();
            byte bytesValue[] = stringValue == null ? null : stringValue.getBytes();
            return new DsColumnBeforeValue(stringValue, bytesValue);
        }
    }

    protected DsColumnAfterValue afterColumn(Object value) {
        if(value == null) {
            return new DsColumnAfterValue(null, null);
        } else if (value instanceof String) {
            return new DsColumnAfterValue((String)value, ((String)value).getBytes());
        } else if (value instanceof byte[]) {
            return new DsColumnAfterValue(null, (byte[])value);
        } else if (value instanceof DateString) {
            return new DsColumnAfterValue((DateString)value);
        } else {
            String stringValue = value == null ? null : value.toString();
            byte bytesValue[] = stringValue == null ? null : stringValue.getBytes();
            return new DsColumnAfterValue(stringValue, bytesValue);
        }
    }

    protected DsColumnComposite columnComposite(Object value, boolean before) {
        DsColumn col = before ? beforeColumn(value) : afterColumn(value);
        DsColumnComposite composite = new DsColumnComposite();
        composite.add(col);
        return composite;
    }

    public GGInputBuilder withPrimaryKeyColumn(String columnName, String currentValue, boolean before) {
        this.columns.add(columnComposite(currentValue, before));
        ColumnMetaData columnMetaData = new ColumnMetaData(columnName, this.columnMetaData.size(), columnName.length(), (short)0, (short)DsType.GGType.GG_ASCII_V.getValue(), (short) DsType.GGSubType.GG_SUBTYPE_CHAR_TYPE.getValue(), (short)1, (short)1, currentKeyColumnIndex++, 0L, 0L, 0L, (short)0, (short)0);
        this.columnMetaData.add(columnMetaData);
        return this;
    }

    public GGInputBuilder withPrimaryKeyColumn(String columnName, String currentValue) {
        return this.withPrimaryKeyColumn(columnName, currentValue, false);
    }

    public GGInputBuilder withPrimaryKeyColumn(String columnName, Long currentValue, boolean before) {
        verifyNotCommitted();

        this.columns.add(columnComposite(currentValue, before));
        ColumnMetaData columnMetaData = new ColumnMetaData(
            columnName,
            this.columnMetaData.size(),
            columnName.length(),
            (short)0,
            (short)DsType.GGType.GG_64BIT_S.getValue(),
            (short)DsType.GGSubType.GGSubType_UNSET.getValue(),
            (short)1,
            (short)1,
            currentKeyColumnIndex++,
            0L,
            0L,
            0L,
            (short)0,
            (short)0
        );
        this.columnMetaData.add(columnMetaData);
        return this;
    }

    public GGInputBuilder withPrimaryKeyColumn(String columnName, Long currentValue) {
        return this.withPrimaryKeyColumn(columnName, currentValue, false);
    }


    public GGInputBuilder withColumn(String columnName, Integer currentValue) {
        verifyNotCommitted();
        this.columns.add(columnComposite(currentValue, false));
        ColumnMetaData columnMetaData = new ColumnMetaData(columnName, this.columnMetaData.size(), columnName.length(), (short)0, (short)DsType.GGType.GG_32BIT_S.getValue(), (short) DsType.GGSubType.GGSubType_UNSET.getValue(), (short)0, (short)0, (short)0, 0L, 0L, 0L, (short)0, (short)0);
        this.columnMetaData.add(columnMetaData);
        return this;
    }

    public GGInputBuilder withColumn(String columnName, String currentValue) {
        verifyNotCommitted();
        this.columns.add(columnComposite(currentValue, false));
        ColumnMetaData columnMetaData = new ColumnMetaData(columnName, this.columnMetaData.size(), columnName.length(), (short)0, (short)DsType.GGType.GG_ASCII_V.getValue(), (short) DsType.GGSubType.GG_SUBTYPE_CHAR_TYPE.getValue(), (short)0, (short)0, (short)0, 0L, 0L, 0L, (short)0, (short)0);
        this.columnMetaData.add(columnMetaData);
        return this;
    }

    public GGInputBuilder withColumn(String columnName, DateString currentValue) {
        verifyNotCommitted();
        this.columns.add(columnComposite(currentValue, false));
        ColumnMetaData columnMetaData = new ColumnMetaData(columnName, this.columnMetaData.size(), columnName.length(), (short)0, (short)DsType.GGType.GG_DATETIME.getValue(), (short) DsType.GGSubType.GG_SUBTYPE_DEFAULT.getValue(), (short)0, (short)0, (short)0, 0L, 0L, 0L, (short)0, (short)0);
        this.columnMetaData.add(columnMetaData);
        return this;
    }

    public GGInputBuilder withTimestampColumn(String columnName, String currentValue) {
        verifyNotCommitted();
        this.columns.add(columnComposite(currentValue, false));
        ColumnMetaData columnMetaData = new ColumnMetaData(columnName, this.columnMetaData.size(), columnName.length(), (short)0, (short)DsType.GGType.GG_DATETIME.getValue(), (short) DsType.GGSubType.GG_SUBTYPE_DEFAULT.getValue(), (short)0, (short)0, (short)0, 0L, 0L, 0L, (short)0, (short)0);
        this.columnMetaData.add(columnMetaData);
        return this;
    }

    public GGInputBuilder withColumn(String columnName, byte[] currentValue) {
        verifyNotCommitted();
        this.columns.add(columnComposite(currentValue, false));
        ColumnMetaData columnMetaData = new ColumnMetaData(columnName, this.columnMetaData.size(), columnName.length(), (short)0, (short)DsType.GGType.GGType_UNSET.getValue(), (short) DsType.GGSubType.GG_SUBTYPE_BINARY.getValue(), (short)0, (short)0, (short)0, 0L, 0L, 0L, (short)0, (short)0);
        this.columnMetaData.add(columnMetaData);
        return this;
    }

    public GGInputBuilder withColumn(String columnName, ZonedDateTime currentValue) {
        return this.withColumn(columnName, new DateString(currentValue));
    }







    public GGInputBuilder commit() throws IllegalStateException {
        verifyNotCommitted();
        List<String> errors = new ArrayList<>();

        if(this.schemaName == null) {
            errors.add("schemaName is missing");
        }

        if(this.tableName == null) {
            errors.add("tableName is missing");
        }

        if(!errors.isEmpty()) {
            throw new IllegalStateException(String.join(", ", errors));
        }

        TableName ggTableName = new TableName(this.schemaName + "." + this.tableName);
        TableMetaData tableMetaData = new TableMetaData(ggTableName, this.columnMetaData);
        DsMetaData dsMetaData = new DsMetaData();
        dsMetaData.setTableMetaData(tableMetaData);

        long i = 233;
        long j = 32323;
        GGTranID ggTranID = GGTranID.getID(i, j);
        DsTransaction dsTransaction = new DsTransaction(ggTranID);
        DsEvent dsEvent = new DsEventManager.TxEvent(dsTransaction, ggTranID, dsMetaData, "Sample Transaction");

        DataSourceConfig ds = new DataSourceConfig();
        DsMetric dms = new DsMetric();

        marklogicHandler.setHandlerMetric(dms);
        marklogicHandler.init(ds, dsMetaData);

        DsRecord dsRecord = new DsRecord(columns);

        DsOperation dsOperation = new DsOperation(
            ggTableName,
            tableMetaData,
            opType,
            new DateString(ZonedDateTime.now()),
            GGTranID.getID(0L, 0L),
            TxState.UNKNOWN,
            dsRecord,
            false,
            GGTranID.UNSET,
            null,
            Optional.ofNullable(this.scn).map(Object::toString).orElse(null),  // csn
            1,
            false,
            false
        );

        this.addStatus = marklogicHandler.operationAdded(dsEvent, dsTransaction, dsOperation);
        this.commitStatus = marklogicHandler.transactionCommit(dsEvent, dsTransaction);

        this.built = true;

        return this;
    }

    public GGDataSource.Status getAddStatus() {
        return addStatus;
    }

    public GGDataSource.Status getCommitStatus() {
        return commitStatus;
    }

    public MarkLogicHandler getMarklogicHandler() {
        return marklogicHandler;
    }
}
