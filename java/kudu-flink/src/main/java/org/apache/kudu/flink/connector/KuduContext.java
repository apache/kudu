package org.apache.kudu.flink.connector;

import org.apache.flink.util.Preconditions;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.client.AsyncKuduClient;
import org.apache.kudu.client.AsyncKuduSession;
import org.apache.kudu.client.DeleteTableResponse;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.PartialRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KuduContext implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(KuduContext.class);

    private transient KuduClient syncClient;
    private transient KuduTable table;

    private final KuduTableInfo tableInfo;

    public KuduContext(KuduTableInfo tableInfo) throws KuduException {
        this.tableInfo = Preconditions.checkNotNull(tableInfo,"kudu table info cannot be null");

        Preconditions.checkNotNull(tableInfo.getMaster(),"kudu master must be defined");

        this.syncClient = KuduConnection.getSyncClient(tableInfo.getMaster());

        synchronized (this.tableInfo) {
            if (tableExists()) {
                this.table = openTable();
            } else if (tableInfo.createIfNotExist()) {
                this.table = createTable();
            } else {
                throw new UnsupportedOperationException("table not exists and is marketed to not be created");
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (syncClient != null) {
            syncClient.close();
        }
    }

    protected boolean tableExists() throws KuduException {
        return syncClient.tableExists(tableInfo.getName());
    }

    protected KuduTable openTable() throws KuduException {
        return syncClient.openTable(tableInfo.getName());
    }

    protected KuduTable createTable() throws KuduException {
        return syncClient.createTable(tableInfo.getName(), tableInfo.getSchema(), tableInfo.getCreateTableOptions());
    }

    protected boolean deleteTable() throws KuduException {
        syncClient.deleteTable(tableInfo.getName());
        return true;
    }

    public boolean writeRow(KuduRow row) throws Exception {
        final Operation operation;

        if (tableInfo.isUpsertMode()) {
            operation = table.newUpsert();
        } else if (tableInfo.isInsertMode()) {
            operation = table.newInsert();
        } else if (tableInfo.isUpdateMode()) {
            operation = table.newUpdate();
        } else {
            throw new IllegalArgumentException("unknown table mode");
        }

        return writeRow(operation, row);
    }

    private boolean writeRow(Operation operation, KuduRow row) throws Exception {
        final PartialRow partialRow = operation.getRow();

        for(ColumnSchema column : operation.getTable().getSchema().getColumns()) {
            KuduUtils.addPartialRowValue(partialRow, column, row.getValue(column.getName()));
        }

        AsyncKuduSession session = table.getAsyncClient().newSession();
        session.apply(operation);
        session.flush();

        return session.close().addCallback(new KuduLogCallback()).join();

    }

}
