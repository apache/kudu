package org.apache.kudu.flink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Preconditions;
import org.apache.kudu.flink.connector.KuduContext;
import org.apache.kudu.flink.connector.KuduRow;
import org.apache.kudu.flink.connector.KuduTableInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KuduSink<T>  extends RichSinkFunction<T> {

    private static final Logger LOG = LoggerFactory.getLogger(KuduSink.class);

    private transient KuduContext context;
    private KuduTableInfo tableInfo;

    public KuduSink(KuduTableInfo tableInfo) throws Exception {
        Preconditions.checkNotNull(tableInfo,"tableInfo could not be null");
        this.tableInfo = tableInfo;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        context = new KuduContext(tableInfo);
    }

    public void close() throws Exception {
        if(context == null) return;
        this.context.close();
    }

    @Override
    public void invoke(T tuple) throws Exception {
        context.writeRow(new KuduRow(tuple));
    }

}
