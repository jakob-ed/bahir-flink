package org.apache.flink.streaming.connectors.pinot.emulator;

import org.apache.flink.streaming.connectors.pinot.PinotControllerApi;
import org.apache.flink.streaming.connectors.pinot.exceptions.PinotControllerApiException;
import org.apache.pinot.client.*;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class PinotHelper extends PinotControllerApi {

    private static final Logger LOG = LoggerFactory.getLogger(PinotHelper.class);

    public PinotHelper(String controllerHost, String controllerPort) {
        super(controllerHost, controllerPort);
    }

    private void addSchema(Schema tableSchema) throws IOException {
        ApiResponse res = this.post("/schemas", JsonUtils.objectToString(tableSchema));
        LOG.info("Schema add request for schema {} returned {}", tableSchema.getSchemaName(), res.responseBody);
        if (res.statusLine.getStatusCode() != 200) {
            throw new PinotControllerApiException(res.responseBody);
        }
    }

    private void deleteSchema(Schema tableSchema) throws IOException {
        ApiResponse res = this.delete(String.format("/schemas/%s", tableSchema.getSchemaName()));
        LOG.info("Schema delete request for schema {} returned {}", tableSchema.getSchemaName(), res.responseBody);
        if (res.statusLine.getStatusCode() != 200) {
            throw new PinotControllerApiException(res.responseBody);
        }
    }

    private void addTable(TableConfig tableConfig) throws IOException {
        ApiResponse res = this.post("/tables", JsonUtils.objectToString(tableConfig));
        LOG.info("Table creation request for table {} returned {}", tableConfig.getTableName(), res.responseBody);
        if (res.statusLine.getStatusCode() != 200) {
            throw new PinotControllerApiException(res.responseBody);
        }
    }

    private void removeTable(TableConfig tableConfig) throws IOException {
        ApiResponse res = this.delete(String.format("/tables/%s", tableConfig.getTableName()));
        LOG.info("Table deletion request for table {} returned {}", tableConfig.getTableName(), res.responseBody);
        if (res.statusLine.getStatusCode() != 200) {
            throw new PinotControllerApiException(res.responseBody);
        }
    }

    public void createTable(TableConfig tableConfig, Schema tableSchema) throws IOException {
        this.addSchema(tableSchema);
        this.addTable(tableConfig);
    }

    public void deleteTable(TableConfig tableConfig, Schema tableSchema) throws IOException {
        this.removeTable(tableConfig);
        this.deleteSchema(tableSchema);
    }

    public ResultSet getTableEntries(String tableName) {
        String zkUrl = "localhost:2181";
        String pinotClusterName = "PinotCluster";
        Connection pinotConnection = ConnectionFactory.fromZookeeper(zkUrl + "/" + pinotClusterName);

        String query = String.format("SELECT * FROM %s", tableName);

        // set queryType=sql for querying the sql endpoint
        Request pinotClientRequest = new Request("sql", query);
        ResultSetGroup pinotResultSetGroup = pinotConnection.execute(pinotClientRequest);
        return pinotResultSetGroup.getResultSet(0);
    }
}
