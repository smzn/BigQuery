package bigquery;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.JobException;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TableResult;

public class BigQuery_lib {
	
	BigQuery bigQuery;
	private int column;
	private String datasetId;
	TableId tableId;
	
	//コンストラクタ
	public BigQuery_lib(String key, int column, String datasetId, String tableName) {
		this.column = column;
		try {
			this.bigQuery = getClientWithJsonKey(key);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		this.datasetId = datasetId;
		this.tableId = TableId.of(datasetId, tableName);
	}

	public TableResult getTotal(int idx) {
		TableResult response = null;
		StringBuilder buff = new StringBuilder();
		buff.append("select ");
		for(int i = 0; i < column; i++) {
			if(i == column - 1) buff.append("sum(int64_field_"+i+") as sum"+i+" ");
			else buff.append("sum(int64_field_"+i+") as sum"+i+",");
		}
		buff.append("from `mznfe.Fe_OFF_0000*` ");
		buff.append("where int64_field_2048 = "+idx);
		String query = buff.toString();
		System.out.println(query);
		QueryJobConfiguration queryConfig = QueryJobConfiguration.of(query);
	    try {
			response = bigQuery.query(queryConfig);
		} catch (JobException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return response;
	}
	
	public void createTable() { //BigQueryにテーブルを作成
		Field row[] = new Field[column+1];
	    for(int i = 0; i < column; i++) {
	    		row[i] = Field.of("sum"+i, LegacySQLTypeName.INTEGER);
	    }
	    row[column] = Field.of("index", LegacySQLTypeName.INTEGER);
	    Schema schema = Schema.of(row);
	    StandardTableDefinition tableDefinition = StandardTableDefinition.of(schema);
	    bigQuery.create(TableInfo.of(tableId, tableDefinition));
	}
	
	public void insertTotals(TableResult response, int index){
		
		System.out.println("Totals : Insert開始");
		for (FieldValueList row : response.iterateAll()) {
			Map<String, Object> rowvalue = new HashMap<>();
		    for(int i = 0; i < column; i++) {
		    		rowvalue.put("sum"+i, row.get("sum"+i).getValue());
		    }
		    rowvalue.put("index", index);
			InsertAllRequest insertRequest = InsertAllRequest.newBuilder(tableId).addRow(rowvalue).build();
			InsertAllResponse insertResponse = bigQuery.insertAll(insertRequest);
		}
	}

	//jsonファイル認証関数
	public BigQuery getClientWithJsonKey(String key) throws IOException {
        return BigQueryOptions.newBuilder()
                .setCredentials(ServiceAccountCredentials.fromStream(new FileInputStream(key)))
                .build()
                .getService();
    }

}
