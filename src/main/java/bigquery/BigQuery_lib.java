package bigquery;

import java.io.FileInputStream;
import java.io.IOException;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.JobException;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;

public class BigQuery_lib {
	
	BigQuery bigQuery;
	private int column;
	
	//コンストラクタ
	public BigQuery_lib(String key, int column) {
		this.column = column;
		try {
			this.bigQuery = getClientWithJsonKey(key);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public TableResult getTotal(int idx) {
		TableResult response = null;
		StringBuilder buff = new StringBuilder();
		buff.append("select ");
		for(int i = 0; i < column; i++) {
			if(i == column - 1) buff.append("sum(int64_field_"+i+") as sum"+i+" ");
			else buff.append("sum(int64_field_"+i+") as sum"+i+",");
		}
		buff.append("from `mznfe.Fe_OFF_00000*` ");
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

	//jsonファイル認証関数
	public BigQuery getClientWithJsonKey(String key) throws IOException {
        return BigQueryOptions.newBuilder()
                .setCredentials(ServiceAccountCredentials.fromStream(new FileInputStream(key)))
                .build()
                .getService();
    }

}
