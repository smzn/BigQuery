package bigquery;

import java.io.FileInputStream;
import java.io.IOException;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.JobException;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;

public class BigQuery_main {
	
    public static void main(String args[]) throws IOException {
        BigQuery bigQuery = getClientWithJsonKey("/Users/mizuno/Downloads/closedqueue-929a267e03b8.json");
        
        String query = "select sum(int64_field_0) as sum0, sum(int64_field_1) as sum1,sum(int64_field_2) as sum2 from `mznfe.Fe_OFF_00000*` where int64_field_2048 = 1";
        QueryJobConfiguration queryConfig = QueryJobConfiguration.of(query);
        TableResult response = null;
		try {
			response = bigQuery.query(queryConfig);
		} catch (JobException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        for (FieldValueList row : response.iterateAll()) {
            System.out.println(row.get("sum0").getValue() + " " + row.get("sum1").getValue() + " " + row.get("sum2").getValue());
        }
    }
    
    private static BigQuery getClientWithJsonKey(String key) throws IOException {
        return BigQueryOptions.newBuilder()
                .setCredentials(ServiceAccountCredentials.fromStream(new FileInputStream(key)))
                .build()
                .getService();
    }


}
