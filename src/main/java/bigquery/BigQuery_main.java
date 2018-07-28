package bigquery;

import java.io.IOException;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.TableResult;

public class BigQuery_main {
	
	public static void main(String args[]) throws IOException {
    	
		int column = 10, query_id = 1;
		BigQuery_lib blib = new BigQuery_lib("/Users/mizuno/Downloads/closedqueue-929a267e03b8.json", column);
		
		for(int i = 1; i <= 3; i++) {
			TableResult response = blib.getTotal(i);
			MySQL mysql = new MySQL(query_id, column);
			mysql.insertMeans(response, i);
			for (FieldValueList row : response.iterateAll()) {
        			for(int j = 0; j < column; j++) {
        				System.out.println("("+i+","+j+")sum"+j+": "+row.get("sum"+j).getValue());
        			}	
			}
		}
    }
}
