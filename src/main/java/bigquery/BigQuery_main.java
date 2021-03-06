package bigquery;

import java.io.IOException;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.TableResult;

public class BigQuery_main {
	
	public static void main(String args[]) throws IOException {
    	
		int column = 2048, query_id = 1;
		BigQuery_lib blib = new BigQuery_lib("/Users/mizuno/Downloads/closedqueue-929a267e03b8.json", column, "mznfe", "Fe_OFF_1_99");
		
		//BigQueryに格納する場合、テーブルを作成する
		//blib.createTable();
		
		for(int i = 1; i <= 2048; i++) {
			TableResult response = blib.getTotal(i);
			/*  //MySQLに格納する場合
			//MySQL mysql = new MySQL(query_id, column);
			mysql.insertTotals(response, i);
			for (FieldValueList row : response.iterateAll()) {
        			for(int j = 0; j < column; j++) {
        				System.out.println("("+i+","+(j+1)+")sum"+j+": "+row.get("sum"+j).getValue());
        			}	
			}
			mysql.selectTotals();
			//MySQL格納ここまで 
			*/
			
			//BigQueryに格納する場合
			blib.insertTotals(response, i);
			System.out.println("完了 : "+ i +"/2048");
		}
    }
}
