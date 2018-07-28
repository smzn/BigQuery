package bigquery;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.TableResult;

public class MySQL {
	String driver;// JDBCドライバの登録
    String server, dbname, url, user, password;// データベースの指定
    Connection con;
    Statement stmt;
    ResultSet rs;
    private int query_id, column;
    
	public MySQL(int query_id, int column) {
		this.driver = "org.gjt.mm.mysql.Driver";
        this.server = "mznfe.sist.ac.jp";
        this.dbname = "mznfe";
        this.url = "jdbc:mysql://" + server + "/" + dbname + "?useUnicode=true&characterEncoding=UTF-8";
        this.user = "mznfe";
        this.password = "kansoukikashiteyo";
        this.query_id = query_id;
        this.column = column;
        try {
            this.con = DriverManager.getConnection(url, user, password);
            this.stmt = con.createStatement ();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            Class.forName (driver);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
	}
    
	public void insertTotals(TableResult response, int idx){
		System.out.println("Means : Insert開始");
		
		for (FieldValueList row : response.iterateAll()) {
			StringBuffer buf = new StringBuffer();
			buf.append("INSERT INTO totals(`query_id` ,  `row` ,  `column` ,  `value`) VALUES");
			for(int j = 0; j < column; j++) {
				if(j == column -1) buf.append("("+query_id+","+idx+","+(j+1)+","+row.get("sum"+j).getValue()+")");
				else buf.append("("+query_id+","+idx+","+(j+1)+","+row.get("sum"+j).getValue()+"),");
			}	
			String sql = buf.toString();
			System.out.println(sql);
            try {
				stmt.execute (sql);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
            System.out.println("Insert完了");
		}
		
	}
	
	
}
