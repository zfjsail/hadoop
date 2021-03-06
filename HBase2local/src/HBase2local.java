
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;


public class HBase2local {
	
	private static Configuration conf = null;
	static Table table = null;
	static Connection connection = null;
	static PrintWriter writer = null; 
	
	private static void setup() throws IOException {
		conf = HBaseConfiguration.create();
		connection = ConnectionFactory.createConnection(conf);
		table = connection.getTable(TableName.valueOf("Wuxia"));
		writer = new PrintWriter("r.txt", "UTF-8");
	}

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		setup();
		scan2local();
	}
	
	private static void scan2local() throws IOException {
		Scan scan = new Scan();
		ResultScanner scanner = table.getScanner(scan);
		for(Result r = scanner.next(); r != null; r = scanner.next()) {
			String word = Bytes.toString(r.getRow());
			String str_avg =Bytes.toString(CellUtil.cloneValue(r.listCells().get(0)));
			writer.println(word + "\t" + str_avg);
		}
		scanner.close();
		writer.close();
	}

}
