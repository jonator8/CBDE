
// Standard classes
import java.io.IOException;
// HBase classes
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
// Hadoop classes
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class GroupBy extends Configured implements Tool {
  private static String inputTable;
  private static String outputTable;

  //=================================================================== Main
  /*
       Explanation: This MapReduce job either requires at input both family and column name defined (family:column),
  	or if only the column name is provided, it assumes that both family and column name are the same.
       For example, for a table with columns a, b and c, it would assume three families (a, b and c).

       This MapReduce takes three parameters:
       - Input HBase table from where to read data.
       - Output HBase table where to store data.
       - A list of [family:]columns to project.

  	We distinguish two following cases:
  	1) 	For example, assume the following HBase table UsernameInput (the corresponding shell create statement follows):
  		create 'UsernameInput', 'a', 'b' -- It contains two families: a and b
  		put 'UsernameInput', 'key1', 'a:a', '1' -- It creates an attribute a under the a family with value 1
  		put 'UsernameInput', 'key1', 'b:b', '2' -- It creates an attribute b under the b family with value 2

  		A correct call would be this: yarn jar myJarFile.jar Projection jonathan.nebot_ProjectionInput jonathan.nebot_Projection_Output a -- It projects a
  		The result (stored in UsernameOutput) would be: 'key1', 'a:a', '1' -- b is not there
  		Notice that in this case providing family name is optional.

  	2) 	However, assume the following case where HBase table is created as follows:
  		create 'UsernameInputF', 'cf1', 'cf2' -- It contains two families: cf1 and cf2
  		put 'UsernameInputF', 'key1', 'cf1:a', '1' -- It creates an attribute a under the cf1 family with value 1
  		put 'UsernameInputF', 'key1', 'cf2:b', '2' -- It creates an attribute b under the cf2 family with value 2

  		In this case, a correct call would require both family and column defined, as follows:
  		yarn jar myJarFile.jar Projection UsernameInputF UsernameOutputF cf1:a -- It projects cf1:a
  		The result (stored in UsernameOutputF) would be: 'key1', 'cf1:a', '1' -- cf2:b is not there
  		Notice that in this case providing family name is mandatory.

  */

  public static void main(String[] args) throws Exception {
    if (args.length != 4) {
      System.err.println("Parameters missing: 'inputTable outputTable aggregateAttribute groupByAttribute'");
      System.exit(1);
    }
    inputTable = args[0];
    outputTable = args[1];

    int tablesRight = checkIOTables(args);
    if (tablesRight == 0) {
      int ret = ToolRunner.run(new GroupBy(), args);
      System.exit(ret);
    } else {
      System.exit(tablesRight);
    }
  }

  //============================================================== checkTables
  private static int checkIOTables(String[] args) throws Exception {
    // Obtain HBase's configuration
    Configuration config = HBaseConfiguration.create();
    // Create an HBase administrator
    HBaseAdmin hba = new HBaseAdmin(config);

    // With an HBase administrator we check if the input table exists
    if (hba.tableExists(inputTable)) {
      hba.disableTable(inputTable);
      hba.deleteTable(inputTable);

    }

    //Create input table
    HTableDescriptor htdInput = new HTableDescriptor(inputTable.getBytes());
    htdInput.addFamily(new HColumnDescriptor("a"));
    htdInput.addFamily(new HColumnDescriptor("b"));
    hba.createTable(htdInput);

    // Check if the output table exists
    if (hba.tableExists(outputTable)) {
      hba.disableTable(outputTable);
      hba.deleteTable(outputTable);
    }
    // Create the columns of the output table
    HTableDescriptor htdOutput = new HTableDescriptor(outputTable.getBytes());
    htdOutput.addFamily(new HColumnDescriptor(args[2])); //aggregateAttribute
    hba.createTable(htdOutput);

    // If you want to insert data do it here
    // -- Inserts
    // -- Inserts
    HTable input = new HTable(config, inputTable);
    //Row1
    Put put = new Put(Bytes.toBytes("key1"));
    put.add(Bytes.toBytes("a"), Bytes.toBytes("a"), Bytes.toBytes("1"));
    put.add(Bytes.toBytes("b"), Bytes.toBytes("b"), Bytes.toBytes("100"));
    input.put(put);
    //Row2
    put = new Put(Bytes.toBytes("key2"));
    put.add(Bytes.toBytes("a"), Bytes.toBytes("a"), Bytes.toBytes("1"));
    put.add(Bytes.toBytes("b"), Bytes.toBytes("b"), Bytes.toBytes("100"));
    input.put(put);
    //Row3
    put = new Put(Bytes.toBytes("key3"));
    put.add(Bytes.toBytes("a"), Bytes.toBytes("a"), Bytes.toBytes("2"));
    put.add(Bytes.toBytes("b"), Bytes.toBytes("b"), Bytes.toBytes("100"));
    input.put(put);
    //Row4
    put = new Put(Bytes.toBytes("key4"));
    put.add(Bytes.toBytes("a"), Bytes.toBytes("a"), Bytes.toBytes("2"));
    put.add(Bytes.toBytes("b"), Bytes.toBytes("b"), Bytes.toBytes("100"));
    input.put(put);

    return 0;
  }

  //============================================================== Job config
  public int run(String[] args) throws Exception {
    //Create a new job to execute

    //Retrive the configuration
    Job job = new Job(HBaseConfiguration.create());
    //Set the MapReduce class
    job.setJarByClass(GroupBy.class);
    //Set the job name
    job.setJobName("Projection");
    //Create an scan object
    Scan scan = new Scan();
    //Set the columns to scan and keep header to project
    String header = "";
    for (int i = 2; i < args.length; i++) {
      String[] familyColumn = new String[2];
      if (!args[i].contains(":")) {
        //If only the column name is provided, it is assumed that both family and column names are the same
        familyColumn[0] = args[i];
        familyColumn[1] = args[i];
      } else {
        //Otherwise, we extract family and column names from the provided argument "family:column"
        familyColumn = args[i].split(":");
      }

      scan.addColumn(familyColumn[0].getBytes(), familyColumn[1].getBytes());
      header = header + "," + args[i];
    }
    job.getConfiguration().setStrings("attributes", header);
    //Set the Map and Reduce function
    TableMapReduceUtil.initTableMapperJob(inputTable, scan, Mapper.class, Text.class, Text.class, job);
    TableMapReduceUtil.initTableReducerJob(outputTable, Reducer.class, job);

    boolean success = job.waitForCompletion(true);
    return success ? 0 : 4;
  }

  //=================================================================== Mapper
  public static class Mapper extends TableMapper<Text, Text> {

    public void map(ImmutableBytesWritable rowMetadata, Result values, Context context)
        throws IOException, InterruptedException {
      String tuple = "";
      String rowId = new String(rowMetadata.get(), "US-ASCII");
      String[] attributes = context.getConfiguration().getStrings("attributes", "empty");

      String[] firstFamilyColumn = new String[2];
      if (!attributes[0].contains(":")) {
        //If only the column name is provided, it is assumed that both family and column names are the same
        firstFamilyColumn[0] = attributes[0];
        firstFamilyColumn[1] = attributes[0];
      } else {
        firstFamilyColumn = attributes[0].split(":");
      }
      tuple = new String(values.getValue(firstFamilyColumn[0].getBytes(), firstFamilyColumn[1].getBytes()));

      for (int i = 1; i < attributes.length; i++) {

        String[] familyColumn = new String[2];
        if (!attributes[i].contains(":")) {
          //If only the column name is provided, it is assumed that both family and column names are the same
          familyColumn[0] = attributes[i];
          familyColumn[1] = attributes[i];
        } else {
          //Otherwise, we extract family and column names from the provided argument "family:column"
          familyColumn = attributes[i].split(":");
        }

        tuple = tuple + ";" + new String(values.getValue(familyColumn[0].getBytes(), familyColumn[1].getBytes()));
      }

      context.write(new Text(tuple), new Text(rowId));
    }
  }

  //================================================================== Reducer
  public static class Reducer extends TableReducer<Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> inputList, Context context) throws IOException, InterruptedException {
      Text outputKey = inputList.iterator().next();

      // Create a tuple for the output table
      Put put = new Put(outputKey.getBytes());
      //Set the values for the columns
      String[] attributes = context.getConfiguration().getStrings("attributes", "empty");
      String[] values = key.toString().split(";");
      for (int i = 0; i < attributes.length; i++) {
        String[] familyColumn = new String[2];
        if (!attributes[i].contains(":")) {
          //If only the column name is provided, it is assumed that both family and column names are the same
          familyColumn[0] = attributes[i];
          familyColumn[1] = attributes[i];
        } else {
          //Otherwise, we extract family and column names from the provided argument "family:column"
          familyColumn = attributes[i].split(":");
        }

        put.add(familyColumn[0].getBytes(), familyColumn[1].getBytes(), values[i].getBytes());
      }
      // Put the tuple in the output table
      context.write(outputKey, put);
    }

  }
}
