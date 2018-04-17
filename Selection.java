
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
import org.apache.hadoop.hbase.KeyValue;
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

public class Selection extends Configured implements Tool {
  private static String inputTable;
  private static String outputTable;

  public static void main(String[] args) throws Exception {
    if (args.length != 4) {
      System.err.println("Parameters missing: 'inputTable outputTable [family:]attribute value'");
      System.exit(1);
    }
    inputTable = args[0];
    outputTable = args[1];

    int tablesRight = checkIOTables(args);
    if (tablesRight == 0) {
      int ret = ToolRunner.run(new Selection(), args);
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
    for (byte[] key : htdInput.getFamiliesKeys()) {
      htdOutput.addFamily(new HColumnDescriptor(key));
    }
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
    job.setJarByClass(Selection.class);
    //Set the job name
    job.setJobName("Selection");
    //Create an scan object
    Scan scan = new Scan();
    //Set the columns to scan and keep header to project
    String header = args[2] + "," + args[3];
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

      String famcol = attributes[0];
      String value = attributes[1];

      String[] familyColumn = new String[2];
      if (!famcol.contains(":")) {
        //If only the column name is provided, it is assumed that both family and column names are the same
        familyColumn[0] = famcol;
        familyColumn[1] = famcol;
      } else {
        //Otherwise, we extract family and column names from the provided argument "family:column"
        familyColumn = famcol.split(":");
      }
      
      String valueFromRow = new String(values.getValue(familyColumn[0].getBytes(), familyColumn[1].getBytes()));
      if (value.equals(valueFromRow)) {
        KeyValue[] valuesContent = values.raw();
        //Write content of the whole tuple
        tuple = new String(valuesContent[0].getFamily()) + ":" + new String(valuesContent[0].getQualifier()) + ":" + new String(valuesContent[0].getValue());
        for (int i = 1; i < valuesContent.length; i++) {
          tuple += ";" + new String(valuesContent[i].getFamily()) + ":" + new String(valuesContent[i].getQualifier()) + ":" + new String(valuesContent[i].getValue());
        }
        context.write(new Text(rowId), new Text(tuple));
      }
    }
  }

  //================================================================== Reducer
  public static class Reducer extends TableReducer<Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> inputList, Context context) throws IOException, InterruptedException {
      //for every tuple for the same key
      for (Text tuple : inputList) {
        Put put = new Put(key.getBytes());
        //for every value 
        for (String value : tuple.toString().split(";")) {
          String [] valuesContent = value.split(":");
          put.add(valuesContent[0].getBytes(), valuesContent[1].getBytes(), valuesContent[2].getBytes());
        }
        context.write(key,put);
      }
    }
  }
}
