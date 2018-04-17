
// Standard classes
import java.io.IOException;
import java.util.Vector;
import java.util.ArrayList;

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
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.TableName;

// Hadoop classes
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Join extends Configured implements Tool {

  // These are three global variables, which coincide with the three parameters in the call
  public static String inputTable1;
  public static String inputTable2;
  private static String outputTable;

  //=================================================================== Main

  public static void main(String[] args) throws Exception {
    if (args.length != 5) {
      System.err.println("Parameters missing: 'leftInputTable rightInputTable outputTable leftAttribute rightAttribute'");
      System.exit(1);
    }
    inputTable1 = args[0];
    inputTable2 = args[1];
    outputTable = args[2];

    int tablesRight = checkIOTables(args);
    if (tablesRight == 0) {
      int ret = ToolRunner.run(new Join(), args);
      System.exit(ret);
    } else {
      System.exit(tablesRight);
    }
  }

  //============================================================== checkTables

  // Explanation: Handles HBase and its relationship with the MapReduce task

  private static int checkIOTables(String[] args) throws Exception {
    // Obtain HBase's configuration
    Configuration config = HBaseConfiguration.create();
    // Create an HBase administrator
    HBaseAdmin hba = new HBaseAdmin(config);

    /* With an HBase administrator we check if the input tables exist.
       You can modify this bit and create it here if you want. */
    if (hba.tableExists(inputTable1)) {
      hba.disableTable(inputTable1);
      hba.deleteTable(inputTable1);
    }
    if (hba.tableExists(inputTable2)) {
      hba.disableTable(inputTable2);
      hba.deleteTable(inputTable2);
    }
    HTableDescriptor htdInput1 = new HTableDescriptor(inputTable1.getBytes());
    HTableDescriptor htdInput2 = new HTableDescriptor(inputTable2.getBytes());
    htdInput1.addFamily(new HColumnDescriptor("a"));
    htdInput1.addFamily(new HColumnDescriptor("b"));
    htdInput2.addFamily(new HColumnDescriptor("c"));
    htdInput2.addFamily(new HColumnDescriptor("d"));
    hba.createTable(htdInput1);
    hba.createTable(htdInput2);

    /* Check whether the output table exists.
       Just the opposite as before, we do create the output table.
       Normally, MapReduce tasks attack an existing table (not created on the fly) and
       they store the result in a new table. */
    if (hba.tableExists(outputTable)) {
      hba.disableTable(outputTable);
      hba.deleteTable(outputTable);
    }
    // If you want to insert data through the API, do it here
    // -- Inserts
      HTable input1 = new HTable(config, inputTable1);
      HTable input2 = new HTable(config, inputTable2);
      //You may not need this, but if batch loading, set the recovery manager to Not Force (setAutoFlush false) and deactivate the WAL
      //table.setAutoFlush(false); --This is needed to avoid HBase to flush at every put (instead, it will buffer them and flush when told by using FlushCommits)
      //put.setWriteToWAL(false); --This is to deactivate the Write Ahead Logging

      //Data inputTable1
      Put put = new Put(Bytes.toBytes("key1"));
      put.add(Bytes.toBytes("a"), Bytes.toBytes("a"), Bytes.toBytes("1"));
      put.add(Bytes.toBytes("b"), Bytes.toBytes("b"), Bytes.toBytes("2"));
      input1.put(put);
      put = new Put(Bytes.toBytes("key2"));
      put.add(Bytes.toBytes("a"), Bytes.toBytes("a"), Bytes.toBytes("1"));
      put.add(Bytes.toBytes("b"), Bytes.toBytes("b"), Bytes.toBytes("2"));
      input1.put(put);
      //Data inputTable2
      put = new Put(Bytes.toBytes("key3"));
      put.add(Bytes.toBytes("c"), Bytes.toBytes("c"), Bytes.toBytes("2"));
      put.add(Bytes.toBytes("d"), Bytes.toBytes("d"), Bytes.toBytes("3"));
      input2.put(put);
      put = new Put(Bytes.toBytes("key4"));
      put.add(Bytes.toBytes("c"), Bytes.toBytes("c"), Bytes.toBytes("3"));
      put.add(Bytes.toBytes("d"), Bytes.toBytes("d"), Bytes.toBytes("4"));
      input2.put(put);

      //If you do it through the HBase Shell, this is equivalent to:
      //put 'Username_InputTable1', 'key1', 'Family:Attribute', 'Value'
    // -- Inserts
    // Get an HBase descriptors pointing at the input tables (the HBase descriptors handle the tables' metadata)
    //HTableDescriptor htdInput1 = hba.getTableDescriptor(inputTable1.getBytes());
    //HTableDescriptor htdInput2 = hba.getTableDescriptor(inputTable2.getBytes());

    // Get an HBase descriptor pointing at the new table
    HTableDescriptor htdOutput = new HTableDescriptor(outputTable.getBytes());

    // We copy the structure of the input tables in the output one by adding the input columns to the new table
    for (byte[] key : htdInput1.getFamiliesKeys()) {
      System.out.println("family-t1 = " + new String(key));
      htdOutput.addFamily(new HColumnDescriptor(key));
    }
    for (byte[] key : htdInput2.getFamiliesKeys()) {
      System.out.println("family-t2 = " + new String(key));
      htdOutput.addFamily(new HColumnDescriptor(key));
    }

    //Create the new output table based on the descriptor we have been configuring
    hba.createTable(htdOutput);
    return 0;
  }

  //============================================================== Job config
  //Create a new job to execute. This is called from the main and starts the MapReduce job
  public int run(String[] args) throws Exception {

    //Create a new MapReduce configuration object.
    Job job = new Job(HBaseConfiguration.create());
    //Set the MapReduce class
    job.setJarByClass(Join.class);
    //Set the job name
    job.setJobName("Join");
    // To pass parameters to the mapper and reducer we must use the setStrings of the Configuration object
    // We pass the names of two input tables as External and Internal tables of the Cartesian product, and a hash random value.
    job.getConfiguration().setStrings("LeftTable", inputTable1);
    job.getConfiguration().setStrings("RightTable", inputTable2);
    job.getConfiguration().setStrings("LeftAttribute", args[3]);
    job.getConfiguration().setStrings("RightAttribute", args[4]);

    /* Set the Map and Reduce function:
       These are special mapper and reducers, which are prepared to read and store data on HBase tables
    */

    // To initialize the mapper, we need to provide two Scan objects (ArrayList of two Scan objects) for two input tables, as follows.
    ArrayList scans = new ArrayList();

    Scan scan1 = new Scan();
    System.out.println("inputTable1: " + inputTable1);

    scan1.setAttribute("scan.attributes.table.name", Bytes.toBytes(inputTable1));
    scans.add(scan1);

    Scan scan2 = new Scan();
    System.err.println("inputTable2: " + inputTable2);
    scan2.setAttribute("scan.attributes.table.name", Bytes.toBytes(inputTable2));

    scans.add(scan2);

    TableMapReduceUtil.initTableMapperJob(scans, Mapper.class, Text.class, Text.class, job);
    TableMapReduceUtil.initTableReducerJob(outputTable, Reducer.class, job);

    //Then we wait until the MapReduce task finishes? (block the program)
    boolean success = job.waitForCompletion(true);
    return success ? 0 : 1;
  }

  //=================================================================== Mapper

  /* This is the mapper class, which implements the map method.
     The MapReduce framework will call this method automatically when needed.
     Note its header defines the input key-value and an object where to store the resulting key-values produced in the map.
     - ImmutableBytesWritable rowMetadata: The input key
     - Result values: The input value associated to that key
     - Context context: The object where to store all the key-values generated (i.e., the map output) */

  public static class Mapper extends TableMapper<Text, Text> {

    public static final int HASH = 10;

    public void map(ImmutableBytesWritable rowMetadata, Result values, Context context) throws IOException, InterruptedException {

      int i;

      String[] leftTable = context.getConfiguration().getStrings("LeftTable", "Default");
      String[] rightTable = context.getConfiguration().getStrings("RightTable", "Default");

      // From the context object we obtain the input TableSplit this row belongs to
      TableSplit currentSplit = (TableSplit) context.getInputSplit();
      /*
        From the TableSplit object, we can further extract the name of the table that the split belongs to.
        We use the extracted table name to distinguish between external and internal tables as explained below.
      */
      TableName tableNameB = currentSplit.getTable();
      String tableName = tableNameB.getQualifierAsString();

      // We create a string as follows for each key: tableName#key;family:attributeValue
      String tuple = tableName + "#" + new String(rowMetadata.get(), "US-ASCII");

      KeyValue[] attributes = values.raw();
      for (i = 0; i < attributes.length; i++) {
        tuple = tuple + ";" + new String(attributes[i].getFamily()) + ":" + new String(attributes[i].getQualifier())
            + ":" + new String(attributes[i].getValue());
      }

      //Is this key external (e.g., from the external table)?
      if (tableName.equalsIgnoreCase(leftTable[0])) {
        //This writes a key-value pair to the context object
        //If it is external, it gets as key a hash value and it is written only once in the context object
        context.write(new Text(Integer.toString(Double.valueOf(Math.random() * HASH).intValue())), new Text(tuple));
      }
      //Is this key internal (e.g., from the internal table)?
      //If it is internal, it is written to the context object many times, each time having as key one of the potential hash values
      if (tableName.equalsIgnoreCase(rightTable[0])) {
        for (i = 0; i < HASH; i++) {
          context.write(new Text(Integer.toString(i)), new Text(tuple));
        }
      }
    }
  }

  //================================================================== Reducer
  public static class Reducer extends TableReducer<Text, Text, Text> {

    /* The reduce is automatically called by the MapReduce framework after the Merge Sort step.
    It receives a key, a list of values for that key, and a context object where to write the resulting key-value pairs. */

    public void reduce(Text key, Iterable<Text> inputList, Context context) throws IOException, InterruptedException {
      int i, j, k;
      Put put;
      String eTableTuple, iTableTuple;
      String eTuple, iTuple;
      String outputKey;
      String[] leftTable = context.getConfiguration().getStrings("LeftTable", "Default");
      String[] rightTable = context.getConfiguration().getStrings("RightTable", "Default");
      String[] leftAttribute = context.getConfiguration().getStrings("LeftAttribute", "empty");
      String[] rightAttribute = context.getConfiguration().getStrings("RightAttribute", "empty");
      String[] eAttributes, iAttributes;
      String[] attribute_value;
      String leftValue = "";
      String rightValue = "";

      //All tuples with the same hash value are stored in a vector
      Vector<String> tuples = new Vector<String>();
      for (Text val : inputList) {
        tuples.add(val.toString());
      }

      //In this for, each internal tuple is joined with each external tuple
      //Since the result must be stored in a HBase table, we configure a new Put, fill it with the joined data and write it in the context object
      for (i = 0; i < tuples.size(); i++) {
        eTableTuple = tuples.get(i);
        // we extract the information from the tuple as we packed it in the mapper
        eTuple = eTableTuple.split("#")[1];
        eAttributes = eTuple.split(";");
        if (eTableTuple.startsWith(leftTable[0])) {
          for (j = 0; j < tuples.size(); j++) {
            iTableTuple = tuples.get(j);
            // we extract the information from the tuple as we packed it in the mapper
            iTuple = iTableTuple.split("#")[1];
            iAttributes = iTuple.split(";");
            if (iTableTuple.startsWith(rightTable[0])) {
              // Create a key for the output
              outputKey = eAttributes[0] + "_" + iAttributes[0];
              // Create a tuple for the output table
              put = new Put(outputKey.getBytes());
              //Set the values for the columns of the external table
              for (k = 1; k < eAttributes.length; k++) {
                attribute_value = eAttributes[k].split(":");
                //If the column name is the correct, store the value
                if (attribute_value[1].equals(leftAttribute[0])) {
                  leftValue = attribute_value[2];
                }
                put.addColumn(attribute_value[0].getBytes(), attribute_value[1].getBytes(),
                    attribute_value[2].getBytes());
              }
              //Set the values for the columns of the internal table
              for (k = 1; k < iAttributes.length; k++) {
                attribute_value = iAttributes[k].split(":");
                //If the column name is the correct, store the value
                if (attribute_value[1].equals(rightAttribute[0])) {
                  rightValue = attribute_value[2];
                }
                put.addColumn(attribute_value[0].getBytes(), attribute_value[1].getBytes(),
                    attribute_value[2].getBytes());
              }
              // Put the tuple in the output table through the context object
              if (!leftValue.isEmpty() && !rightValue.isEmpty() && leftValue.equals(rightValue)) {
                context.write(new Text(outputKey), put);
              }
            }
          }
        }
      }
    }
  }
}
