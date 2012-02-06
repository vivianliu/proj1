/*
  CS 61C Project1: Small World

  Name:  Vivian Liu
  Login: cs61c-ec

  Name:  Henry Wang
  Login: cs61c-ey
 */


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.Math;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class SmallWorld {
    // Maximum dept for any breadth-first search
    public static final int MAX_ITERATIONS = 20;

    // Skeleton code uses this to share denom cmd-line arg across cluster
    public static final String DENOM_PATH = "denom.txt";

    // Example enumerated type, used by EValue and Counter example
    public static enum ValueUse {EDGE};

    // Used to store the start nodes randomly picked up 1.0/denom.
    public static Set StartNodes = new HashSet();

    public static boolean allTraversed;

    // Example writable type
    public static class EValue implements Writable {
        public ValueUse use;
        public long value;

        public EValue(ValueUse use, long value) {
            this.use = use;
            this.value = value;
        }

        public EValue() {
            this(ValueUse.EDGE, 0);
        }

        // Serializes object - needed for Writable
        public void write(DataOutput out) throws IOException {
            out.writeUTF(use.name());
            out.writeLong(value);
        }

        // Deserializes object - needed for Writable
        public void readFields(DataInput in) throws IOException {
            use = ValueUse.valueOf(in.readUTF());
            value = in.readLong();
        }

        public void set(ValueUse use, long value) {
            this.use = use;
            this.value = value;
        }

        public String toString() {
            return use.name() + ": " + value;
        }
    }


    /* This example mapper loads in all edges but only propagates a subset.
       You will need to modify this to propagate all edges, but it is 
       included to demonstate how to read & use the denom argument.         */
    public static class LoaderMap extends Mapper<LongWritable, LongWritable, LongWritable, LongWritable> {

        /* Will need to modify to not lose any edges. */
        @Override
        public void map(LongWritable key, LongWritable value, Context context)
                throws IOException, InterruptedException {

	    context.write(key, value);
            // Example of using a counter (counter tagged by EDGE)
            context.getCounter(ValueUse.EDGE).increment(1);
        }
    }

    public static class LoaderReduce extends Reducer<LongWritable, LongWritable, LongWritable, Text> {
        /** Actual reduce function.
         * 
         * @param key Word.
         * @param values Values for this word (partial counts).
         * @param context ReducerContext object for accessing output,
         *                configuration information, etc.
         */

        /* Setup is called automatically once per map task. This will
           read denom in from the DistributedCache, and it will be
           available to each call of map later on via the instance
           variable.                                                  */

	public long denom;

        @Override
        public void setup(Context context) {
            try {
//                Configuration conf = context.getConfiguration();
//                Path cachedDenomPath = DistributedCache.getLocalCacheFiles(conf)[0];
//                BufferedReader reader = new BufferedReader(
//				    new FileWriter(localDenomPath.toString()));
                BufferedReader reader = new BufferedReader(
				        new FileReader(DENOM_PATH));
                String denomStr = reader.readLine();
                reader.close();
                denom = Long.decode(denomStr);
            } catch (IOException ioe) {
                System.err.println("IOException reading denom from distributed cache");
                System.err.println(ioe.toString());
            }
        }


        @Override 
        public void reduce(LongWritable key, Iterable<LongWritable> values,
			   Context context)
	    throws IOException, InterruptedException {
	    StringBuilder toReturn = new StringBuilder();
	    boolean first = true;

	    long keyValue = key.get();
            // Send node forward only if part of random subset
            if (Math.random() < 1.0/denom) {
		StartNodes.add (keyValue);
            }

	    // Propagate all nodes to BFS mapper.  Append distance from start
	    // and mark for traversal.
	    for (LongWritable value : values) {
		if (!first) {
		    toReturn.append(",");
		}
		first=false;
		// append the original reachable list
		toReturn.append(value.toString());
	    }
	    if (StartNodes.contains(key.get())) {
		toReturn.append(" T,0,");
		toReturn.append(key.get());
	    }
     
	    //System.out.println("==== Loaderreduce " + key + " " + toReturn.toString());
	    context.write(key, new Text(toReturn.toString()));
	}
    }

    /******** BFS mapper ********/
    public static class BFSMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
	@Override
	public void map(LongWritable key, Text value, Context context)
	    throws IOException, InterruptedException {
	    StringBuilder toReturn = new StringBuilder();
	    StringBuilder updatedValue = new StringBuilder();
	    boolean first = true;

	    String line = value.toString();
	    // list[0] is always the reachable list. list[1..n] is a triplet.
	    String[] list = line.split(" ");

	    updatedValue.append(list[0]);
	    updatedValue.append(" ");

	    for (int x = 1; x < list.length; x++) {
		String[] kv = list[x].split(",");
		if (kv[0].equals("T")) {
		    String[] reachable = list[0].split(",");
		    for (int y = 0; y < reachable.length; y++) {
			long reach = Integer.parseInt(reachable[y]);
			int dist = Integer.parseInt(kv[1]) + 1;
			toReturn.append("-1 T,");
			toReturn.append(dist);
			toReturn.append(",");
			toReturn.append(kv[2]);
			context.write(new LongWritable (reach), new Text(toReturn.toString()));
			toReturn = new StringBuilder();
			allTraversed = false;
		    }
		    updatedValue.append("V,");
		    updatedValue.append(kv[1]);
		    updatedValue.append(",");
		    updatedValue.append(kv[2]);
		    updatedValue.append(" ");
		}
		else {
		    updatedValue.append(list[x]);
		    updatedValue.append(" ");
		}
	    }
	    // Propagate the original.
	    context.write(key, new Text(updatedValue.toString()));
	    //System.out.println("==== BFSMap " + key + " " + updatedValue);
        }
    }

    /******** BFS reducer ********/
    public static class BFSReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
	@Override 
	public void reduce(LongWritable key, Iterable<Text> values,
			   Context context)
	    throws IOException, InterruptedException {
	    String reachable = new String("");
	    ArrayList<Long> traverseDistArray = new ArrayList<Long>();
	    ArrayList<Long> traverseStartArray = new ArrayList<Long>();
	    ArrayList<Long> visitedDistArray = new ArrayList<Long>();
	    ArrayList<Long> visitedStartArray = new ArrayList<Long>();
	    ArrayList<Long> visitedTagArray = new ArrayList<Long>();
	    StringBuilder toReturn = new StringBuilder();
	    for (Text value : values) {
		String line = value.toString();
		String[] list = line.split(" ");
		if (list[0].equals("-1")) {
		    // traverse request
		    String[] kv = list[1].split(",");
		    long dist = Integer.parseInt(kv[1]);
		    long start = Integer.parseInt(kv[2]);
		    traverseDistArray.add(dist);
		    traverseStartArray.add(start);
		}
		else {
		    // Decompose the regular reachable list and dist,start pairs
		    reachable = list[0];
		    for (int x = 1; x < list.length; x++) {
			String[] kv = list[x].split(",");
			Long dist = Long.parseLong(kv[1]);
			Long start = Long.parseLong(kv[2]);
			visitedDistArray.add(dist);
			visitedStartArray.add(start);
			visitedTagArray.add(new Long(0));
		    }
		}
	    }
	    for (int x = 0; x < traverseStartArray.size(); x++) {
		boolean found = false;
		for (int y = 0; y < visitedStartArray.size(); y++) {
		    if (visitedStartArray.get(y) == traverseStartArray.get(x)) {
			found = true;
			break;
		    }
		}
		if (!found) {
		    // Insert the dist,start to visited list
		    visitedDistArray.add(traverseDistArray.get(x));
		    visitedStartArray.add(traverseStartArray.get(x));
		    visitedTagArray.add(new Long(1));
		}
	    }
	    // append reachable first
	    toReturn.append(reachable);
	    // append all dist,start pairs
	    for (int y = 0; y < visitedStartArray.size(); y++) {
		toReturn.append(" ");
		if (visitedTagArray.get(y) == 1) {
		    toReturn.append("T,");
		}
		else
		    toReturn.append("V,");
		toReturn.append(visitedDistArray.get(y));
		toReturn.append(",");
		toReturn.append(visitedStartArray.get(y));
	    }
	    context.write(key, new Text(toReturn.toString()));
	    //System.out.println("==== BFSReduce " + key + " " + toReturn.toString());
	}
    }

    /******** Histogram mapper ********/
    public static class HistMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
	// Input format:
	//	key: Source node
	//	value: "reachable V,dist1,start1 V,dist2,start2 ..."
	// Output format:
	//	(Dist, Source node)
	//	...
	@Override
	public void map(LongWritable key, Text value, Context context)
	    throws IOException, InterruptedException {

	    String line = value.toString();
	    // list[0] is always the reachable list. list[1..n] is a triplet.
	    String[] list = line.split(" ");

	    for (int x = 1; x < list.length; x++) {
		String[] kv = list[x].split(",");
		if (kv[0].equals("T")) {
		    // Skip any non-traversed kv pair.  The distance
		    // is not accurate
		    continue;
		}
		// output is (dist sourceNode) pair
		int dist = Integer.parseInt(kv[1]);
		int sourceNode = Integer.parseInt(kv[2]);
		context.write(new LongWritable(dist), new LongWritable(sourceNode));
	       
		//System.out.println("==== HistMap " + dist + " " + sourceNode);
	    }
	}
    }

    /******** Histogram reducer ********/
    public static class HistReducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
	@Override 
	public void reduce(LongWritable key, Iterable<LongWritable> values,
			   Context context)
	    throws IOException, InterruptedException {
	    long sum = 0;

	    for (LongWritable value : values) {
		sum++;
	    }
	    context.write(key, new LongWritable (sum));
	    //System.out.println("==== HistReduce final " + key + " " + sum);
	}
    }

    // Shares denom argument across the cluster via DistributedCache
    public static void shareDenom(String denomStr, Configuration conf) {
        try {
	    Path localDenomPath = new Path(DENOM_PATH + "-source");
	    Path remoteDenomPath = new Path(DENOM_PATH);
	    BufferedWriter writer = new BufferedWriter(
				    new FileWriter(localDenomPath.toString()));
	    writer.write(denomStr);
	    writer.newLine();
	    writer.close();
	    FileSystem fs = FileSystem.get(conf);
	    fs.copyFromLocalFile(true,true,localDenomPath,remoteDenomPath);
	    DistributedCache.addCacheFile(remoteDenomPath.toUri(), conf);
        } catch (IOException ioe) {
            System.err.println("IOException writing to distributed cache");
            System.err.println(ioe.toString());
        }
    }


    public static void main(String[] rawArgs) throws Exception {
        GenericOptionsParser parser = new GenericOptionsParser(rawArgs);
        Configuration conf = parser.getConfiguration();
        String[] args = parser.getRemainingArgs();

        // Set denom from command line arguments
        shareDenom(args[2], conf);

        // Setting up mapreduce job to load in graph
        Job job = new Job(conf, "load graph");
        job.setJarByClass(SmallWorld.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(LoaderMap.class);
        job.setReducerClass(LoaderReduce.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        // Input from command-line argument, output to predictable place
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path("bfs-0-out"));

        // Actually starts job, and waits for it to finish
        job.waitForCompletion(true);

        // Example of reading a counter
        System.out.println("==== Read in " + 
                   job.getCounters().findCounter(ValueUse.EDGE).getValue() + 
                           " edges");

	// Display start node list
	Iterator it = StartNodes.iterator();
	while(it.hasNext()) {
	    System.out.println("==== Start node :"+it.next());
	}

        // Repeats your BFS mapreduce
        int i=0;
        // Will need to change terminating conditions to respond to data
        while (i<MAX_ITERATIONS) {
	    System.out.println("==== Iteration " + i);
            job = new Job(conf, "bfs" + i);
            job.setJarByClass(SmallWorld.class);

            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(Text.class);

            job.setMapperClass(BFSMapper.class);
            job.setReducerClass(BFSReducer.class);

            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);

            // Notice how each mapreduce job gets gets its own output dir
            FileInputFormat.addInputPath(job, new Path("bfs-" + i + "-out"));
            FileOutputFormat.setOutputPath(job, new Path("bfs-"+ (i+1) +"-out"));

	    allTraversed = true;
            job.waitForCompletion(true);
	    if (allTraversed)
		break;
            i++;
        }
	System.out.println("==== BFS done " + i);

        // Mapreduce config for histogram computation
        job = new Job(conf, "hist");
        job.setJarByClass(SmallWorld.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);

        job.setMapperClass(HistMapper.class);
//        job.setCombinerClass(Reducer.class);
        job.setReducerClass(HistReducer.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // By declaring i above outside of loop conditions, can use it
        // here to get last bfs output to be input to histogram
        FileInputFormat.addInputPath(job, new Path("bfs-"+ i +"-out"));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
