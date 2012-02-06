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

    private static final boolean useDebugData = false;
    private static boolean debugDataSet = false;
    private static final boolean debug = false;

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
        /* Will need to modify to not loose any edges. */
        @Override
        public void map(LongWritable key, LongWritable value, Context context)
                throws IOException, InterruptedException {
	    StringBuilder toReturn = new StringBuilder();
	    if (useDebugData) {
		if (debugDataSet)
		    return;
		debugDataSet = true;
		class link {
		    public int src;
		    public int dest;
		    public link(int s, int d) {
			src = s;
			dest = d;
		    }
		};
		link[] debugList = new link[12];
		debugList[0] = new link(1, 0);
		debugList[1] = new link(4, 1);
		debugList[2] = new link(1, 4);
		debugList[3] = new link(4, 7);
		debugList[4] = new link(1, 7);
		debugList[5] = new link(6, 0);
		debugList[6] = new link(7, 1);
		debugList[7] = new link(6, 7);
		debugList[8] = new link(7, 4);
		debugList[9] = new link(7, 6);
		debugList[10] = new link(0, 6);
		debugList[11] = new link(5, 4);
		for (int x = 0; x < debugList.length; x++) {
		    System.out.println("==== Loadermap " + debugList[x].src + " " + debugList[x].dest);
		    context.write(new LongWritable(debugList[x].src), new LongWritable(debugList[x].dest));
		    context.getCounter(ValueUse.EDGE).increment(1);
		}
		StartNodes.add (0L);
		StartNodes.add (4L);
		return;
	    }
	    if (debug)
		System.out.println("==== Loadermap " + key + " " + value);
	    context.write(key, value);
	    // Example of using a counter (counter tagged by EDGE)
	    context.getCounter(ValueUse.EDGE).increment(1);
        }
    }

    public static class LoaderReduce extends Reducer<LongWritable, LongWritable, LongWritable, Text> {
        public static long denom;

        /* Setup is called automatically once per map task. This will
           read denom in from the DistributedCache, and it will be
           available to each call of map later on via the instance
           variable.                                                  */
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

        /** Actual reduce function.
         * 
         * @param key Word.
         * @param values Values for this word (partial counts).
         * @param context ReducerContext object for accessing output,
         *                configuration information, etc.
         */
        @Override 
        public void reduce(LongWritable key, Iterable<LongWritable> values,
			   Context context)
	    throws IOException, InterruptedException {
	    StringBuilder toReturn = new StringBuilder();
	    boolean first = true;

            if (!useDebugData && Math.random() < 1.0/denom) {
		StartNodes.add (key.get());
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
	    if (debug)
		System.out.println("==== Loaderreduce " + key + " " + toReturn.toString());
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
	    String[] reachable = null;
	    boolean doSplitReachable = true;

	    String line = value.toString();
	    int pos = 0, ptr;
	    while ((ptr = line.indexOf('T', pos)) >= 0) {
		if (doSplitReachable) {
		    reachable = line.substring(0, line.indexOf(' ',0)).split(",");
		    doSplitReachable = false;
		}
		// ptr points to "T,dist,start" get dist and start node
		int ptr2 = line.indexOf(',',ptr+2);
		int dist = Integer.parseInt(line.substring(ptr+2,ptr2)) + 1;
		int ptr3 = line.indexOf(' ',ptr2+1);
		int start;
		if (ptr3 >= 0)
		    start = Integer.parseInt(line.substring(ptr2+1, ptr3));
		else
		    start = Integer.parseInt(line.substring(ptr2+1));
		// Do real traverse, emit the traverse
		for (int y = 0; y < reachable.length; y++) {
		    long reach = Integer.parseInt(reachable[y]);
		    toReturn.append("-1 T,");
		    toReturn.append(dist);
		    toReturn.append(",");
		    toReturn.append(start);
		    context.write(new LongWritable (reach), new Text(toReturn.toString()));
		    if (debug)
			System.out.println("==== BFSMap " + reach + " " + toReturn);
		    toReturn = new StringBuilder();
		    // Mark allTraversed false since more traverse is needed
		    allTraversed = false;
		}
		// Continue to search for the next (T,dist,start)
		pos = ptr + 1;
	    }
	    
	    context.write(key, new Text(line.replace('T', 'V')));
	    if (debug)
		System.out.println("==== BFSMap " + key + " " + line.replace('T', 'V'));
        }
    }

    /******** BFS reducer ********/
    public static class BFSReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
	@Override 
	public void reduce(LongWritable key, Iterable<Text> values,
			   Context context)
	    throws IOException, InterruptedException {
	    StringBuilder toReturn = new StringBuilder();
	    ArrayList<String> newTravArray = new ArrayList<String>();
	    // Two passes:
	    // First pass finds the value without -1. It includes
	    // the links and visited V,dist,start list
	    // Second pass has to go through all -1 items and check
	    // if same start node already reach this node.  Append
	    // the T,dist,start only if the same start node appear.
	    for (Text value : values) {
		String line = value.toString();
		// Check if the first character is - "-1"
		if (line.charAt(0) == '-') {
		    newTravArray.add(line);
		    continue;
		}
		toReturn.append(line);
	    }
	    for (int x = 0; x < newTravArray.size(); x++) {
		boolean found = false;
		String line = newTravArray.get(x);
		// Check if the first character is - (-1 in the front)
		if (line.charAt(0) == '-') {
		    int pos, ptr, ptr2, length;
		    String startToken;
		    // 'start' node is the number after second ','
		    ptr = line.indexOf(',', 0);
		    ptr2 = line.indexOf(',', ptr+1);
		    startToken = line.substring(ptr2);
		    int startTokenLength = startToken.length();
		    // Check if startToken is in the visited start node list
		    pos = 0;
		    while ((ptr = toReturn.indexOf(startToken, pos)) >= 0) {
			// Since some node name is a substring of another,
			// we have to make sure it matches exactly
			if ((ptr + startTokenLength) >= toReturn.length() ||
			    toReturn.charAt(ptr + startTokenLength) == ' ') {
			    // do not add it since the same start node exists
			    found = true;
			    break;
			}
			pos = ptr + 1;
		    }
		    if (!found) {
			// Append T,dist,start to the visited string
			toReturn.append(" ");
			toReturn.append(line.substring(3));
		    }
		}
	    }
	    context.write(key, new Text(toReturn.toString()));
	    if (debug)
		System.out.println("==== BFSReduce " + key + " " + toReturn.toString());
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
		if (debug)
		    System.out.println("==== HistMap " + dist + " " + sourceNode);
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
	    if (debug)
		System.out.println("==== HistReduce final " + key + " " + sum);
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
        job.setCombinerClass(Reducer.class);
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
