import java.io.* ;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.Counter;
import  org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;

/***
 * Using counter mechanism , this program computes  distance of each node in the graph from given starting node.  This program terminates 
 * when there  is no change is noticed. That means distance of each of accessible node from starting node has been computed. 
 * **/
public class GraphPath
{
    public enum StateCounter {TOTAL_STATE_CHANGE};
    public static class Node
    {
        private String id ;
        private String neighbours ;
        private int distance ;
        private String state ;
        Node( Text t)
        {
            String[] parts = t.toString().split("\t") ;
            this.id = parts[0] ;
            this.neighbours = parts[1] ;
            if (parts.length<3 || parts[2].equals(""))
                this.distance = -1 ;
            else
                this.distance = Integer.parseInt(parts[2]) ;
            if (parts.length< 4 || parts[3].equals(""))
                this.state = "P" ;
            else
                this.state = parts[3] ;
        }
        Node(Text key, Text value)
        {
            this(new Text(key.toString()+"\t"+value.toString())) ;
        }
        public String getId()
        {return this.id ;
        }
        public String getNeighbours()
        {
            return this.neighbours ;
        }
        public int getDistance()
        {
            return this.distance ;
        }
        public String getState()
        {
            return this.state ;
        }
    }


    public static class GraphPathMapper extends Mapper<Object, Text, Text, Text>
    {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            Node n = new Node(value) ;
            if (n.getState().equals("C"))
            {
                context.write(new Text(n.getId()), new
                        Text(n.getNeighbours()+"\t"+n.getDistance()+"\t"+"D")) ;
                for (String neighbour:n.getNeighbours().split(","))
                {
                    context.write(new Text(neighbour), new Text("\t"+(n.getDistance()+1)+"\tC")) ;
                }
            }
            else
            {
                context.write(new Text(n.getId()), new
                        Text(n.getNeighbours()+"\t"+n.getDistance()
                        +"\t"+n.getState())) ;
            }
        }
    }

    public static class GraphPathReducer extends Reducer<Text, Text, Text, Text>
    {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
            String neighbours = null ;
            int distance = -1 ;
            String state = "P" ;
            for(Text t: values)
	    {
		Node n = new Node(key, t) ;
		if (n.getState().equals("D"))
		{
		    neighbours = n.getNeighbours() ;
		    distance = n.getDistance() ;
		    state = n.getState() ;
		    break ;
		}
		if (n.getNeighbours() != null && n.getNeighbours().trim().length() > 0)
		{
		    neighbours = n.getNeighbours() ;
		}
		if (n.getDistance() > distance)
		    distance = n.getDistance() ;
		if (n.getState().equals("D") ||
			(n.getState().equals("C") &&state.equals("P")))
		    state=n.getState() ;
	    }
            if (! state.equals("P"))
            {
                Counter counter = context.getCounter(StateCounter.class.getName(),
                        StateCounter.TOTAL_STATE_CHANGE.toString());
                counter.increment(1);
            }
            context.write(key, new
                    Text(neighbours+"\t"+distance+"\t"+state)) ;
        }
    }
    public static void main(String[] args) throws Exception
    {

	int count = 0;
	Configuration conf = new Configuration();
	Path inputPath = new Path(args[0]);

	Path basePath = new Path(args[1] + "-iteration-");
	Path  outputPath = new Path(basePath.toString()+ count);
	Path  finalPath = new Path(args[1]);
	Long oldValue =  new Long(0);

	do 
	{
	    Job job =Job.getInstance();
	    job.setJarByClass(GraphPath.class);
	    job.setMapperClass(GraphPathMapper.class);
	    job.setReducerClass(GraphPathReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job,inputPath);
	    FileOutputFormat.setOutputPath(job, outputPath);
	    outputPath.getFileSystem(job.getConfiguration()).delete(outputPath,true);

	    Boolean flag = job.waitForCompletion(true) ;
	    Counters counters= job.getCounters();
	    Counter counter = counters.findCounter( StateCounter.TOTAL_STATE_CHANGE);
	    if (flag){
		Long newValue = counter.getValue();
		if (newValue == oldValue) break;
		else
		{
		    oldValue = newValue;
		    count +=1 ;
		    inputPath = new Path(outputPath.toString());
		    outputPath = new Path(basePath.toString() + count);
		}
	    }
	    else{
		System.exit(1);
	    }
	}while (true);

	//  Copy output  data from program created hdfs directory to user specified  hdfs output  directory.
	{
	    // Delete  the final output file if already exist.
	    finalPath.getFileSystem(conf).delete(finalPath,true);
	    FileSystem srcFs = outputPath.getFileSystem(conf);

	    FileSystem dstFs = finalPath.getFileSystem(conf);

	    FileUtil.copy(srcFs, outputPath, dstFs, finalPath, false, conf);
	}
	System.exit(0);
    }
}

