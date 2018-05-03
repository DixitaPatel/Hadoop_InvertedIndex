import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//Mapper class
public class InvertedIndexJob {
public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{
    private Text word = new Text();
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String[] arr = value.toString().split("\t");
      Text doc_id = new Text(arr[0]);
      value = new Text(arr[1]);
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, doc_id);
      }
    }
  }
    
//Reducer class
public static class IntSumReducer extends Reducer<Text,Text,Text,Text> {
    private Text result;
    public void reduce(Text key, Iterable<Text> values, Context context
                       ) throws IOException, InterruptedException {
      Map<String, Integer> map = new HashMap<String, Integer>();
      for (Text val : values) {
                String k = val.toString();
                if(map.containsKey(k)){
                        map.put(k, map.get(k)+1);
                }else{
                        map.put(k, 1);
                }
      }
          StringBuilder output = new StringBuilder();
          boolean first = true;
          for(String k : map.keySet()){
                if(!first)
                        output.append("\t"+k+":"+map.get(k));
                else{
                        first = false;
                        output.append(k+":"+map.get(k));
                }
          }
      result = new Text(output.toString());
      context.write(key, result);
    }
  }
  
    public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Inverted Index");
    job.setJarByClass(InvertedIndexJob.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
