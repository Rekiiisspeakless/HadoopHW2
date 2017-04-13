package org.apache.hadoop.map_reduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.WordCount;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;

//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MapReduce {

    public static class MatrixConstructMapper
        extends MapReduceBase implements Mapper<Object, Text, IntWritable, IntWritable> {
        private  IntWritable from = new IntWritable();
        private  IntWritable to = new IntWritable();
    public void map(Object key, Text value, OutputCollector <IntWritable, IntWritable> outputCollector,
                    Reporter reporter
                    ) throws IOException{
        StringTokenizer itr = new StringTokenizer(value.toString(), " ");
        int nodeNum = 5;
        from.set(Integer.valueOf(itr.nextToken()));
        to.set(Integer.valueOf(itr.nextToken()));
        outputCollector.collect(from, to);
    }
}




public static class MatrixConstructReducer
        extends MapReduceBase implements Reducer<IntWritable,IntWritable,MyKeyPair,MyFloatValuePair2> {
    private  MyKeyPair myKeyPair = new MyKeyPair();
    private MyFloatValuePair2 myFloatValuePair2 = new MyFloatValuePair2();

    public void reduce(IntWritable key, Iterator<IntWritable> values,
                        OutputCollector <MyKeyPair, MyFloatValuePair2> outputCollector, Reporter reporter
                        ) throws IOException{
        int totalNodeNum = 5;
        Vector<Integer> tos = new Vector<Integer>();
        int nodeNum = 0;
        tos.setSize(totalNodeNum + 1);
        for(int i = 1; i <= totalNodeNum; ++i){
            tos.setElementAt(0, i);
        }
        while (values.hasNext()){
            IntWritable val = values.next();
            tos.setElementAt(1, val.get());
            nodeNum ++;
        }
        float beta = 0.8f;
        for(int i = 1; i <= totalNodeNum; ++i){
            //myValuePair1.set(1, key.get(), i);
            //myValuePair2.set(2, key.get(), i);
            myKeyPair.set(key.get(), i);


            Integer cur = tos.get(i);
            if(cur == 1){
                myFloatValuePair2.set(1, (float)cur / nodeNum * beta + (1-beta) / totalNodeNum);
                outputCollector.collect(myKeyPair, myFloatValuePair2);
                myFloatValuePair2.set(2, (float)1 / totalNodeNum);
                outputCollector.collect(myKeyPair, myFloatValuePair2);
            } else{
                myFloatValuePair2.set(1, (1-beta) / totalNodeNum);
                outputCollector.collect(myKeyPair, myFloatValuePair2);
                myFloatValuePair2.set(2, (float)1 / totalNodeNum);
                outputCollector.collect(myKeyPair, myFloatValuePair2);
            }
        }
    }
}

public static class PageRankMultiplyMapper
    extends MapReduceBase implements Mapper<MyKeyPair, MyFloatValuePair2, MyKeyPair, MyFloatValuePair>{
    private MyKeyPair keyPair = new MyKeyPair();
    private MyFloatValuePair valuePair = new MyFloatValuePair();

    public void map(MyKeyPair key, MyFloatValuePair2 value, OutputCollector outputCollector,
                    Reporter reporter
    ) throws IOException{
        int nameValue = -1, index1 = -1, index2 = -1;
        float val;

        nameValue = value.getIndex1();

        index1 = key.getI();
        index2 = key.getK();
        val = value.getIndex2();


        for (int k = 1; k <= 5; k++) {
            if (nameValue == 1) {
                keyPair.set(index1, k);
                valuePair.set(nameValue, index2, val);
                /*message += "( " + String.valueOf(keyPair.getI()) + "," + String.valueOf(keyPair.getK()) + " ) ";
                message += "( " + String.valueOf(valuePair.getName()) + "," + String.valueOf(valuePair.getIndex1())
                        + "," +  String.valueOf(valuePair.getIndex2()) + " )\n";*/
                outputCollector.collect(keyPair, valuePair);
            } else {
                keyPair.set(k, index2);
                valuePair.set(nameValue, index1, val);
                outputCollector.collect(keyPair, valuePair);
                /*message += "( " + String.valueOf(keyPair.getI()) + "," + String.valueOf(keyPair.getK()) + " ) ";
                message += "( " + String.valueOf(valuePair.getName()) + "," + String.valueOf(valuePair.getIndex1())
                        + "," +  String.valueOf(valuePair.getIndex2()) + " )\n";*/
            }

        }

    }
}

public static class IntSumReducer
        extends MapReduceBase implements Reducer<MyKeyPair,MyFloatValuePair, IntWritable, MyFloatValuePair> {
    private IntWritable myKey = new IntWritable();
    private  MyFloatValuePair myFloatValuePair = new MyFloatValuePair();


    public void reduce(MyKeyPair key, Iterator<MyFloatValuePair> values,
                       OutputCollector <IntWritable, MyFloatValuePair> outputCollector, Reporter reporter
    ) throws IOException{
        float sum = 0;
        int count = 0;
        HashMap<Integer, Float> hashMapM = new HashMap<Integer, Float>();
        HashMap<Integer, Float> hashMapN = new HashMap<Integer, Float>();

        while(values.hasNext()){
            MyFloatValuePair val = values.next();
            if(val.getName() == 1){
                hashMapM.put(val.getIndex1(), val.getIndex2());
            }else{
                hashMapN.put(val.getIndex1(), val.getIndex2());
            }
            count++;
        }
        for(int i = 1; i <= count / 2; i++){
            float curM = hashMapM.get(i);
            sum += curM * hashMapN.get(i);
            myKey.set(key.getI());
            myFloatValuePair.set(1, i, curM);
            outputCollector.collect(myKey, myFloatValuePair);
        }


        myKey.set(key.getI());
        myFloatValuePair.set(2, key.getK(), sum);
        outputCollector.collect(myKey, myFloatValuePair);
    }
}

public static class PageRankDeadEndPreventMapper
        extends MapReduceBase implements   Mapper<IntWritable, MyFloatValuePair, IntWritable, MyFloatValuePair> {


    public void map(IntWritable key, MyFloatValuePair value,
                    OutputCollector<IntWritable, MyFloatValuePair> outputCollector, Reporter reporter
    ) throws IOException {
        outputCollector.collect(key, value);
    }
}


public static class PageRankDeadEndPreventReducer
        extends MapReduceBase implements Reducer<IntWritable, MyFloatValuePair, MyKeyPair, MyFloatValuePair2> {

        private  MyFloatValuePair2 myFloatValuePair2 = new MyFloatValuePair2();
        private MyKeyPair myKeyPair = new MyKeyPair();

    public void reduce(IntWritable key, Iterator<MyFloatValuePair> values,
                       OutputCollector<MyKeyPair, MyFloatValuePair2> outputCollector, Reporter reporter
    ) throws IOException{
        HashMap<MyKeyPair, Float> hashMapM = new HashMap<MyKeyPair, Float>();
        HashMap<Integer, Float> hashMapN = new HashMap<Integer, Float>();
        int count = 0;
        float s = 0;
        while(values.hasNext()){
            MyFloatValuePair val = values.next();
            if(val.getName() == 1){
                MyKeyPair keyPair = new MyKeyPair(key.get(), val.getIndex1());
                if(!hashMapM.containsKey(keyPair)){
                    myFloatValuePair2.set(1, val.getIndex2());
                    outputCollector.collect(keyPair,   myFloatValuePair2);
                    hashMapM.put(keyPair, val.getIndex2());
                }
            }else{
                hashMapN.put(val.getIndex1(), val.getIndex2());
                s += val.getIndex2();
                count ++;
            }
        }
        for(int i = 1; i <= count; ++i){
            float rNew = hashMapN.get(i) + (1 - s) / 2;
            myFloatValuePair2.set(2, rNew);
            myKeyPair.set(key.get(), i);
            outputCollector.collect(myKeyPair, myFloatValuePair2);
        }
    }
}

public static class OutputMapper
        extends MapReduceBase implements   Mapper<MyKeyPair, MyFloatValuePair2, IntWritable, FloatWritable> {
    private  IntWritable myKey = new IntWritable();
    private  FloatWritable myValue = new FloatWritable();

    public void map(MyKeyPair key, MyFloatValuePair2 value, OutputCollector <IntWritable, FloatWritable> outputCollector,
                    Reporter reporter
    ) throws IOException{
        myKey.set(key.getI());
        myValue.set(value.getIndex2());
        outputCollector.collect(myKey, myValue);
    }
}


public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
        System.err.println("Usage: map_reduce <in> <out>");
        System.exit(2);
    }

    ////////////////////////job1///////////////////////////

    //Job job1 = new Job(conf, "matrix construct");
    JobConf job1 = new JobConf();
    job1.setJobName("matrix construct");
    job1.setJarByClass(MapReduce.class);
    job1.setMapperClass(MatrixConstructMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    job1.setReducerClass(MatrixConstructReducer.class);

    job1.setMapOutputKeyClass(IntWritable.class);
    job1.setMapOutputValueClass(IntWritable.class);

    job1.setOutputKeyClass(MyValuePair.class);
    job1.setOutputValueClass(FloatWritable.class);

    /*job.setOutputKeyClass(MyKeyPair.class);
    job.setOutputValueClass(MyValuePair.class);*/

    FileInputFormat.setInputPaths(job1, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job1, new Path("/user/root/data/hw2/job1"));

    JobClient.runJob(job1);

    ////////////////////loop start//////////////////////////

    ////////////////////////job2///////////////////////////

    JobConf job2 = new JobConf( );
    job2.setJobName("page rank matrix multiply");
    job2.setJarByClass(MapReduce.class);
    job2.setMapperClass(PageRankMultiplyMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    job2.setReducerClass(IntSumReducer.class);

    job2.setMapOutputKeyClass(MyKeyPair.class);
    job2.setMapOutputValueClass(MyFloatValuePair.class);

    job2.setOutputKeyClass(IntWritable.class);
    job2.setOutputValueClass(MyFloatValuePair.class);

    /*job.setOutputKeyClass(MyKeyPair.class);
    job.setOutputValueClass(MyValuePair.class);*/

    FileInputFormat.addInputPath(job2, new Path("/user/root/data/hw2/job1"));
    FileOutputFormat.setOutputPath(job2, new Path("/user/root/data/hw2/job2"));

    JobClient.runJob(job2);

    ////////////////////////job3///////////////////////////

    //Job job3 = new Job(conf, "prevent dead end");
    JobConf job3 = new JobConf();
    job3.setJarByClass(MapReduce.class);
    job3.setMapperClass(PageRankDeadEndPreventMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    job3.setReducerClass(PageRankDeadEndPreventReducer.class);

    job3.setMapOutputKeyClass(IntWritable.class);
    job3.setMapOutputValueClass(MyFloatValuePair.class);

    job3.setOutputKeyClass(MyKeyPair.class);
    job3.setOutputValueClass(MyFloatValuePair2.class);

    /*job.setOutputKeyClass(MyKeyPair.class);
    job.setOutputValueClass(MyValuePair.class);*/

    FileInputFormat.addInputPath(job3, new Path("/user/root/data/hw2/job2"));
    FileOutputFormat.setOutputPath(job3, new Path("/user/root/data/hw2/job3"));

    JobClient.runJob(job3);

    /////////////////////job4/////////////////////////////
    //Job job4 = new Job(conf, "output page rank");
    JobConf job4 = new JobConf();
    job4.setJobName("output page rank");
    job4.setJarByClass(MapReduce.class);
    job4.setMapperClass(OutputMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    //job4.setReducerClass(PageRankDeadEndPreventReducer.class);

    /*job4.setMapOutputKeyClass(IntWritable.class);
    job4.setMapOutputValueClass(MyFloatValuePair.class);*/

    job4.setOutputKeyClass(IntWritable.class);
    job4.setOutputValueClass(FloatWritable.class);

    /*job.setOutputKeyClass(MyKeyPair.class);
    job.setOutputValueClass(MyValuePair.class);*/

    FileInputFormat.addInputPath(job4, new Path("/user/root/data/hw2/job3"));
    FileOutputFormat.setOutputPath(job4, new Path(otherArgs[1]));

    JobClient.runJob(job4);




    //System.exit(job4.waitForCompletion(true) ? 0 : 1);
    }
}
