// PatentJoin.java

// Blake Caldwell <blake.caldwell@colorado.edu>
// Lab 10
// CSCI 7000: Data Center Scale Computing 

package org.myorg;
        
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.mapred.lib.ChainReducer;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PatentJoin extends Configured implements Tool {
    public static class mapperA extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable,Text> {
	private Text state = new Text();
	private Text country = new Text();
	private Text cited = new Text();
	private LongWritable patent = new LongWritable();
	private String token = new String();
	private final static Text us_country = new Text("\"US\"");

	public void map(LongWritable key, Text value, OutputCollector<LongWritable,Text> output, Reporter reporter) throws IOException {
	    int count = 0;
	    int numTokens = 0;
	    String line = value.toString();
	    String first_token = new String();
	    StringTokenizer tokenizer = new StringTokenizer(line,",");

	    numTokens = tokenizer.countTokens();
	    if (numTokens > 2) {
		// this is the patent file
		while (tokenizer.hasMoreTokens()) {           // iterate tokens separated by "," in a line
		    token = tokenizer.nextToken();
		    // skip the first 4 tokens (3 + the first one)
		    if (count == 0) {
			// just store this in case this is the first line
			first_token=token;
		    }
		    else if (count == 4) {
			country.set(token);
			if (country.equals(us_country) == false) { 	// make sure its in the US and then get the state
			    break;
			}
		    }	
		    else if (count == 5) {
			// check that state is two characters
 		    
			if (token.length() == 4) {
			    // now set the patent number
			    patent.set(Long.parseLong(first_token,10));
			
			    state.set(token);
			    output.collect(patent,state);
			}
			break;
		    }
		    count++;
		}
	    }
	    else if (numTokens == 2) {
		// this is the citation file
		String skip_string = "\"CITING\"";
		while (tokenizer.hasMoreTokens()) {           // iterate tokens separated by "," in a line
		    token = tokenizer.nextToken();
		    if (count == 0) {
			if (token.startsWith(skip_string)) {
			    break;
			}
			else {
			    patent.set(Long.parseLong(token,10));
			}
		    }
		    else if (count == 1) {
			cited.set(token);
			output.collect(patent,cited);
		    }	
		    count++;
		}
	    }
	}
    }

    public static class Reduce extends MapReduceBase implements Reducer<LongWritable, Text, LongWritable,Text>  {
        public void reduce(LongWritable key, Iterator<Text> values, OutputCollector<LongWritable,Text> output, Reporter reporter) throws IOException {
	    String patent_state=null;
	    ArrayList<String> citedPatents= new ArrayList<String>();
	    String currentVal=null;
	    Text value = new Text();
	    while (values.hasNext()) { 
		currentVal = values.next().toString();
		if (currentVal.length() == 4) {
		    patent_state = currentVal;
		}
		else {
		    citedPatents.add(currentVal);
		}
	    }
	    
	    if (patent_state != null) {
		for (String val: citedPatents) {
		    value.set(val + " " + patent_state);
		    output.collect(key, value);
		}
	    }
	}
    }
    public static class swizzler extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable,Text> {
	private Text state = new Text();
	private Text country = new Text();
	private Text cited = new Text();
	private LongWritable cited_key = new LongWritable();
	private Text value = new Text();
	private LongWritable patent = new LongWritable();
	private String token = new String();
	private String value_string = new String();
	private final static Text us_country = new Text("\"US\"");

	public void map(LongWritable key, Text value, OutputCollector<LongWritable,Text> output, Reporter reporter) throws IOException {
	    int count = 0;
	    int numTokens = 0;
	    String line = value.toString();
	    String first_token = new String();
	    StringTokenizer tokenizer = new StringTokenizer(line,",");

	    numTokens = tokenizer.countTokens();
	    if (numTokens > 3) {	// this is the patent file
		while (tokenizer.hasMoreTokens()) {           // iterate tokens separated by "," in a line
		    token = tokenizer.nextToken();
		    if (count == 0) { 			// just store this in case this is the first line
			first_token=token;
		    }
		    else if (count == 4) { 		    // skip the first 4 tokens (3 + the first one)
			country.set(token);
			if (country.equals(us_country) == false) { 	// make sure its in the US and then get the state
			    break;
			}
		    }	
		    else if (count == 5) {
			if (token.length() == 4) { 			// check that state is two characters
			    value_string = token;
			}
			break;
		    }
		    else if (count == 13) {
			if (token != null) {
			    value_string = value_string + " " + token;
			}
			else {
			    value_string = value_string + ",";
			}
			patent.set(Long.parseLong(first_token,10));
			value.set(value_string);
			output.collect(patent,value);
			break;
		    }
		    count++;
		}
	    }
	    else if (numTokens == 3) {
		// this is the citation file
		String skip_string = "\"CITING\"";
		first_token = null;
		while (tokenizer.hasMoreTokens()) {           // iterate tokens separated by "," in a line
		    token = tokenizer.nextToken();
		    if (count == 0) {
			if (token.startsWith(skip_string)) {
			    break;
			}
			else {
			    first_token = token;
			}
		    }
		    else if (count == 1) {
			cited_key.set(Long.parseLong(token,10));
		    }
		    else if (count == 2) {
			if (value_string != null) {
			    // append the state to the patent. this will be the value
			    value_string = first_token + " " +  token;
			    value.set(value_string);
			    output.collect(cited_key, value);
			}
		    }
		    count++;
		}
	    }
	}
    }
    
    public int run(String[] args) throws Exception {
	JobConf conf = new JobConf(new Configuration(), PatentJoin.class);
	conf.setJobName("patentjoin");        

	Path patent_data=new Path(args[0]);
	Path citation_data=new Path(args[1]);
	Path intermediate_data=new Path(args[2]);
	Path output_data=new Path(args[3]);

	FileInputFormat.setInputPaths(conf, patent_data);
	FileInputFormat.addInputPath(conf, citation_data);
	FileOutputFormat.setOutputPath(conf, intermediate_data);

	conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        conf.setJarByClass(org.myorg.PatentJoin.class); // added to example for local build

        JobConf mapAConf = new JobConf(false);
        ChainMapper.addMapper(conf, mapperA.class, LongWritable.class, Text.class, LongWritable.class, Text.class, true, mapAConf);
	
	// Just use a single Reducer. Use these lines to use a chain reducer
	JobConf reduceConf = new JobConf(false);
	ChainReducer.setReducer(conf, Reduce.class, LongWritable.class, Text.class, LongWritable.class, Text.class, true, reduceConf);
	//	conf.setReducerClass(Reduce.class);

        JobConf mapBConf = new JobConf(false);
        ChainReducer.addMapper(conf, swizzler.class, LongWritable.class, Text.class, LongWritable.class, Text.class, true, mapBConf);        

        JobClient.runJob(conf);
	/*
	JobConf conf2 = new JobConf(new Configuration(), PatentJoin.class);
	conf2.setJobName("patentjoin2");        

	FileInputFormat.addInputPath(conf2, new Path(args[0]));
	FileInputFormat.addInputPath(conf2, new Path(args[2]));
	FileOutputFormat.setOutputPath(conf2, new Path(args[3]));

	conf2.setInputFormat(TextInputFormat.class);
        conf2.setOutputFormat(TextOutputFormat.class);
        conf2.setJarByClass(org.myorg.PatentJoin.class); // added to example for local build

        JobConf mapBConf = new JobConf(false);
        ChainMapper.addMapper(conf2, swizzler.class, LongWritable.class, Text.class, LongWritable.class, Text.class, true, mapBConf);        
	//conf2.addDependingJob(conf);
	JobClient.runJob(conf2);
	*/
	return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PatentJoin(), args);
        System.exit(res);
    }
    
}