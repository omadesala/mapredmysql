package com.chinacloud.mapreduce.example.mysql;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapred.lib.db.DBInputFormat;
import org.apache.hadoop.mapred.lib.db.DBOutputFormat;
import org.apache.hadoop.mapred.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

//public class Mysql2Mr {

public class StudentinfoRecord extends Configured implements Writable,
		DBWritable, Tool {

	private static Logger logger = Logger.getLogger(StudentinfoRecord.class);
	Date date;
	Double open;
	Double high;
	Double low;
	Double close;
	Double volume;
	Double adjclose;

	public StudentinfoRecord() {

	}

	public void readFields(DataInput in) throws IOException {

		logger.info("readFields ...");
		int dateLen = in.readInt();
		byte[] datebyte = new byte[dateLen];
		in.readFully(datebyte);
		this.date = Date.valueOf(new String(datebyte));

		this.open = in.readDouble();
		this.high = in.readDouble();
		this.low = in.readDouble();
		this.close = in.readDouble();
		this.volume = in.readDouble();
		this.adjclose = in.readDouble();

	}

	@Override
	public String toString() {
		return date + "," + open + "," + high + "," + low + ", " + close + ", "
				+ volume + ", " + adjclose;
	}

	@Override
	public void write(PreparedStatement stmt) throws SQLException {

		logger.info("write(PreparedStatement stmt) ...");
		stmt.setDate(1, this.date);
		// stmt.setDouble(2, this.open);
		// stmt.setDouble(3, this.high);
		// stmt.setDouble(4, this.low);
		// stmt.setDouble(5, this.close);
		stmt.setDouble(2, this.volume);
		// stmt.setDouble(7, this.adjclose);
	}

	@Override
	public void readFields(ResultSet result) throws SQLException {

		logger.info("readFields(ResultSet result) ...");

		this.date = result.getDate(1);
		this.open = result.getDouble(2);
		this.high = result.getDouble(3);
		this.low = result.getDouble(4);
		this.close = result.getDouble(5);
		this.volume = result.getDouble(6);
		this.adjclose = result.getDouble(7);
	}

	@Override
	public void write(DataOutput out) throws IOException {

		logger.info("write(DataOutput out) ...");

		SimpleDateFormat format = new SimpleDateFormat();
		String dateStr = format.format(this.date);
		byte[] temp = dateStr.getBytes();
		out.writeInt(temp.length);
		out.write(temp);
		out.writeDouble(this.open);
		out.writeDouble(this.high);
		out.writeDouble(this.low);
		out.writeDouble(this.close);
		out.writeDouble(this.volume);
		out.writeDouble(this.adjclose);
	}

	public static class DBInputMapper extends MapReduceBase implements
			Mapper<LongWritable, StudentinfoRecord, LongWritable, Text> {

		public void map(LongWritable key, StudentinfoRecord value,
				OutputCollector<LongWritable, Text> collector, Reporter reporter)
				throws IOException {
			logger.info("map key: " + key + " value: " + value.toString());
			collector.collect(key, new Text(value.toString()));
		}
	}

	public static class MyReducer extends MapReduceBase implements
			Reducer<LongWritable, Text, StudentinfoRecord, Text> {

		@Override
		public void reduce(LongWritable key, Iterator<Text> values,
				OutputCollector<StudentinfoRecord, Text> output,
				Reporter reporter) throws IOException {

			logger.info("reduce key: " + key + " values: " + values.toString());

			String[] splits = values.next().toString().split(",");
			StudentinfoRecord r = new StudentinfoRecord();
			r.date = Date.valueOf(splits[0]);
			r.open = Double.valueOf(splits[1]);
			r.high = Double.valueOf(splits[2]);
			r.low = Double.valueOf(splits[3]);
			r.close = Double.valueOf(splits[4]);
			r.volume = Double.valueOf(splits[5]);
			r.adjclose = Double.valueOf(splits[6]);

			output.collect(r, new Text(r.toString()));
		}
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException {

		logger.info("main method running ... ");

		for (String arg : args) {
			logger.info("arg is:  " + arg);
		}

		for (Map.Entry<Object, Object> entry : System.getProperties()
				.entrySet()) {
			System.out.println(entry.getKey() + "\t" + entry.getValue());
		}

		JobConf conf = new JobConf(StudentinfoRecord.class);
		// Configuration conf = new Configuration();

		// Path filePath = new Path(
		// "hdfs://master:8020/usr/share/java/mysql-connector-java-5.1.34-bin.jar");
		// DistributedCache.addFileToClassPath(filePath, conf);

		// DistributedCache.addArchiveToClassPath(filePath, conf);

		conf.setMapOutputKeyClass(LongWritable.class);
		// conf.setClass(JobContext.MAP_OUTPUT_KEY_CLASS, LongWritable.class,
		// Object.class);
		conf.setMapOutputValueClass(Text.class);
		// conf.setClass(JobContext.MAP_OUTPUT_VALUE_CLASS, Text.class,
		// Object.class);
		conf.setOutputKeyClass(LongWritable.class);
		// conf.setClass(JobContext.OUTPUT_KEY_CLASS, LongWritable.class,
		// Object.class);
		conf.setOutputValueClass(Text.class);
		// conf.setClass(JobContext.OUTPUT_VALUE_CLASS, Text.class,
		// Object.class);
		conf.setInputFormat(DBInputFormat.class);
		// conf.setClass("mapred.input.format.class", DBInputFormat.class,
		// InputFormat.class);
		conf.setOutputFormat(DBOutputFormat.class);
		// conf.setClass("mapred.output.format.class", DBOutputFormat.class,
		// OutputFormat.class);

		DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver",
				"jdbc:mysql://172.16.50.80:3306/stocks", "root", "hadoop");

		String[] fields = { "date", "open", "high", "low", "close", "volume",
				"adjclose" };

		// DBConfiguration dbConf = new DBConfiguration(job);
		// dbConf.setInputClass(inputClass);
		// dbConf.setInputTableName(tableName);
		// dbConf.setInputFieldNames(fieldNames);
		// dbConf.setInputConditions(conditions);
		// dbConf.setInputOrderBy(orderBy);

		DBInputFormat.setInput(conf, StudentinfoRecord.class, "IBM", null,
				"date", fields);
		DBOutputFormat.setOutput(conf, "t2", "date", "volume");

		conf.setMapperClass(DBInputMapper.class);
		conf.setReducerClass(MyReducer.class);

		// Job job = Job.getInstance(conf);
		// job.addFileToClassPath(filePath);
		// conf.setClassLoader(Configuration.class.getClassLoader());

		try {
			int res = ToolRunner.run(conf, new StudentinfoRecord(), args);
			// System.exit(res);
			// job.wait();
			// job.waitForCompletion(true);
			Thread.sleep(10000000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// JobClient.runJob(conf);
	}

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = super.getConf();
		String string = conf.get("tmpjars");
		logger.info("tmpjars is: " + string);
		// logger.info("conf is: " + conf.toString());
		for (String arg : args) {
			logger.info("args is: " + arg);
		}

		Job job = new Job(conf, "My Job");
		job.addFileToClassPath(new Path(
				"/usr/share/java/mysql-connector-java-5.1.34-bin.jar"));
		// job.addArchiveToClassPath("/");
		// Job job = Job.getInstance(conf);
		job.waitForCompletion(true);
		return 0;
	}
}
// }