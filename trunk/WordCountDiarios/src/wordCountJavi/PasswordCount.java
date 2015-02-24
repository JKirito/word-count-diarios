package wordCountJavi;

import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PasswordCount {

	public static void main(String[] args) throws Exception {
		long init = new Date().getTime();
		// Para ejecutar desde eclipse..
		args = new String[2];
		// args[0] =
		// "/home/pruebahadoop/Documentos/DescargasPeriodicos/Procesado/LaNacion/PRUEBA";
		// args[1] = "/home/pruebahadoop/Documentos/output";
		args[0] = "/home/pruebahadoop/Documentos/a";
		args[1] = "/home/pruebahadoop/Documentos/output";

		if (args.length != 2) {
			throw new Exception("Los parametros deben ser 2: input y ouput");
		}
		Configuration conf = new Configuration();

		/*
		 * Even if your MapReduce application reads and writes uncompressed
		 * data, it may ben- efit from compressing the intermediate output of
		 * the map phase. Since the map output is written to disk and
		 * transferred across the network to the reducer nodes, by using a fast
		 * compressor such as LZO, LZ4, or Snappy, you can get performance gains
		 * simply because the volume of data to transfer is reduced.
		 */
		// Se consigue agregando las siguientes 2 lineas!
		// conf.setBoolean("mapreduce.map.output.compress", true);
		// conf.setClass("mapreduce.map.output.compress.codec",
		// BZip2Codec.class, CompressionCodec.class);

		Job job = Job.getInstance(conf, "WordCount");
		job.setJarByClass(PasswordCount.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(PsswordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		/*
		 * To compress the output of a MapReduce job, in the job configuration,
		 * set the mapred.output.compress property to true and the
		 * mapred.output.compression.codec property to the classname of the
		 * compression codec you want to use. Alternatively, you can use the
		 * static convenience methods on FileOutputFormat to set these
		 * properties
		 */
		// FileOutputFormat.setCompressOutput(job, true);
		// FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);

		long fin = new Date().getTime();
		if (job.waitForCompletion(true)) {
			System.out.println("Tardó estos miisegundos: " + (fin - init));
		}
	}
}
