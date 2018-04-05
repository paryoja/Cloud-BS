package bio.align.misc;

import java.io.IOException;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class FromBowtieMain {

	public static CommandLine parseInput( String args[] ) {
		Options options = new Options();

		// required
		options.addOption( "ifile", true, "Input file path (HDFS)" );
		options.addOption( "ofile", true, "Output file path (HDFS)" );
		options.addOption( "rfile", true, "Reference file path (Local)" );
		options.addOption( "machines", true, "Number of machines used" );
		CommandLineParser parser = new BasicParser();
		CommandLine cmd = null;

		/**
		 * Parse parameters
		 */

		try {
			cmd = parser.parse( options, args );
		}
		catch( ParseException e ) {
			e.printStackTrace();
			System.exit( 1 );
		}

		return cmd;
	}

	public static void main( String args[] ) throws Exception {
		Configuration conf = new Configuration();
		final String[] otherArgs = new GenericOptionsParser( conf, args ).getRemainingArgs();
		CommandLine cmd = parseInput( otherArgs );

		final Job localJob = getLocalJob( conf, cmd );

		// DEBUG
		final boolean localDone = localJob.waitForCompletion( true );
		if( localDone == false ) {
			System.out.println( "Error: Local Phase" );
			System.exit( 1 );
		}
	}

	public static Job getLocalJob( Configuration conf, CommandLine cmd ) throws IllegalArgumentException, IOException {

		String refPath = cmd.getOptionValue( "rfile" );
		System.out.println( refPath );
		conf.set( "refPath", refPath );

		final Job localJob = Job.getInstance( conf, "From bowtie LOCAL" );

		localJob.setJarByClass( FromBowtieMain.class );

		localJob.setMapperClass( FromMapper.class );
		localJob.setReducerClass( FromReducer.class );

		// output classes
		localJob.setMapOutputKeyClass( Text.class );
		localJob.setMapOutputValueClass( Text.class );
		localJob.setOutputKeyClass( Text.class );
		localJob.setOutputValueClass( Text.class );

		// shuffling pharameters
		localJob.setNumReduceTasks( Integer.parseInt( cmd.getOptionValue( "machines" ) ) - 1 );

		FileInputFormat.addInputPath( localJob, new Path( cmd.getOptionValue( "ifile" ) ) );
		FileOutputFormat.setOutputPath( localJob, new Path( cmd.getOptionValue( "ofile" ) ) );

		return localJob;
	}
}
