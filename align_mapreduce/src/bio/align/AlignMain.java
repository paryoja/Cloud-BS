package bio.align;

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

import bio.align.misc.Util;

public class AlignMain {

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

		String inputFile = cmd.getOptionValue( "ifile" );
		String tempFile = "temp";
		String outputFile = cmd.getOptionValue( "ofile" );
		// String referenceFile = cmd.getOptionValue( "rfile" );

		int machines = Integer.parseInt( cmd.getOptionValue( "machines" ) ) - 1;

		long inputsize = Util.size( inputFile );
		long splitsize = inputsize / machines + ( inputsize % machines == 0 ? 0 : 1 );
		conf.setLong( FileInputFormat.SPLIT_MAXSIZE, splitsize );

		conf.set( "textinputformat.record.delimiter", ">" );

		final Job localJob = getLocalJob( conf, cmd, tempFile );

		// DEBUG
		final boolean localDone = localJob.waitForCompletion( true );
		if( localDone == false ) {
			System.out.println( "Error: Local Phase" );
			System.exit( 1 );
		}

		conf = new Configuration();
		new GenericOptionsParser( conf, args ).getRemainingArgs();

		final Job globalJob = getGlobalJob( conf, cmd, tempFile, outputFile );
		final boolean globalDone = globalJob.waitForCompletion( true );
		if( globalDone == false ) {
			System.out.println( "Error: Global Phase" );
			System.exit( 1 );
		}

	}

	public static Job getLocalJob( Configuration conf, CommandLine cmd, String localOutPath )
			throws IllegalArgumentException, IOException {
		final Job localJob = Job.getInstance( conf, "Align LOCAL" );

		// localJob.setPartitionerClass( LocalPartitioner.class );
		// localJob.setGroupingComparatorClass( MergedGroupComparator.class );
		// localJob.setSortComparatorClass( MergedSortComparator.class );

		// localJob.addCacheFile( new Path( cmd.getOptionValue( "ofile" ) ).toUri() );
		localJob.setJarByClass( AlignMain.class );

		localJob.setMapperClass( AlignMapper.class );
		localJob.setReducerClass( AlignReducer.class );

		// output classes
		localJob.setMapOutputKeyClass( Text.class );
		localJob.setMapOutputValueClass( Text.class );
		localJob.setOutputKeyClass( Text.class );
		localJob.setOutputValueClass( Text.class );

		// shuffling pharameters
		localJob.setNumReduceTasks( Integer.parseInt( cmd.getOptionValue( "machines" ) ) - 1 );

		FileInputFormat.addInputPath( localJob, new Path( cmd.getOptionValue( "ifile" ) ) );
		FileOutputFormat.setOutputPath( localJob, new Path( localOutPath ) );

		return localJob;
	}

	public static Job getGlobalJob( Configuration conf, CommandLine cmd, String tempPath, String globalOutPath )
			throws IOException {

		String refPath = cmd.getOptionValue( "rfile" );
		System.out.println( refPath );
		conf.set( "refPath", refPath );

		final Job globalJob = Job.getInstance( conf, "Align GLOBAL" );

		// localJob.setPartitionerClass( LocalPartitioner.class );
		// localJob.setGroupingComparatorClass( MergedGroupComparator.class );
		// localJob.setSortComparatorClass( MergedSortComparator.class );
		// final FileSystem fs = FileSystem.get( conf );

		// String hdfsTempPath = "temp/_" + refPath.substring( refPath.lastIndexOf( '/' ) );

		// System.out.println( "Ref: " + refPath + " hdfs: " + hdfsTempPath );
		// saveFileInHDFS( refPath, hdfsTempPath, fs );

		// globalJob.addCacheFile( new Path( hdfsTempPath ).toUri() );
		globalJob.setJarByClass( AlignMain.class );

		globalJob.setMapperClass( CalcMapper.class );
		globalJob.setReducerClass( CalcReducer.class );

		// output classes
		globalJob.setMapOutputKeyClass( Text.class );
		globalJob.setMapOutputValueClass( Text.class );
		globalJob.setOutputKeyClass( Text.class );
		globalJob.setOutputValueClass( Text.class );

		// shuffling pharameters
		globalJob.setNumReduceTasks( Integer.parseInt( cmd.getOptionValue( "machines" ) ) - 1 );

		FileInputFormat.addInputPath( globalJob, new Path( tempPath ) );
		FileOutputFormat.setOutputPath( globalJob, new Path( globalOutPath ) );

		return globalJob;
	}

}
