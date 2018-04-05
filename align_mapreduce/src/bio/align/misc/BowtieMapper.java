package bio.align.misc;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BowtieMapper extends Mapper<LongWritable, Text, Text, Text> {
	ArrayList<String> idList = new ArrayList<String>();
	ArrayList<String> seqList = new ArrayList<String>();

	Text outKey = new Text();
	Text outValue = new Text();
	int count = 0;

	@Override
	public void map( LongWritable key, Text value, Context context ) throws IOException, InterruptedException {
		if( value.getLength() == 0 ) {
			return;
		}
		count++;

		String text = value.toString();
		String[] temp = text.split( "\n" );

		String chrom_id = transformId( temp[ 0 ] );
		String seq = temp[ 1 ];

		idList.add( chrom_id );
		seqList.add( seq );

		outKey.set( chrom_id );
		outValue.set( seq );
		context.write( outKey, outValue );
	}

	@Override
	public void cleanup( Context context ) throws IOException, InterruptedException {
		Runtime rt = Runtime.getRuntime();
		Process pr = rt.exec( "mkdir /tmp/map" );
		BufferedReader input = new BufferedReader( new InputStreamReader( pr.getInputStream() ) );
		String line = null;

		try {
			while( ( line = input.readLine() ) != null ) {
				System.out.println( line );
			}
		}
		catch( IOException e ) {
			e.printStackTrace();
		}

		int taskId = context.getTaskAttemptID().getTaskID().getId();

		String ctInputPath = "/tmp/map/iv_C2T_" + taskId + ".fa";
		String gaInputPath = "/tmp/map/iv_G2A_" + taskId + ".fa";
		BufferedWriter bw_ct = new BufferedWriter( new FileWriter( ctInputPath ) );
		BufferedWriter bw_ga = new BufferedWriter( new FileWriter( gaInputPath ) );
		for( int i = 0; i < idList.size(); i++ ) {
			String seq = seqList.get( i );
			String id = idList.get( i );
			saveToFile( bw_ct, id, translate( seq, "C", "T" ) );
			saveToFile( bw_ga, id, translate( seq, "G", "A" ) );
		}
		bw_ct.close();
		bw_ga.close();

		// String refPath = "/home/hadoop/bio/chr_ref_genomes";
		String refPath = "/home/hadoop/bio/chr_ref_genomes";

		runBowtie2( refPath, "W_C2T", ctInputPath, "/tmp/map/" + "W_C2T_" + taskId + ".sam" );
		runBowtie2( refPath, "C_C2T", ctInputPath, "/tmp/map/" + "C_C2T_" + taskId + ".sam" );
		runBowtie2( refPath, "W_G2A", gaInputPath, "/tmp/map/" + "W_G2A_" + taskId + ".sam" );
		runBowtie2( refPath, "C_G2A", gaInputPath, "/tmp/map/" + "C_G2A_" + taskId + ".sam" );

		emitResult( context, "/tmp/map/" + "W_C2T_" + taskId + ".sam", "W_C2T" );
		emitResult( context, "/tmp/map/" + "C_C2T_" + taskId + ".sam", "C_C2T" );
		emitResult( context, "/tmp/map/" + "W_G2A_" + taskId + ".sam", "W_G2A" );
		emitResult( context, "/tmp/map/" + "C_G2A_" + taskId + ".sam", "C_G2A" );

		System.out.println( "Count: " + count );
	}

	public String translate( String seq, String cFrom, String cTo ) {
		return seq.replaceAll( cFrom, cTo );
	}

	public String transformId( String id ) {
		return id.replaceAll( "[^a-zA-Z0-9]", "_" );
	}

	public void emitResult( Context context, String path, String method ) throws IOException, InterruptedException {
		BufferedReader br = new BufferedReader( new FileReader( path ) );
		String line = null;

		while( ( line = br.readLine() ) != null ) {
			String[] temp = line.split( "\t", 12 );

			String QNAME = temp[ 0 ];
			String FLAG = temp[ 1 ];
			String RNAME = temp[ 2 ];
			String POS = temp[ 3 ];
			// String MAPQ = temp[ 4 ];
			String CIGAR = temp[ 5 ];
			// String RNEXT = temp[ 6 ];
			// String PNEXT = temp[ 7 ];
			// String TLEN = temp[ 8 ];
			// String SEQ = temp[ 9 ];
			// String QUAL = temp[ 10 ];

			int unmapped = Integer.parseInt( FLAG ) & 4;

			if( unmapped == 0 ) {
				int mismatches = Integer.MAX_VALUE;

				String OPTIONAL = temp[ 11 ];
				for( String tk : OPTIONAL.split( "\t" ) ) {
					// System.out.println( "tk: " + tk );
					if( tk.startsWith( "AS" ) ) {
						mismatches = 1 - Integer.parseInt( tk.substring( 5 ) );
					}
				}

				outKey.set( QNAME );
				outValue.set( setValue( method, mismatches, RNAME, POS, CIGAR ) );

				context.write( outKey, outValue );
			}
		}
		br.close();
		File f = new File( path );
		f.delete();
	}

	public String setValue( String method, int mismaches, String RNAME, String POS, String CIGAR ) {
		StringBuilder bld = new StringBuilder();
		bld.append( "(" );
		bld.append( method + "\t" );
		bld.append( mismaches + "\t" );
		bld.append( RNAME + "\t" );
		bld.append( ( Integer.parseInt( POS ) - 1 ) + "\t" );
		bld.append( CIGAR + ")" );

		return bld.toString();
	}

	public void saveToFile( BufferedWriter bw, String id, String seq ) throws IOException {
		bw.write( ">" );
		bw.write( id );
		bw.write( "\n" );
		bw.write( seq );
		bw.write( "\n" );
	}

	public void runBowtie2( String refPath, String method, String iPath, String oPath ) throws IOException {
		Runtime rt = Runtime.getRuntime();
		Process pr = rt.exec( genBowtieQuery( refPath + "/" + method, iPath, oPath ) );

		BufferedReader input = new BufferedReader( new InputStreamReader( pr.getInputStream() ) );
		String line = null;

		try {
			while( ( line = input.readLine() ) != null ) {
				System.out.println( line );
			}
		}
		catch( IOException e ) {
			e.printStackTrace();
		}
	}

	public String genBowtieQuery( String refPrefix, String inputFile, String outputFile ) {
		StringBuilder bld = new StringBuilder();

		bld.append( "/home/hadoop/bio/bowtie2-2.2.9/bowtie2 " );
		bld.append( "--local " );
		bld.append( "--quiet " );
		bld.append( "-p 1 " );
		bld.append( "-D 50 " );
		bld.append( "--norc " );
		bld.append( "--sam-nohead " );
		bld.append( "-k 2 " );
		bld.append( "-x " );
		bld.append( refPrefix + " " );
		bld.append( "-f " );
		bld.append( "-U " );
		bld.append( inputFile + " " );
		bld.append( "-S " );
		bld.append( outputFile );

		// DEBUG
		System.out.println( bld.toString() );
		return bld.toString();
	}
}
