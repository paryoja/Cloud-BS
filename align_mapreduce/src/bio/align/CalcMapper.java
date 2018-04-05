package bio.align;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CalcMapper extends Mapper<LongWritable, Text, Text, Text> {
	// HashMap<String, String> refGenomeMap = new HashMap<String, String>();
	Text outKey = new Text();
	Text outValue = new Text();

	// MyStringBuffer ref_chrm;
	String chrom_id;
	int ref_length = 0;

	@Override
	public void setup( Context context ) throws IOException, InterruptedException {
		// final Configuration conf = context.getConfiguration();
		//
		// URI[] patternsURIs = Job.getInstance( conf ).getCacheFiles();
		//
		// for( URI patternsURI : patternsURIs ) {
		// Path patternsPath = new Path( patternsURI.getPath() );
		// String patternsFileName = patternsPath.getName().toString();
		//
		// BufferedReader br = new BufferedReader( new FileReader( patternsFileName ) );
		// String line = null;
		//
		// ArrayList<String> chrom_seq_buf = new ArrayList<String>();
		// ArrayList<Integer> index = new ArrayList<Integer>();
		// chrom_id = null;
		// // int c = 0;
		// while( ( line = br.readLine() ) != null ) {
		// if( line.startsWith( ">" ) ) {
		// // if( chrom_id != null ) {
		// // addRef( chrom_id, chrom_seq.toString() );
		// // }
		// chrom_id = line.substring( 1 );
		// // System.out.println( "chrom_id " + chrom_id );
		// }
		// else {
		// chrom_seq_buf.add( line.toUpperCase() );
		// index.add( ref_length );
		// ref_length += line.length();
		// // System.out.println( "c: " + c++ );
		// }
		// }
		// // if( chrom_id != null ) {
		// // addRef( chrom_id, chrom_seq.toString() );
		// // }
		// ref_chrm = new MyStringBuffer( chrom_seq_buf, index );
		// br.close();
		// }
		// // System.out.println( "Loaded" );
	}

	// public void addRef( String id, String line ) {
	// // update refGenomeMap with line
	// refGenomeMap.put( id, line );
	// System.out.println( id );
	// }

	@Override
	public void map( LongWritable key, Text value, Context context ) throws IOException, InterruptedException {
		String input = value.toString();
		String[] temp = input.split( "\t" );

		// System.out.println( Arrays.toString( temp ) );

		// String read_id = temp[ 0 ];
		// String uniq = temp[ 1 ];
		// String method = temp[ 2 ];
		// String mis = temp[ 3 ];
		String chrm = temp[ 4 ];
		// int pos = Integer.parseInt( temp[ 5 ] );
		// String cigar_str = temp[ 6 ];
		// String origin_seq = temp[ 7 ];

		outKey.set( chrm );
		// outValue.set( value.toString() );

		context.write( outKey, value );

		// // String ref_chrm = refGenomeMap.get( chrm );
		// ArrayList<String[]> cigar = new ArrayList<String[]>();
		//
		// int ref_targeted_length = parse_cigar( cigar_str, cigar );
		//
		// // for( int i = 0; i < cigar.size(); i++ ) {
		// // System.out.println( Arrays.toString( cigar.get( i ) ) );
		// // }
		//
		// String target_strand = null;
		// int start_pos;
		// String target_seq;
		// if( method.equals( "W_C2T" ) ) {
		// target_strand = "W";
		// start_pos = pos;
		// target_seq = origin_seq;
		// }
		// else if( method.equals( "W_G2A" ) ) {
		// target_strand = "C";
		// start_pos = pos;
		// target_seq = StringUtils.reverse( translate( origin_seq ) );
		// Collections.reverse( cigar );
		// }
		// else if( method.equals( "C_G2A" ) ) {
		// target_strand = "W";
		// start_pos = ref_length - pos - ref_targeted_length;
		// target_seq = StringUtils.reverse( translate( origin_seq ) );
		// Collections.reverse( cigar );
		// }
		// else if( method.equals( "C_C2T" ) ) {
		// target_strand = "C";
		// start_pos = ref_length - pos - ref_targeted_length;
		// target_seq = origin_seq;
		// }
		// else {
		// throw new InterruptedException( "unknown method " + method );
		// }
		//
		// // System.out.println( target_strand );
		// // System.out.println( start_pos );
		// // System.out.println( target_seq );
		//
		// int end_pos = start_pos + ref_targeted_length - 1;
		// int prev2 = Integer.max( 2 - start_pos, 0 );
		// int next2 = Integer.max( end_pos - ref_length + 2, 0 );
		//
		// String prev2_seq = StringUtils.repeat( "N", prev2 ) + ref_chrm.substring( start_pos + prev2 - 2, start_pos, false );
		// String ref_seq = ref_chrm.substring( start_pos, ( end_pos + 1 ), false );
		//
		// if( ref_seq.length() != ( end_pos - start_pos + 1 ) ) {
		// System.out.println( start_pos );
		// System.out.println( end_pos );
		// System.out.println( ref_seq );
		// ref_chrm.substring( start_pos, ( end_pos + 1 ), true );
		//
		// throw new InterruptedException( "error size mismatch" );
		// }
		//
		// String next2_seq = ref_chrm.substring( ( end_pos + 1 ), ( end_pos + 1 + 2 - next2 ), false )
		// + StringUtils.repeat( "N", next2 );
		//
		// if( target_strand.equals( "C" ) ) {
		// ref_seq = StringUtils.reverse( translate( ref_seq ) );
		// // # swap prev and next
		// String tmp = StringUtils.reverse( translate( prev2_seq ) );
		// prev2_seq = StringUtils.reverse( translate( next2_seq ) );
		// next2_seq = tmp;
		// }
		//
		// // System.out.println( ref_seq );
		// // System.out.println( prev2_seq );
		// // System.out.println( next2_seq );
		//
		// // with contig, refseq, cigar
		// // reconstruct alignment
		// int r_pos = cigar.get( 0 )[ 0 ].equals( "S" ) ? Integer.parseInt( cigar.get( 0 )[ 1 ] ) : 0;
		// int g_pos = 0;
		// String r_aln = "";
		// String g_aln = "";
		//
		// for( String[] cig : cigar ) {
		//
		// String opt = cig[ 0 ];
		// int count = Integer.parseInt( cig[ 1 ] );
		//
		// if( opt.equals( "M" ) ) {
		// r_aln += target_seq.substring( r_pos, ( r_pos + count ) );
		// try {
		// g_aln += ref_seq.substring( g_pos, ( g_pos + count ) );
		// }
		// catch( Exception e ) {
		// e.printStackTrace();
		// System.out.println( ref_seq );
		// System.out.println( "start: " + start_pos + " end: " + ( end_pos + 1 ) );
		// System.out.println( g_pos );
		// System.out.println( count );
		//
		// throw e;
		// }
		// r_pos += count;
		// g_pos += count;
		// }
		// else if( opt.equals( "D" ) ) {
		// r_aln += StringUtils.repeat( "-", count );
		// g_aln += ref_seq.substring( g_pos, ( g_pos + count ) );
		// g_pos += count;
		// }
		// else if( opt.equals( "I" ) ) {
		// r_aln += target_seq.substring( r_pos, ( r_pos + count ) );
		// g_aln += StringUtils.repeat( "-", count );
		// r_pos += count;
		// }
		// }
		//
		// // System.out.println( r_aln );
		// // System.out.println( g_aln );
		// // System.out.println( r_pos );
		//
		// // # count mismatches
		// int slen = r_aln.length();
		// if( slen != g_aln.length() ) {
		// return;
		// }
		//
		// int mismatches = 0;
		// for( int i = 0; i < slen; i++ ) {
		// if( ( r_aln.charAt( i ) != g_aln.charAt( i ) ) && ( r_aln.charAt( i ) != 'N' ) && ( g_aln.charAt( i ) != 'N' )
		// && !( r_aln.charAt( i ) == 'T' && g_aln.charAt( i ) == 'C' ) ) {
		// mismatches += 1;
		// }
		// }
		//
		// // # get methylation sequence
		// String methy = "";
		// String tmp = "-";
		// // String read = r_aln;
		// String gn_appended = g_aln + next2_seq;
		// // # TODO: context should be added
		// for( int i = 0; i < slen; i++ ) {
		// if( gn_appended.charAt( i ) == '-' ) {
		// continue;
		// }
		// else if( r_aln.charAt( i ) == 'T' && gn_appended.charAt( i ) == 'C' ) { // unmeth
		// String[] n = get_next2( gn_appended, i );
		// String n1 = n[ 0 ];
		// String n2 = n[ 1 ];
		// if( n1.equals( "G" ) ) {
		// tmp = "x";
		// }
		// else if( n2.equals( "G" ) ) {
		// tmp = "y";
		// }
		// else {
		// tmp = "z";
		// }
		// }
		// else if( r_aln.charAt( i ) == 'C' && gn_appended.charAt( i ) == 'C' ) { // meth
		// String[] n = get_next2( gn_appended, i );
		// String n1 = n[ 0 ];
		// String n2 = n[ 1 ];
		// if( n1.equals( "G" ) ) {
		// tmp = "X";
		// }
		// else if( n2.equals( "G" ) ) {
		// tmp = "Y";
		// }
		// else {
		// tmp = "Z";
		// }
		// }
		// else {
		// tmp = "-";
		// }
		// methy += tmp;
		// }
		// // StringBuilder bld = new StringBuilder();
		// // bld.append( mismatches );
		// // bld.append( ' ' );
		// // bld.append( method );
		// // bld.append( ' ' );
		// // bld.append( chrm );
		// // bld.append( ' ' );
		// // bld.append( target_strand );
		// // bld.append( ' ' );
		// // bld.append( start_pos );
		// // bld.append( ' ' );
		// // bld.append( cigar_str );
		// // bld.append( ' ' );
		// // bld.append( target_seq );
		// // bld.append( ' ' );
		// // bld.append( methy );
		// // bld.append( ' ' );
		// // bld.append( prev2_seq + "_" + g_aln + "_" + next2_seq );
		// // bld.append( ' ' );
		// // bld.append( uniq );
		//
		// outKey.set( read_id );
		// outValue.set( res_to_string( mismatches, method, chrm, target_strand, start_pos, cigar_str, target_seq, methy,
		// prev2_seq + "_" + g_aln + "_" + next2_seq, uniq ) );
		//
		// context.write( outKey, outValue );
	}

	public static String res_to_string( int mismatchs, String method, String chrm, String strand, int start_pos, String cigar_str,
			String bs_seq, String methyl, String ref_config, String uniq ) {
		String optional = "XO:Z:" + method + "\t" + "XS:i:0\tNM:i:" + mismatchs + "\tXM:Z:" + methyl + "\tXG:Z:" + ref_config
				+ "\tXU:Z:" + uniq;

		String res = String.format( "%d\t%s\t%d\t%d\t%s\t%s\t%d\t%d\t%s\t%s\t%s", ( strand == "C" ? 16 : 0 ), chrm, start_pos,
				255, cigar_str, "*", 0, 0, bs_seq, "*", optional );

		return res;

	}

	public static String translate( String origin ) {
		return StringUtils.replaceChars( origin, "acgtACGT", "TGCATGCA" );
	}

	public static String[] get_next2( String seq, int pos ) {
		// def get_next2(seq, pos):
		String[] res = { "N", "N" };
		int rpos = 0;

		for( int i = pos + 1; i < seq.length(); i++ ) {
			// for i in range((pos + 1), len(seq)):
			if( rpos >= 2 ) {
				break;
			}
			else if( seq.charAt( i ) == '-' ) {
				continue;
			}
			else {
				res[ rpos ] = seq.substring( i, i + 1 );
				rpos += 1;
			}
		}
		return res;
	}

	public static int parse_cigar( String cigar, ArrayList<String[]> res ) {
		// CIGARS = ["M", "I", "D", "S"]
		HashSet<Character> CIGARS = new HashSet<Character>();
		CIGARS.add( 'M' );
		CIGARS.add( 'I' );
		CIGARS.add( 'D' );
		CIGARS.add( 'S' );

		// res = []

		int start = 0, end = 0;
		int ref_length = 0;

		while( end < cigar.length() ) {
			char cig = cigar.charAt( end );
			if( CIGARS.contains( cig ) ) {
				// if cigar[ end ] in CIGARS:
				// # for cigar
				String num = cigar.substring( start, end );
				res.add( new String[] { "" + cig, num } );

				if( cig == 'M' || cig == 'D' ) {
					ref_length += Integer.parseInt( num );
				}

				end = end + 1;
				start = end;
			}
			else {
				end += 1;
			}
		}
		return ref_length;
		// return (res, ref_length)
	}

	@Override
	public void cleanup( Context context ) throws IOException, InterruptedException {

	}
}
