package bio.align.misc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FromMapper extends Mapper<LongWritable, Text, Text, Text> {
	// HashMap<String, String> refGenomeMap = new HashMap<String, String>();
	Text outKey = new Text();
	Text outValue = new Text();

	// MyStringBuffer ref_chrm;
	String chrom_id;
	int ref_length = 0;

	@Override
	public void map( LongWritable key, Text origValue, Context context ) throws IOException, InterruptedException {
		String[] string = origValue.toString().split( ":" );
		if( string.length == 1 ) {
			return;
		}
		
		ArrayList<String[]> list = new ArrayList<String[]>();

		String seq = string[ 0 ];

		for( String value : string[ 1 ].split( "\\)\"" ) ) {
			String temp = value.replace( "\"(", "" ).trim();

			list.add( temp.split( "\t" ) );
		}

		// System.out.println( "key: " + key );
		String uniq = Util.select_and_find_uniq_alignment( list );
		if( uniq != null ) {
			String[] temp = uniq.split( "\t" );

			// System.out.println( Arrays.toString( temp ) );

			// String read_id = temp[ 0 ];
			// String uniq = temp[ 1 ];
			// String method = temp[ 2 ];
			// String mis = temp[ 3 ];
			String chrm = temp[ 3 ];
			// int pos = Integer.parseInt( temp[ 5 ] );
			// String cigar_str = temp[ 6 ];
			// String origin_seq = temp[ 7 ];

			outKey.set( chrm );
			// outValue.set( value.toString() );
			outValue.set( seq + "\t" + uniq );
			context.write( outKey, outValue );
		}
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
}
