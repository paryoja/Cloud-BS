package bio.align.misc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WithoutAlignReducer extends Reducer<Text, Text, Text, Text> {
	public static Text outKey = new Text();
	public static Text outValue = new Text();

	@Override
	public void reduce( Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException {
		String seq = null;
		ArrayList<String[]> list = new ArrayList<String[]>();
		for( Text value : values ) {
			String temp = value.toString();

			if( temp.startsWith( "(" ) ) {
				list.add( temp.substring( 1, temp.length() - 1 ).split( "\t" ) );
			}
			else {
				seq = temp;
			}
			// DEBUG
			// context.write( key, value );
		}

		// DEBUG
		if( seq == null ) {
			System.out.println( "Error" );
			throw new InterruptedException( "Has no original sequence" );
		}

		// System.out.println( "key: " + key );
		String uniq = select_and_find_uniq_alignment( list );
		if( uniq != null ) {
			outKey.set( key );
			// outValue.set( uniq + ", '" + seq + "'" );
			outValue.set( uniq + "\t" + seq );
			context.write( outKey, outValue );
		}
	}

	public String select_and_find_uniq_alignment( ArrayList<String[]> list ) {

		// # no other in next?
		// if length == 0:
		// return None

		String[] value = null;
		String uniq = null;

		if( list.size() == 0 ) {
			return null;
		}

		@SuppressWarnings( "unchecked" )
		ArrayList<String[]>[] group = new ArrayList[ 4 ];
		for( int i = 0; i < 4; i++ ) {
			group[ i ] = new ArrayList<String[]>();
		}
		for( String[] l : list ) {
			if( l[ 0 ].equals( "W_C2T" ) ) {
				group[ 0 ].add( l );
			}
			else if( l[ 0 ].equals( "C_C2T" ) ) {
				group[ 1 ].add( l );
			}
			else if( l[ 0 ].equals( "W_G2A" ) ) {
				group[ 2 ].add( l );
			}
			else if( l[ 0 ].equals( "C_G2A" ) ) {
				group[ 3 ].add( l );
			}
			else {
				throw new RuntimeException();
			}
		}

		ArrayList<String[]> sorted_list = new ArrayList<String[]>();

		for( int i = 0; i < 4; i++ ) {
			String[] temp = get_uniq( group[ i ] );

			if( temp != null ) {
				sorted_list.add( temp );
			}

			// System.out.println( temp[ 0 ] );
			// System.out.println( temp[ 1 ] );
		}

		if( sorted_list.size() == 0 ) {
			return null;
		}
		// else {
		// System.out.println( "size: " + sorted_list.size() );
		// }

		Comparator<String[]> distComparator = new Comparator<String[]>() {
			public int compare( String[] s1, String[] s2 ) {

				int rollno1 = Integer.parseInt( s1[ 1 ] );
				int rollno2 = Integer.parseInt( s2[ 1 ] );

				/* For ascending order */
				return rollno1 - rollno2;

				/* For descending order */
				// rollno2-rollno1;
			}
		};

		Collections.sort( sorted_list, distComparator );
		// sorted_list.sort( distComparator );
		// for( int i = 0; i < list.size(); i++ ) {
		// System.out.println( Arrays.toString( list.get( i ) ) );
		// }

		if( sorted_list.size() == 1 ) {
			// elif idx == length - 1:
			value = sorted_list.get( 0 );
			uniq = "U";
		}
		else {
			// else:
			// curr = sorted_list[idx]
			// next = sorted_list[idx+1]

			String[] curr = sorted_list.get( 0 );
			String[] next = sorted_list.get( 1 );

			// # is unique?
			// if curr[1] != next[1]:

			if( !curr[ 1 ].equals( next[ 1 ] ) ) {
				value = curr;
				uniq = "U";
			}
			else {
				return null;
				// # count num
				// int cnt = 1;
				// for( int i = 1; i < sorted_list.size(); i++ ) {
				// if( curr[ 1 ].equals( sorted_list.get( i )[ 1 ] ) ) {
				// cnt += 1;
				//
				// // DEBUG
				// if( curr[ 0 ].length() > sorted_list.get( i )[ 0 ].length() ) {
				// curr = sorted_list.get( i );
				// }
				// // // DEBUG
				// // if( list.get( i )[ 0 ].equals( "W_C2T" ) ) {
				// // curr = list.get( i );
				// // }
				// }
				// else {
				// break;
				// }
				// }
				// value = curr;
				// uniq = "M" + cnt;
				// return null;
			}
		}

		// return toDebugString( uniq, value );
		return toSimple( uniq, value );
	}

	public String[] get_uniq( ArrayList<String[]> list ) {
		// System.out.println( "get_uniq " + list.size() + " " + list.toString() );

		if( list == null || list.size() == 0 ) {

			return null;
		}
		else if( list.size() == 1 ) {

			return list.get( 0 );
		}

		Comparator<String[]> distComparator = new Comparator<String[]>() {
			public int compare( String[] s1, String[] s2 ) {

				int rollno1 = Integer.parseInt( s1[ 1 ] );
				int rollno2 = Integer.parseInt( s2[ 1 ] );

				/* For ascending order */
				return rollno1 - rollno2;

				/* For descending order */
				// rollno2-rollno1;
			}
		};

		Collections.sort( list, distComparator );
		// list.sort( distComparator );

		if( !list.get( 0 )[ 1 ].equals( list.get( 1 )[ 1 ] ) ) {
			return list.get( 0 );
		}
		// System.out.println( "return: " + null );
		return null;
	}

	public String toSimple( String uniq, String[] value ) {
		StringBuilder bld = new StringBuilder();

		bld.append( uniq );
		bld.append( "\t" );
		for( int i = 0; i < value.length; i++ ) {
			if( i != 0 ) {
				bld.append( "\t" );
			}
			bld.append( value[ i ] );

		}
		return bld.toString();
	}

	public String toDebugString( String uniq, String[] value ) {
		StringBuilder bld = new StringBuilder();
		bld.append( "(('" );
		bld.append( uniq );
		bld.append( "', " );
		for( int i = 0; i < value.length; i++ ) {
			if( i != 0 ) {
				bld.append( ", " );
			}
			if( i == 2 || i == 4 ) {
				bld.append( value[ i ] );
			}
			else {
				bld.append( "'" );
				bld.append( value[ i ] );
				bld.append( "'" );
			}

		}
		bld.append( "'))" );
		return bld.toString();
	}
}
