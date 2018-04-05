package bio.align.misc;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Util {

	public static void saveFileInHDFS( String localTempPath, String remoteOutPath, FileSystem fs ) throws IOException {
		Path outFile = new Path( remoteOutPath );
		if( fs.exists( outFile ) ) {
			// delete outFile
			fs.delete( outFile, false );
		}
		FSDataOutputStream out = fs.create( outFile );

		// input stream
		FileInputStream inFile = new FileInputStream( localTempPath );
		int bytesRead;
		byte[] buffer = new byte[ 1024 ];
		while( ( bytesRead = inFile.read( buffer ) ) > 0 ) {
			out.write( buffer, 0, bytesRead );
		}
		inFile.close();
		out.close();
	}

	public static long size( String input ) throws IOException {
		Path p = new Path( input );
		FileSystem fs = p.getFileSystem( new Configuration() );
		return fs.getContentSummary( p ).getLength();
	}

	public static String select_and_find_uniq_alignment( ArrayList<String[]> list ) {

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
				System.out.println( l[ 0 ] );
				for( String[] tmp : list ) {
					System.out.println( Arrays.toString( tmp ) );
				}
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

	public static String[] get_uniq( ArrayList<String[]> list ) {
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

	public static String toSimple( String uniq, String[] value ) {
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

}
