package bio.align;

import java.util.ArrayList;

public class MyStringBuffer {
	ArrayList<String> stringBuffer;
	ArrayList<Integer> index;

	public MyStringBuffer( ArrayList<String> buf, ArrayList<Integer> index ) {
		this.stringBuffer = buf;
		this.index = index;
	}

	public String substring( int start, int end, boolean debug ) {
		StringBuilder bld = new StringBuilder();

		IntPair startPos = getListIndex( start );
		IntPair endPos = getListIndex( end );

		if( debug ) {
			System.out.println( "start: " + startPos.i + " " + startPos.j );
			System.out.println( "end: " + endPos.i + " " + endPos.j );
		}

		if( startPos.i == endPos.i ) {
			return stringBuffer.get( startPos.i ).substring( startPos.j, endPos.j );
		}

		bld.append( stringBuffer.get( startPos.i ).substring( startPos.j ) );
		for( int i = startPos.i + 1; i < endPos.i; i++ ) {
			bld.append( stringBuffer.get( i ) );
		}
		bld.append( stringBuffer.get( endPos.i ).substring( 0, endPos.j ) );

		return bld.toString();
	}

	public IntPair getListIndex( int pos ) {
		int size = index.size();
		int idx = findEqualOrLess( pos, 0, size - 1 );

		return new IntPair( idx, pos - index.get( idx ) );
	}

	public int findEqualOrLess( int pos, int start, int end ) {
		if( start >= end ) {
			if( index.get( start ) <= pos ) {
				return start;
			}
			else {
				return start - 1;
			}
		}
		int mid = ( start + end ) / 2;

		int midValue = index.get( mid );

		if( midValue > pos ) {
			// search left
			return findEqualOrLess( pos, start, mid - 1 );
		}
		else if( midValue < pos ) {
			return findEqualOrLess( pos, mid + 1, end );
		}
		else {
			return mid;
		}
	}

	public static class IntPair {
		int i;
		int j;

		IntPair( int i, int j ) {
			this.i = i;
			this.j = j;
		}
	}

	public static void main( String args[] ) {
		String temp = "TTCACCATTTTTCTTTTCGTTAACTTGCCGTCAGCCTTTTCTTTGACCTC";
		int offset = 0;

		ArrayList<String> list = new ArrayList<String>();
		ArrayList<Integer> index = new ArrayList<Integer>();
		list.add( temp );
		index.add( offset );

		offset += temp.length();

		temp = "TTCTTTCTGTTCATGTGTATTTGCTGTCTCTTAGCCCAGACTTCCCGTGT";

		list.add( temp );
		index.add( offset );

		offset += temp.length();

		temp = "CCTTTCCACCGGGCCTTTGAGAGGTCACAGGGTCTTGATGCTGTGGTCTT";

		list.add( temp );
		index.add( offset );

		offset += temp.length();

		System.out.println( list.toString() );
		System.out.println( index.toString() );

		MyStringBuffer buf = new MyStringBuffer( list, index );

		IntPair p = buf.getListIndex( 1 );
		System.out.println( p.i + " " + p.j );

		p = buf.getListIndex( 4 );
		System.out.println( p.i + " " + p.j );

		p = buf.getListIndex( 10 );
		System.out.println( p.i + " " + p.j );

		int beginIndex = 1;
		int endIndex = 101;
		System.out.println( buf.substring( beginIndex, endIndex, true ) );
		System.out.println(
				"TTCACCATTTTTCTTTTCGTTAACTTGCCGTCAGCCTTTTCTTTGACCTCTTCTTTCTGTTCATGTGTATTTGCTGTCTCTTAGCCCAGACTTCCCGTGTCCTTTCCACCGGGCCTTTGAGAGGTCACAGGGTCTTGATGCTGTGGTCTT"
						.substring( beginIndex, endIndex ) );
	}
}
