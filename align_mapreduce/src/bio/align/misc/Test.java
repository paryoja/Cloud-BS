package bio.align.misc;

import java.io.File;

public class Test {
	public static void main( String args[] ) {
		String temp = " \"(hello)\" \"(hi)\"";

		for( String x : temp.split( "\\)\"" ) ) {
			System.out.println( x );
		}

		File folder = new File( "E:/backups/downloads" );
		for( final File fileEntry : folder.listFiles() ) {
			if( !fileEntry.isDirectory() ) {
				System.out.println( fileEntry.getName().startsWith( "iv" ) );
			}
		}

	}
}
