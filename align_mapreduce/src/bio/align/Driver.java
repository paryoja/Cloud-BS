package bio.align;

import org.apache.hadoop.util.ProgramDriver;

import bio.align.misc.BowtieMain;
import bio.align.misc.FromBowtieMain;
import bio.align.misc.WithoutBowtieMain;

public class Driver {
	public static void main( String argv[] ) {
		int exitCode = -1;
		ProgramDriver pgd = new ProgramDriver();

		try {
			pgd.addClass( "Align", AlignMain.class, "Main Function for the align" );
			pgd.addClass( "Bowtie", BowtieMain.class, "Bowtie only job" );
			pgd.addClass( "From", FromBowtieMain.class, "Align from the results of bowtie" );
			pgd.addClass( "WithoutBowtie", WithoutBowtieMain.class, "Align without calling bowtie" );

			pgd.driver( argv );

			// Success
			exitCode = 0;
		}

		catch( Throwable e ) {
			e.printStackTrace();
		}

		System.exit( exitCode );
	}
}
