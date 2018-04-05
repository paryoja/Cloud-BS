package bio.align.misc;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BowtieReducer extends Reducer<Text, Text, Text, Text> {
	public static Text outKey = new Text();
	public static Text outValue = new Text();

	@Override
	public void reduce( Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException {
		StringBuilder val = new StringBuilder();
		boolean first = true;
		String orig = null;
		for( Text value : values ) {
			String temp = value.toString();

			if( temp.startsWith( "(" ) ) {
				if( !first ) {
					val.append( " " );
				}
				first = false;
				val.append( "\"" );
				val.append( temp );
				val.append( "\"" );
			}
			else {
				orig = temp;
			}

		}
		outValue.set( orig + ":" + val.toString() );
		context.write( key, outValue );
	}
}
