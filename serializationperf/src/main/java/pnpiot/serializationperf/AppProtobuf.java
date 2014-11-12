package pnpiot.serializationperf;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;

import com.google.protobuf.InvalidProtocolBufferException;

public class AppProtobuf {
	static String className = null;
	static {
		className = new Object() {
		}.getClass().getEnclosingClass().getSimpleName();
	}

	public static void main(String[] args) throws Exception {
		if (args.length > 0) {
			Config.outerloop = Math.abs(Integer.parseInt(args[0]));
		}
		Result.cleanSampleFile(className);
		long startTime = System.currentTimeMillis();

		for (int i = 0; i < Config.outerloop; i++) {
			LocationProtobuf.Location.Builder builder = null;
			ByteArrayOutputStream out = new ByteArrayOutputStream();

			for (int j = 0; j < Config.innerloop; j++) {
				builder = createLocationBuilder(j);
				builder.setTimeStamp(new Date().getTime());
				builder.build().writeDelimitedTo(out);
			}

			out.flush();
			out.close();
			displaySerializedRecord(out);
		}

		long elapsedTime = System.currentTimeMillis() - startTime;
		Result.writeToFile(className, elapsedTime);
	}

	private static LocationProtobuf.Location.Builder createLocationBuilder(int i) {
		LocationProtobuf.Location.Builder builder = LocationProtobuf.Location.newBuilder();
		builder.setTimeStamp(new Date().getTime());
		builder.setFixType(11100000 + i);
		builder.setLatitude(22200000 + i);
		builder.setLongitude(33300000 + i);
		builder.setHeading(44400000 + i);
		builder.setAltitude(55500000 + i);
		builder.setSpeed(66600000 + i);
		return builder;
	}

	private static void displaySerializedRecord(ByteArrayOutputStream out) throws Exception {
		int writeInterval = Config.innerloop / 5; // write 5 records for each innerloop
		InputStream input = new ByteArrayInputStream(out.toByteArray());
		LocationProtobuf.Location.Builder builder = LocationProtobuf.Location.newBuilder();
		for (int j = 0; builder.mergeDelimitedFrom(input); j++) {
			if (j % writeInterval == 0) {
				Result.writeToSampleFile(className, builder.getTimeStamp());
				Result.writeToSampleFile(className, ", ");
				Result.writeToSampleFile(className, builder.getFixType());
				Result.writeToSampleFile(className, ", ");
				Result.writeToSampleFile(className, builder.getLatitude());
				Result.writeToSampleFile(className, ", ");
				Result.writeToSampleFile(className, builder.getLongitude());
				Result.writeToSampleFile(className, ", ");
				Result.writeToSampleFile(className, builder.getHeading());
				Result.writeToSampleFile(className, ", ");
				Result.writeToSampleFile(className, builder.getAltitude());
				Result.writeToSampleFile(className, ", ");
				Result.writeToSampleFile(className, builder.getSpeed());
				Result.writeToSampleFile(className, "\r\n");
			}
		}
		Result.writeToSampleFile(className, "\r\n");
	}
}
// to run the jar file in command line for 3000 loops:
// java -cp perf.jar pnpiot.serializationperf.AppProtobuf 3000

