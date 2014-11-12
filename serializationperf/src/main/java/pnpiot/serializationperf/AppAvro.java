package pnpiot.serializationperf;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
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

public class AppAvro {
	static String AvroSchemaStr;
	static String className = null;
	static {
		className = new Object() {
		}.getClass().getEnclosingClass().getSimpleName();

		AvroSchemaStr = "{`type`: `record`,`name`: `Car.Location`,`fields`: [{`name`: `timeStamp`,`type`: `long`},{`name`: `fixType`,`type`: `int`},{`name`: `latitude`,`type`: `int`},{`name`: `longitude`,`type`: `int`},{`name`: `heading`,`type`: `int`},{`name`: `altitude`,`type`: `int`},{`name`: `speed`,`type`: `int`}]}";
		AvroSchemaStr = AvroSchemaStr.replace('`', '"');
	}

	public static void main(String[] args) throws Exception {
		if (args.length > 0) {
			Config.outerloop = Math.abs(Integer.parseInt(args[0]));
		}
		Result.cleanSampleFile(className);
		Schema schema = new Schema.Parser().parse(AvroSchemaStr);		
		long startTime = System.currentTimeMillis();
		
		for (int i = 0; i < Config.outerloop; i++) {
			GenericRecord genericRecord = createLocationRecord(schema, i);
			ByteArrayOutputStream outStream = new ByteArrayOutputStream();
			BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outStream, null);
			DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);

			for (int j = 0; j < Config.innerloop; j++) {
				genericRecord.put("timeStamp", new Date().getTime());
				datumWriter.write(genericRecord, encoder);
			}

			encoder.flush();
			displaySerializedRecord(outStream, schema);
			outStream.reset();
			outStream.close();
		}
		
		long elapsedTime = System.currentTimeMillis() - startTime;
		Result.writeToFile(className, elapsedTime);
	}

	private static GenericRecord createLocationRecord(Schema schema, int i) {
		GenericRecord genericRecord = new GenericData.Record(schema);
		genericRecord.put("timeStamp", new Date().getTime());
		genericRecord.put("fixType", i % 256);
		genericRecord.put("latitude", 200000 + i);
		genericRecord.put("longitude", 300000 + i);
		genericRecord.put("heading", 400000 + i);
		genericRecord.put("altitude", 500000 + i);
		genericRecord.put("speed", i % 256);
		return genericRecord;
	}

	private static void displaySerializedRecord(ByteArrayOutputStream outStream, Schema schema) throws Exception {
		byte[] serializedBytes = outStream.toByteArray();
		BinaryDecoder decoder = null;
		decoder = DecoderFactory.get().binaryDecoder(serializedBytes, decoder);
		GenericDatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
		GenericRecord rec = null;
		int writeInterval = Config.innerloop / 5; // write 5 records for each innerloop
		for (int j = 0; !decoder.isEnd(); j++) {
			rec = reader.read(rec, decoder);
			if (j % writeInterval == 0) {
				String outputStr = rec.toString() + "\r\n";
				Result.writeToSampleFile(className, outputStr);
			}
		}
		Result.writeToSampleFile(className, "\r\n");
	}
}

// to run the jar file in command line for 3000 loops:
// java -cp perf.jar pnpiot.serializationperf.AppAvro 3000
