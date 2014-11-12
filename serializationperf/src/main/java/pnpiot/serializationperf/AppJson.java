package pnpiot.serializationperf;

import java.util.Date;

import com.fasterxml.jackson.databind.ObjectMapper;

import pnpiot.serializationperf.LocationJson;

public class AppJson {
	// static String jsonInputStr = "{\"timeStamp\":101,\"fixType\":102,\"latitude\":400,\"longitude\":500,\"heading\":300,\"altitude\":100,\"speed\":255}";
	static String className;
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
			ObjectMapper mapper = new ObjectMapper();
			StringBuilder jsonString = new StringBuilder();

			for (int j = 0; j < Config.innerloop; j++) {
				LocationJson jsonObject = createLocationRecord(j);
				// LocationJson jsonObject = mapper.readValue(jsonInputStr, LocationJson.class);
				jsonString.append(mapper.writeValueAsString(jsonObject) + "\r\n");
			}

			String outputStr = jsonString + "\r\n";
			Result.writeToSampleFile(className, outputStr);
		}

		long elapsedTime = System.currentTimeMillis() - startTime;
		Result.writeToFile(className, elapsedTime);
	}

	private static LocationJson createLocationRecord(int i) {
		LocationJson jsonObject = new LocationJson();
		jsonObject.setTimeStamp(new Date().getTime());
		jsonObject.setFixType((short) (i % 256));
		jsonObject.setLatitude(200000 + i);
		jsonObject.setLongitude(300000 + i);
		jsonObject.setHeading(400000 + i);
		jsonObject.setAltitude(500000 + i);
		jsonObject.setSpeed((short) (i % 256));
		return jsonObject;
	}
}

// To generate the jar files with dependencies
// In Eclipse, File->Export->Java->Runnable Jar File
// for Launch configuration: select JsonApp - serializationperf
// Set destination as serializationperf\perf.jar

// To run the jar files in Command Prompt
// Start Command Prompt as admin
// java -jar jsonjackson.jar 20
// open jsonjackson.txt in the current working folder

// To run the jar files in PowerShell
// Start PowerShell as admin
// &"C:\Program Files\Java\jdk1.8.0_05\bin\java.exe" -jar jsonjackson.jar 2000
// open jsonjackson.txt in the current working folder

// To use measure-command
// Start PowerShell as admin
// measure-command { &"C:\Program Files\Java\jdk1.8.0_05\bin\java.exe" -jar jsonjackson.jar 2000}
// open jsonjackson.txt in the current working folder

// to run the jar file in command line for 3000 loops:
// java -cp perf.jar pnpiot.serializationperf.AppJson 3000

