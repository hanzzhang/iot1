package pnpiot.serializationperf;

public class App {

	public static void main(String[] args) throws Exception {
		String appString = "avro";
		if (args.length > 1) {
			appString = args[1];
		}

		switch (appString) {
		case "avro":
			AppAvro.main(args);
			break;
		case "json":
			AppJson.main(args);
			break;
		case "pb":
			AppProtobuf.main(args);
			break;
		default:
			break;
		}
	}
}
