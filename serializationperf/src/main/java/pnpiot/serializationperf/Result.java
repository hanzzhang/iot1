package pnpiot.serializationperf;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

public class Result {
	public static void writeToFile(String filename, long elapsedTime) throws Exception {
		String filePath = System.getProperty("user.dir") + "\\" + filename + ".csv";
		PrintWriter writer = new PrintWriter(filePath, "UTF-8");

		System.out.println("PerfCount, " + filename);
		writer.println("PerfCount, " + filename);

		System.out.println("outer loops, " + Config.outerloop);
		writer.println("outer loops, " + Config.outerloop);

		System.out.println("inner loops, " + Config.innerloop);
		writer.println("inner loops, " + Config.innerloop);

		System.out.println("objects, " + Config.innerloop * Config.outerloop);
		writer.println("objects, " + Config.innerloop * Config.outerloop);

		System.out.println("Mini seconds elapsed , " + elapsedTime);
		writer.println("Mini seconds elapsed , " + elapsedTime);

		OperatingSystemMXBean operatingSystemMXBean = ManagementFactory.getOperatingSystemMXBean();
		for (Method method : operatingSystemMXBean.getClass().getDeclaredMethods()) {
			method.setAccessible(true);
			if (method.getName().startsWith("get") && Modifier.isPublic(method.getModifiers())) {
				Object value = method.invoke(operatingSystemMXBean);
				System.out.println(method.getName().substring(3) + " , " + value);
				writer.println(method.getName().substring(3) + " , " + value);
			}
		}
		writer.close();
	}

	public static void writeToSampleFile(String filename, int intVal) throws Exception {
		writeToSampleFile(filename, Integer.toString(intVal));
	}

	public static void writeToSampleFile(String filename, long longVal) throws Exception {
		writeToSampleFile(filename, Long.toString(longVal));
	}

	public static void writeToSampleFile(String filename, String inputStr) throws Exception {
		String filePath = System.getProperty("user.dir") + "\\" + filename + ".sample" + ".txt";
		// PrintWriter writer = new PrintWriter(filePath, "UTF-8");
		PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(filePath, true)));
		writer.print(inputStr);
		System.out.print(inputStr);
		writer.close();
	}

	public static void cleanSampleFile(String filename) throws Exception {
		String filePath = System.getProperty("user.dir") + "\\" + filename + ".sample" + ".txt";
		PrintWriter writer = new PrintWriter(filePath, "UTF-8");
		writer.println(filename + " Deserialized Data Sample");
		writer.close();
	}
}
