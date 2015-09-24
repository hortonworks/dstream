package dstream.tez.examples;

import java.io.File;

import org.apache.commons.io.FileUtils;

public class SampleUtils {

	public static void clean(String applicationName){
		try {
			File workDir = new File(System.getProperty("user.dir"));
			if (workDir.isDirectory()){
				for (String sub : workDir.list()) {
					if (sub.startsWith(applicationName)){
						File file = new File(workDir, sub);
						FileUtils.deleteDirectory(file);
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
