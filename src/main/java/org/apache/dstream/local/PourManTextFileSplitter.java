package org.apache.dstream.local;

import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.apache.dstream.io.TextSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PourManTextFileSplitter {
	
	private static Logger logger = LoggerFactory.getLogger(PourManTextFileSplitter.class);

	public static Split[] generateSplits(TextSource source) {
		Path[] paths = source.getPath();
		int splitCount = Runtime.getRuntime().availableProcessors() / 2;

		long totalLength = 0;
		for (Path path : paths) {
			File file = path.toFile();
			totalLength += file.length();
		}
		long splits = totalLength / splitCount;
		List<Split> offsets = new ArrayList<Split>();

		FileInputStream fis = null;
		int commonSplitCounter = 0;
		try {
			
			for (Path path : paths) {
				File file = path.toFile();
				fis = new FileInputStream(file);
				long fileLength = file.length();

				int start = 0;
				for (int i = 0; i < splitCount && fis.available() > 0; i++) {
					fis.skip(splits);
					int overlayCounter = 0;
					int newLine = 0;
					while (fis.available() > 0 && newLine != '\n') {
						overlayCounter++;
						newLine = fis.read();
					}
					int length = (int) (splits + overlayCounter);
					if (!(start >= fileLength)) {
						if (logger.isDebugEnabled()){
							logger.debug("Split " + commonSplitCounter++ + " - " + start + "/" + length);
						}
						
						Split split = new Split(file, start, length);
						offsets.add(split);
						start += length;
					}
				}
				fis.close();
			}
		} catch (Exception e) {
			try {
				fis.close();
			} catch (Exception e2) {
				// ignore
			}
		}
		return offsets.toArray(new Split[]{});
	}
}
