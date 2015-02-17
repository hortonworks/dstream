package org.apache.dstream.io;

import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

public class ListStreamableSourceTests {

	@Test
	public void validateSplitList(){
		List<Integer> intList = Arrays.asList(new Integer[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2, 3});
		ListStreamableSource<Integer> source =  ListStreamableSource.<Integer>create(intList, 5);
		
		Integer[] result = source.toStream(0).toArray(Integer[]::new);
		Assert.assertArrayEquals(new Integer[]{1, 2, 3}, result);
		
		result = source.toStream(1).toArray(Integer[]::new);
		Assert.assertArrayEquals(new Integer[]{4, 5, 6}, result);
		
		result = source.toStream(2).toArray(Integer[]::new);
		Assert.assertArrayEquals(new Integer[]{7,8,9}, result);
		
		result = source.toStream(3).toArray(Integer[]::new);
		Assert.assertArrayEquals(new Integer[]{1, 2, 3}, result);
	}
}
