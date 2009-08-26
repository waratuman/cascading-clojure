package org.parsimonygroup;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RollingWindowInputFormat extends MultiFileInputFormat {

	public static final String ROLLING_WINDOW_SIZE_PROPERTY = "rolling.window.size";
	@Override
	public RecordReader getRecordReader(InputSplit in, JobConf job,
			Reporter reporter) throws IOException {
		reporter.setStatus(in.toString());
		return new AllFilesRecordReader(job, (MultiFileSplit) in);
	}

	@Override
	public InputSplit[] getSplits(JobConf job, int numSplits)
			throws IOException {
		/*
		 * get rolling windowed paths calculate total size of each make
		 * multifilesplits to return
		 */
		int windowSize = getWindowSize(job);
		List<Path> listPaths = (List<Path>) new Transform<FileStatus, Path>() {

			@Override
			public Path closure(FileStatus in) {
				return in.getPath();
			}

		}.apply(Arrays.asList(listStatus(job)));

		Path[] paths = listPaths.toArray(new Path[listPaths.size()]);
		List<Object[]> windowedPaths = getWindows(windowSize, paths);
		return makeMultiFileSplitsFromPaths(job, windowedPaths).toArray(
				new MultiFileSplit[windowedPaths.size()]);
	}

	private int getWindowSize(JobConf job) {
		return Integer.valueOf(job.get(ROLLING_WINDOW_SIZE_PROPERTY, ""));
	}

	private List<MultiFileSplit> makeMultiFileSplitsFromPaths(JobConf job,
			List<Object[]> windowedPaths) throws IOException {
		List<MultiFileSplit> result = new ArrayList<MultiFileSplit>();
		for (Object[] paths : windowedPaths) {
			Path[] pathArr = new Path[paths.length];
			System.arraycopy(paths, 0, pathArr, 0, paths.length);
			result
					.add(new MultiFileSplit(job, pathArr, findSizes(pathArr,
							job)));

		}
		return result;
	}

	private long[] findSizes(Path[] paths, JobConf job) throws IOException {

		long[] result = new long[paths.length];
		for (int i = 0; i < paths.length; i++) {
			result[i] = paths[i].getFileSystem(job).getContentSummary(paths[i])
					.getLength();
		}
		return result;
	}

	public List<Object[]> getWindows(int windowSize, Path[] paths) {
		List<Object[]> result = new ArrayList<Object[]>();
		Arrays.sort(paths);
		SortedDeque<Path> deque = new SortedDeque<Path>(windowSize);

		for (int i = 0; i < paths.length; i++) {
			if (!deque.isFull()) {
				deque.addElement(paths[i]);
			} else {
				result.add(deque.getElements());
				deque.addElementRolling(paths[i]);
			}
		}
		result.add(deque.getElements());
		return result;
	}

}
