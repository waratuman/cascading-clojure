package org.parsimonygroup;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class SortedDeque<T extends Comparable> {

	ResizableArray<T> queue;
	LinkedList<T> list;
	private int sampleSize;
	

	public SortedDeque(int sampleSize) {
		this.sampleSize =sampleSize;
		queue = new ResizableArray<T>(sampleSize);
		list = new LinkedList<T>();
	}

	public void addElement(T n) {
		queue.addElement(n);
		addSorted(n);
	}

	public T addElementRolling(T n) {
		T commingOut = (T) queue.addElementRolling(n);
		list.remove(commingOut);
		addSorted(n);
		return commingOut;
	}

	private void addSorted(T n) {
		
		int index = list.indexOf(n);

		if (index < 0) {
			list.add(-index - 1, n);
		} else {
			list.add(index, n);
		}
	}

	public T getElementAtSortedIndex(int i) {
		return list.get(i);
	}

	public int getNumElements() {
		return queue.getNumElements();
	}
	
	public boolean isFull() {
		  return queue.getNumElements() == sampleSize;
	}
	
	public Object[] getElements() {
		return queue.getElements();
	}
}
