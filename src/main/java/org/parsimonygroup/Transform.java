package org.parsimonygroup;

import java.util.Collection;
import java.util.ArrayList;

public abstract class Transform<T, V> {
		public abstract V closure(T in);
		public Collection<V> apply(Collection<T> values) {
			Collection<V> result = new ArrayList<V>();
			for (T val : values) {
				V x = closure(val);
				if(x != null) result.add(x);
			}
			return result;
		}
	}