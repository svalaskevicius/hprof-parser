package edu.tufts.eaftan.hprofparser.parser.datastructures;

public class InstanceFieldWithValue<T> {
	public final InstanceField field;
	public final T value;

	public InstanceFieldWithValue(InstanceField field, T value) {
		this.value = value;
		this.field = field;
	}

	@Override
	public String toString() {
		return value.toString();
	}
}
