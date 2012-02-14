package eu.stratosphere.pact.programs.pagerank_old.tasks;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import eu.stratosphere.pact.common.type.Value;

public class StringSet implements Value {
	
	Set<String> set;
	
	public StringSet() {
		
	}
	
	public StringSet(Set<String> set) {
		this.set = set;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		int size = set.size();
		out.writeInt(set.size());
		
		Iterator<String> iter = set.iterator();
		for (int i = 0; i < size; i++) {
//			out.writeUTF(iter.next());
			byte[] bytes = iter.next().getBytes();
			out.writeInt(bytes.length);
			out.write(bytes);
		}
	}

	@Override
	public void read(DataInput in) throws IOException {
		int size = in.readInt();
		set = new HashSet<String>(size);
		byte[] bytes = new byte[100];
		
		for (int i = 0; i < size; i++) {
//			try {
//				list.add(in.readUTF());
//			} catch (EOFException ex) {
//				System.out.println(size + "::" + i + "\t" + ex);
//				ex.printStackTrace();
//			}
			int byteSize = in.readInt();
			if(byteSize > 0) {
				if(bytes.length < byteSize) {
					bytes = new byte[byteSize];
				}
				try {
					in.readFully(bytes, 0 , byteSize);
				} catch(EOFException ex) {
					System.out.println(size + "::" + i + "\t" + ex);
					System.out.println(byteSize);
					ex.printStackTrace();
				}
				set.add(new String(bytes, 0, byteSize));
			} else {
				set.add(new String());
			}
		}
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("[ " + set.size() + " ]:");
		for (String string : set) {
			builder.append(string);
			builder.append(" ");
		}
		
		return builder.toString();
	}

	public Set<String> getValue() {
		return set;
	}
}
