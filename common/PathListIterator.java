package common;
import java.util.*;

public class PathListIterator implements Iterator<String>{
	
	public Iterator<String> iterator;
	public PathListIterator(Iterator<String> iterator) {
		this.iterator = iterator;
	}

	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		
		return iterator.hasNext();
	}

	@Override
	public String next() {
		// TODO Auto-generated method stub
		return iterator.next();
	}

	
}
