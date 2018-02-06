package mx.iteso.desi.cloud.keyvalue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MemStorage extends BasicKeyValueStore {
	Map<String,List<String>> storage = new HashMap<>();

	@Override
	public Set<String> get(String search) {
		Set<String> theSet = new HashSet<String>();
		if (storage.containsKey(search))
			theSet.addAll(storage.get(search));
		
		return theSet;
	}

	@Override
	public boolean exists(String search) {
		return storage.containsKey(search);
	}

	@Override
	public Set<String> getPrefix(String search) {
		Set<String> results = new HashSet<String>();
		
		for (String k: storage.keySet()) {
			if (k.startsWith(search))
				results.addAll(storage.get(k));
		}
		return results;
	}

	@Override
	public void addToSet(String keyword, String value) {
		if (!storage.containsKey(keyword))
			storage.put(keyword, new ArrayList<String>());
		
		storage.get(keyword).add(value);
	}

	@Override
	public void put(String keyword, String value) {
		addToSet(keyword, value);
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void sync() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean isCompressible() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsMoreThan256Attributes() {
		return true;
	}

	public boolean supportsPrefixes() {
		return true;
	}

}
