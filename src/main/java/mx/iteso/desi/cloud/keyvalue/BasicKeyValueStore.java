package mx.iteso.desi.cloud.keyvalue;

import java.util.Set;

public abstract class BasicKeyValueStore implements IKeyValueStorage {

	@Override
	public void addToSet(String keyword, Set<String> values) {
		for (String str: values)
			addToSet(keyword, str);
	}

}
