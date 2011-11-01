package eu.stratosphere.pact.iterative.nephele.cache;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import eu.stratosphere.pact.iterative.AccessibleConcurrentHashMap;

public class CacheStore {
	@SuppressWarnings("rawtypes")
	private final static ConcurrentMap<String, ConcurrentMap> store = 
			new ConcurrentHashMap<String, ConcurrentMap>();
	
	@SuppressWarnings("unchecked")
	public static <K, V> ConcurrentMap<K, V> getCache(String cacheId, Class<K> keyClass, Class<V> valueClass) {
		ConcurrentMap<K, V> entry = store.get(cacheId);
		return entry;
	}
	
	public static <K, V> void createCache(String cacheId, Class<K> keyClass, Class<V> valueClass) {
		if(!store.containsKey(cacheId)) {
			store.putIfAbsent(cacheId, new AccessibleConcurrentHashMap<K, V>(10000));
		} else {
			throw new RuntimeException("Store already exists");
		}
	}
}
