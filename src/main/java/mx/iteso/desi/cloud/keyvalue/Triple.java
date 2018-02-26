package mx.iteso.desi.cloud.keyvalue;

/**
 * Simple class representing an RDF triple <subject, predicate, object>
 * and providing accessors for it
 * 
 * @author zives
 *
 */
public class Triple {
	String[] contents = new String[3];

	/**
	 * Empty triple
	 */
	public Triple() {
		
	}
	
	/**
	 * Pre-initialized triple
	 * 
	 * @param subject
	 * @param predicate
	 * @param object
	 */
	public Triple(String subject, String predicate, String object) {
		contents[0] = subject;
		contents[1] = predicate;
		contents[2] = object;
	}
	
	/**
	 * Fetch by index
	 * 
	 * @param i
	 * @return
	 */
	public String get(int i) {
		if (i >= 0 && i < 3)
			return contents[i];
		else
			throw new RuntimeException("Out of bounds exception");
	}
	
	/**
	 * Set content by index
	 * 
	 * @param i
	 * @param content
	 */
	public void set(int i, String content) {
		if (i >= 0 && i < 3)
			contents[i] = content;
		else
			throw new RuntimeException("Out of bounds exception");
	}
	
	/**
	 * Return the object of the triple
	 * @return
	 */
	public String getObject() {
		return contents[2];
	}
	
	/**
	 * Return the predicate of the triple
	 * @return
	 */
	public String getPredicate() {
		return contents[1];
	}
	
	/**
	 * Return the subject of the triple
	 * @return
	 */
	public String getSubject() {
		return contents[0];
	}
	
	/**
	 * Initialize/replace the object
	 * @param content
	 */
	public void setObject(String content) {
		contents[2] = content;
	}
	
	/**
	 * Initialize/replace the predicate
	 * @param content
	 */
	public void setPredicate(String content) {
		contents[1] = content;
	}
	
	/**
	 * Initialize/replace the subject
	 * @param content
	 */
	public void setSubject(String content) {
		contents[0] = content;
	}
	
	/**
	 * Accessor to the entire content
	 * @return
	 */
	public String[] getCollection() {
		return contents;
	
	}

	/**
	 * To String Method
	 */
	public String toString(){
		String res = "Triplet:\n0:" + contents[0] + "\n1:" + contents[1] + "\n2:" + contents[2] + "\n"; 
		return res;
	}
}
