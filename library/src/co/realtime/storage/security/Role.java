package co.realtime.storage.security;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
/*
public class Role {
	private String name;
	private HashSet<Policy> policies;
	
	public String getName() { return name; }
	public void setName(String name) { this.name = name; }	
	
	public HashSet<Policy> getPolicies() { return policies; }
	public void setPolicies(HashSet<Policy> policies) { this.policies = policies; }
	
	public Role() {
		this.name = "";
		this.policies = new HashSet<Policy>();
	}
	
	public Role(String name) {
		this();
		this.name = name;
	}
	
	public Role(String name, HashSet<Policy> policies) {
		this.name = name;
		this.policies = policies;
	}
	
	public void addPolicy(Policy policy) {
		policies.add(policy);
	}
	
	public void removePolicy(Policy policy) {
		policies.remove(policy);
	}
}
*/
import java.util.Map;

/**
 * Defines a role which is a set of rules that control access to the Storage database.
 * 
 * @author RTCS Development team
 *
 */
public class Role implements IPolicy {
	private String name;
	private Policies policies;
	
	/**
	 * The name of the role.
	 * 
	 * @return The name of the role.
	 */
	public String getName() { return name; }
	
	/**
	 * Assigns a name to the role.
	 * 
	 * @param name The name of the role.
	 */
	public void setName(String name) { this.name = name; }	
	
	/**
	 * Retrieves the policies related to the database.
	 * 
	 * @return A set of database policies.
	 */
	public HashSet<IPolicy> getDatabasePolicies() { return policies.getDatabase(); }
	
	/**
	 * Retrieves the policies related to the tables.
	 * 
	 * @return A set of table policies.
	 */
	public HashSet<IPolicy> getTablePolicies() { return policies.getTables(); }
	
	/**
	 * Creates an empty Role instance.
	 */
	public Role() {
		this.name = "";
		this.policies = new Policies();
	}
	
	/**
	 * Creates a Role instance with a given name.
	 * 
	 * @param name The name of the role.
	 */
	public Role(String name) {
		this();
		this.name = name;
	}
	
	/**
	 * Creates a Role instance with a given name and its policies.
	 * 
	 * @param name The name of the role.
	 * @param policies The set of rules that control access to the Storage database.
	 */
	public Role(String name, Policies policies) {
		this.name = name;
		this.policies = policies;
	}
	
	/**
	 * Adds policy specific of a table.
	 * @param policy The table access rules.
	 */
	public void addPolicy(TablePolicy policy) {
		policies.add(policy);
	}

	/**
	 * Adds policy specific to the database.
	 * @param policy The database access rules.
	 */ 
	public void addPolicy(DatabasePolicy policy) {
		policies.add(policy);
	}

	/**
	 * Removes a policy specific of a table.
	 * @param policy The database access rules.
	 */ 
	public void removePolicy(TablePolicy policy) {
		policies.add(policy);
	}

	/**
	 * Removes policy specific to the database.
	 * @param policy The database access rules.
	 */ 
	public void removePolicy(DatabasePolicy policy) {
		policies.remove(policy);
	}
	
	@Override
	public Map<String, Object> map() {
		Map<String, Object> roleMap = new HashMap<String, Object>();
		roleMap.put("role", getName());
		roleMap.put("policies", policies.map());	
		
		return roleMap;
	}
	
	@SuppressWarnings("unchecked")
	public final static Role unmap(Map<String, Object> roleMap) {
		LinkedHashMap<String, Object> policiesMap = (LinkedHashMap<String, Object>) roleMap.get("policies");
		return new Role((String) roleMap.get("role"), Policies.unmap(policiesMap));
	}
}