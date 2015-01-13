package co.realtime.storage.security;

import java.util.AbstractMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Holds the database and table policies. Used to create a role or for authenticating a token.
 *  
 * @author RTCS Development team
 *
 */
public final class Policies implements IPolicy {	
	private HashSet<IPolicy> database;
	private HashSet<IPolicy> tables;

	/*
	 * Creates an instance of Policies.
	 */
	public Policies() {
		database = new HashSet<IPolicy>();
		tables = new HashSet<IPolicy>();
	}
	
//	public Policies(HashSet<DatabasePolicy> database, HashSet<TablePolicy> tables) {
//		this.database = database;
//		this.tables = tables;
//	}
	
	/**
	 * Retrieves the database policies.
	 * @return The database policies.
	 */
	public HashSet<IPolicy> getDatabase() {
		return database;
	}

	public void setDatabase(HashSet<DatabasePolicy> database) {
		this.database = new HashSet<IPolicy>(database);
	}
	
	/**
	 * Retrieves the table's policies.
	 * @return The table's policies.
	 */
	public HashSet<IPolicy> getTables() {
		return tables;
	}

//	public void setTables(HashSet<TablePolicy> tables) {
//		this.tables = tables;
//	}

	/**
	 * Adds a database policy.
	 * 
	 * @param dbPolicy The database policy.
	 * @return True if the policy was added.
	 */
	public boolean add(DatabasePolicy dbPolicy) {
	    boolean modified;
	    if (modified = !database.contains(dbPolicy)) {
	    	database.add(dbPolicy);
	    }
	    return modified;		
	}

	/**
	 * Adds a table policy.
	 * 
	 * @param tblPolicy The table policy.
	 * @return True if the policy was added.
	 */	
	public boolean add(TablePolicy tblPolicy) {
	    boolean modified;
	    if (modified = !tables.contains(tblPolicy)) {
	    	tables.add(tblPolicy);
	    }
	    return modified;		
	}
	
	/**
	 * Removes a database policy.
	 * 
	 * @param dbPolicy The database policy.
	 * @return True if the policy was removed.
	 */
	public boolean remove(DatabasePolicy dbPolicy) {
		return database.remove(dbPolicy);
	}
	
	/**
	 * Removes a table policy.
	 * 
	 * @param tblPolicy The table policy.
	 * @return True if the policy was removed.
	 */
	public boolean remove(TablePolicy tblPolicy) {
		return tables.remove(tblPolicy);
	}
	
	/**
	 * Indicates if the database policy is contained in the list.
	 * 
	 * @param dbPolicy The database policy.
	 * @return True if the policy is contained in the list.
	 */
	public boolean contains(DatabasePolicy dbPolicy) {
		return database.contains(dbPolicy);
	}	
	
	/**
	 * Indicates if the table policy is contained in the list.
	 * 
	 * @param tblPolicy The table policy.
	 * @return True if the policy is contained in the list.
	 */
	public boolean contains(TablePolicy tblPolicy) {
		return tables.contains(tblPolicy);
	}

	@SuppressWarnings("unchecked")
	private <P extends IPolicy> LinkedHashMap<String, Object> mapPolicies(HashSet<P> policies) {
		LinkedHashMap<String, Object> policiesMap = new LinkedHashMap<String, Object>();
		for(IPolicy policy : policies) {
			AbstractMap.SimpleEntry<String, Object> gDb = (AbstractMap.SimpleEntry<String, Object>)policy.map();
			policiesMap.put(gDb.getKey(), gDb.getValue());	
		}
		
		return policiesMap;
	}
	
	@Override
	public Object map() {
		// merge policies
		LinkedHashMap<String, Object> genPolicies = new LinkedHashMap<String, Object>();
		
		if(!database.isEmpty()) {
			genPolicies.put("database", mapPolicies(database));
		}
		
		if(!tables.isEmpty()) {
			genPolicies.put("tables", mapPolicies(tables));
		}
		
		return genPolicies;
	}
	
	@SuppressWarnings("unchecked")
	public final static Policies unmap(Map<String, Object> policiesMap) {
		Policies policies = new Policies();		

		if(policiesMap.get("tables") != null) {
			LinkedHashMap<String, Object> tables = (LinkedHashMap<String, Object>) policiesMap.get("tables");
			for (Map.Entry<String, Object> entry : tables.entrySet()) {	    
			    policies.add((TablePolicy)TablePolicy.unmap(entry));
			}
		}

		if(policiesMap.get("database") != null) {
			LinkedHashMap<String, Object> database = (LinkedHashMap<String, Object>) policiesMap.get("database");
			for (Map.Entry<String, Object> entry : database.entrySet()) {
		    	policies.add((DatabasePolicy)DatabasePolicy.unmap(entry));		    
			}
		}
		
		return policies;
	}
}