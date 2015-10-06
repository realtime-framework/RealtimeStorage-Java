package co.realtime.storage.security;

import java.util.AbstractMap;

/**
 * Policy specifically for the authorization of creating tables.
 */
public class DatabaseCreatePolicy extends DatabasePolicy {

	private Boolean allow;

	/**
	 * Retrieves the permission set to create tables.
	 * @return The authorization value.
	 */
	public Boolean getAllow() {
		return allow;
	}

	/**
	 * Sets the permission to create tables.
	 * @param allow The authorization value.
	 */
	public void setAllow(Boolean allow) {
		this.allow = allow;
	}

	/**
	 * Creates an instance of DatabaseCreatePolicy. The permission to create tables is set to false by default.
	 */
	public DatabaseCreatePolicy() {
		this.allow = false;	
	}
	
	/**
	 * Creates an instance of DatabaseCreatePolicy with the given authorization.
	 */
	public DatabaseCreatePolicy(Boolean allow) {
		this.allow = allow;	
	}
	
	@Override
	public Object map() {
		AbstractMap.SimpleEntry<Operation, Object> genDatabase = new AbstractMap.SimpleEntry<Operation, Object>(Operation.CreateTable, getAllow());
		return genDatabase;
	}	
}