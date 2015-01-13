package co.realtime.storage.security;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map.Entry;

/**
 * Policies applied specifically to the operations over the database schema.
 * 
 * @author RTCS Development Team
 *
 */
public class DatabasePolicy implements IPolicy  {
	
	/**
	 * The possible operations over which restrictions can be applied.
	 * @author RTCS Development Team
	 *
	 */
	public enum Operation {
		NONE (""),
		ListTables ("listTables"),
		CreateTable ("createTable"),
		UpdateTable ("updateTable"),
		DeleteTable	("deleteTable");
		
		private String value;
		
		private Operation(String operation) {
			value = operation;
		}

		public String getValue() {
			return this.value;
		}
		
		public static Operation getValue(final String name)
	    {
	        for (Operation operation : Operation.values())
	            if (operation.value.equalsIgnoreCase(name))
	                return operation;

	        return null;
	    }
	};
	
	protected Operation operation;
	private HashSet<String> tables;
	
	/**
	 * Retrieves the selected operation.
	 * @return The selected operation.
	 */
	public Operation getOperation() { return operation; }
	
	/**
	 * Sets the operation.
	 * @param operation The name of the operation
	 */
	public void setOperation(Operation operation) { this.operation = operation; }	

	/**
	 * Retrieves the tables that the operation is res
	 * @return
	 */
	public HashSet<String> getTables() { return tables; }
	public void setTables(HashSet<String> tables) { this.tables = tables; }
	
	public void addTable(String table) {
		tables.add(table);
	}
	
	public DatabasePolicy() {
		operation = Operation.NONE;
		tables = new HashSet<String>();
	}

	public DatabasePolicy(Operation operation) {
		this();
		this.operation = operation;
	}
	
	public DatabasePolicy(Operation operation , HashSet<String> tables) {
		this.operation = operation;
		this.tables = tables;
	}
	
    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;
        if (obj == this)
            return true;
        if (!(obj instanceof DatabasePolicy))
            return false;
        
        DatabasePolicy dp = (DatabasePolicy) obj;
        return this.operation == dp.getOperation();
    }
    
	@Override
	public Object map() {
		AbstractMap.SimpleEntry<Operation, Object> genDatabase = new AbstractMap.SimpleEntry<Operation, Object>(operation, tables.toArray());
		return genDatabase;
	}
	
	@SuppressWarnings("unchecked")
	public static IPolicy unmap(Entry<String, Object> entry) {
		Operation key = Operation.getValue(entry.getKey());
	    if(key == Operation.CreateTable) {	    	
	    	return new DatabaseCreatePolicy((Boolean)entry.getValue());
	    }
	    
    	return new DatabasePolicy(key, new HashSet<String>(((ArrayList<String>)entry.getValue())));
	}
}