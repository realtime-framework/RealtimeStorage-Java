package co.realtime.storage.security;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

/**
 * Policies applied specifically to the tables.
 * 
 * @author RTCS Development Team
 *
 */
public class TablePolicy implements IPolicy {
	/**
	 * Rules regarding the access to tables and their keys.
	 * 
	 * @author RTCS Development Team
	 *
	 */
	public enum Rule {
		CREATE ('C'),
		READ ('R'),
		UPDATE ('U'),
		DELETE ('D');
		
		private char value;
		
		private Rule(char rule) {
			value = rule;
		}		

		public char getValue() {
			return this.value;
		}
		
		public static Rule getRuleByName(final char name)
	    {
	        for (Rule rule : Rule.values())
	            if (rule.value == name)
	                return rule;

	        return null;
	    }
	}
	
	/**
	 * Type of access a rule is set.
	 * 
	 * @author RTCS Development Team
	 *
	 */
	public enum Access {
		ALLOW ("allow"),
		DENY ("deny");
		
		private String value;
		
		private Access(String access) {
			value = access;
		}		

		public String getValue() {
			return this.value;
		}
	}
	
	private String tableName;
	private HashSet<ItemPolicy> items;
	protected HashMap<Access, EnumSet<Rule>> accessRules;
	
	/**
	 * Creates an empty instance of TablePolicy.
	 */
	public TablePolicy() {
		tableName = "";
		items = new HashSet<ItemPolicy>();
		accessRules = new HashMap<Access, EnumSet<Rule>>();
		accessRules.put(Access.ALLOW, EnumSet.noneOf(Rule.class));
		accessRules.put(Access.DENY, EnumSet.noneOf(Rule.class));
	}
	
	/**
	 * Retrieves the name of the table.
	 * 
	 * @return The name of the table.
	 */
	public String getTableName() { return tableName; }
	
	/**
	 * Sets the name of a table.
	 * 
	 * @param tableName The name of the table.
	 */
	public void setTableName(String tableName) { this.tableName = tableName; }
	
	/**
	 * Adds a set of rules to a specific type of access.
	 * 
	 * @param access Type of access.
	 * @param rules The rules applied to the table.
	 */
	public void setAccessRules(Access access, EnumSet<Rule> rules) {
		accessRules.put(access, rules);
	}
	
	/**
	 * Sets the access rules.
	 * 
	 * @param access The access rules applied to the table.
	 */
	public void setAccessRules(HashMap<Access, EnumSet<Rule>> accessRules) {
		this.accessRules = accessRules;
	}
	
	/**
	 * Retrieves the rules set to a type of access.
	 * 
	 * @param access Type of access.
	 * @return
	 */
	public EnumSet<Rule> getAccessRules(Access access) {
		return accessRules.get(access);
	}
	
	/**
	 * Retrieves all of the access rules applied to the table.
	 * 
	 * @return The access rules.
	 */
	public HashMap<Access, EnumSet<Rule>> getAccessRules() {
		return accessRules;
	}
	
	/**
	 * Adds a rule to a specific type of access. 
	 * 
	 * @param access Type of access.
	 * @param rule The rules applied to the table.
	 */
	public void addAccessRule(Access access, Rule rule) {
		EnumSet<Rule> rules = accessRules.get(access);
		rules.add(rule);
		accessRules.put(access, rules);
	}

	@Override
	public Object map() {
		// table
		LinkedHashMap<String, Object> table = new LinkedHashMap<String, Object>();		
		
		// access
		EnumSet<Rule> rules = accessRules.get(Access.ALLOW);
		String ruleStr;
		if(rules != null && !rules.isEmpty()) {
			ruleStr = "";
			for(Rule rule : rules) {
				ruleStr += rule.getValue();
			}
			table.put("allow", ruleStr);
		}
		
		rules = accessRules.get(Access.DENY);			
		if(rules != null && !rules.isEmpty()) {
			ruleStr = "";
			for(Rule rule : rules) {
				ruleStr += rule.getValue();
			}
			table.put("deny", ruleStr);
		}
		
		// items
		if(items != null && !items.isEmpty()) {
			HashSet<Object> genItems = new HashSet<Object>();
			for(ItemPolicy itemPolicy : items) {
				genItems.add(itemPolicy.map());
			}
			table.put("items", genItems.toArray());
		}
		
		return new AbstractMap.SimpleEntry<String, Object>(tableName, table);
	}	

	@SuppressWarnings("unchecked")
	public static IPolicy unmap(Entry<String, Object> entry) {
	    TablePolicy tablePolicy = new TablePolicy();
	    tablePolicy.setTableName(entry.getKey());
	    
	    LinkedHashMap<String, Object> value = (LinkedHashMap<String, Object>)entry.getValue();		    

		// access rules
    	String rulesJSON = (String) value.get("allow");
	    if(rulesJSON != null) {
	    	for (char ruleJSON : rulesJSON.toCharArray()){
		    	tablePolicy.addAccessRule(Access.ALLOW, Rule.getRuleByName(ruleJSON));
	        }
	    }
	    
	    rulesJSON = (String) value.get("deny");
	    if(rulesJSON != null) {
	    	for (char ruleJSON : rulesJSON.toCharArray()){
		    	tablePolicy.addAccessRule(Access.DENY, Rule.getRuleByName(ruleJSON));
	        }			    	
	    }
	    
	    // items
	    if(value.get("items") != null) {
	    	ArrayList<LinkedHashMap<String, Object>> itemsJSON = (ArrayList<LinkedHashMap<String, Object>>)value.get("items");
	    	for(LinkedHashMap<String, Object> itemJSON : itemsJSON) {
	    		Entry<String, Object> itemEntry = new AbstractMap.SimpleEntry<String, Object>("item", itemJSON);	    		
			    tablePolicy.items.add((ItemPolicy)ItemPolicy.unmap(itemEntry));
	    	}
	    }
	    
	    return tablePolicy;
	}
	
	private final static class ItemPolicy extends TablePolicy {
		private LinkedHashMap<String, Object> keys;

		@SuppressWarnings({ "unchecked", "unused" })
		public <P> P getPrimaryKey() { return (P) keys.get("primary"); }
		public <P> void setPrimaryKey(P primaryKey) { keys.put("primary", primaryKey); }
		
		@SuppressWarnings({ "unchecked", "unused" })
		public <S> S getSecondaryKey() { return (S) keys.get("secondary"); }
		public <S> void setSecondaryKey(S secondaryKey) { keys.put("secondary", secondaryKey); }
		
		private ItemPolicy() {
			super();
			keys = new LinkedHashMap<String, Object>();
		}
		
		@Override
		public Object map() {
			// item
			LinkedHashMap<String, Object> item = new LinkedHashMap<String, Object>();
			
			// keys
			item.put("key", keys);
			
			// access		
			EnumSet<Rule> rules = accessRules.get(Access.ALLOW);
			String ruleStr;
			if(rules != null && !rules.isEmpty()) {
				ruleStr = "";
				for(Rule rule : rules) {
					ruleStr += rule.getValue();
				}
				item.put("allow", ruleStr);
			}
			
			rules = accessRules.get(Access.DENY);			
			if(rules != null && !rules.isEmpty()) {
				ruleStr = "";
				for(Rule rule : rules) {
					ruleStr += rule.getValue();
				}
				item.put("deny", ruleStr);
			}
			
			return item;
		}
		
		@SuppressWarnings("unchecked")
		public static IPolicy unmap(Entry<String, Object> entry) {
			ItemPolicy itemPolicy = new ItemPolicy();
			
			LinkedHashMap<String, Object> itemJSON = (LinkedHashMap<String, Object>)entry.getValue();
			
    		// key
    		LinkedHashMap<String, Object> keyJSON = (LinkedHashMap<String, Object>) itemJSON.get("key");
			// primary key
    		itemPolicy.setPrimaryKey((String) keyJSON.get("primary"));
			// secondary key
    		itemPolicy.setSecondaryKey((String) keyJSON.get("secondary"));	
    		
    		// access rules
	    	String itemRulesJSON = (String) itemJSON.get("allow");
		    if(itemRulesJSON != null) {
		    	for (char ruleJSON : itemRulesJSON.toCharArray()) {
		    		itemPolicy.addAccessRule(Access.ALLOW, Rule.getRuleByName(ruleJSON));
		        }
		    }
		    
		    itemRulesJSON = (String) itemJSON.get("deny");
		    if(itemRulesJSON != null) {
		    	for (char ruleJSON : itemRulesJSON.toCharArray()) {
		    		itemPolicy.addAccessRule(Access.DENY, Rule.getRuleByName(ruleJSON));
		        }			    	
		    }
		    
		    return itemPolicy;
		}
	}
	
	/**
	 * Sets the access rules of an item of the table.
	 * 
	 * @param primaryKey The item's primary key.
	 * @param accessRules The access rules applied to the item.
	 */
	public <P> void setItemPolicy(P primaryKey, HashMap<Access, EnumSet<Rule>> accessRules) {
		setItemPolicy(primaryKey, null, accessRules);
	}
	
	/**
	 * Sets the access rules of an item of the table.
	 * 
	 * @param primaryKey The item's primary key.
	 * @param secondaryKey The item's secondary key.
	 * @param accessRules The access rules applied to the item.
	 */
	public <P, S> void setItemPolicy(P primaryKey, S secondaryKey, HashMap<Access, EnumSet<Rule>> accessRules) {
		ItemPolicy itemPolicy = new ItemPolicy();
		itemPolicy.setPrimaryKey(primaryKey);
		itemPolicy.setSecondaryKey(secondaryKey);
		itemPolicy.setAccessRules(accessRules);
		if(items.contains(itemPolicy))
			items.remove(itemPolicy);
		items.add(itemPolicy);		
	}	
	
	/**
	 * Removes the access rules of an item of the table.
	 * 
	 * @param primaryKey The item's primary key.
	 */
	public <P> void removeItemPolicy(P primaryKey) {
		removeItemPolicy(primaryKey, null);
	}	
	
	/**
	 * Removes the access rules of an item of the table.
	 * 
	 * @param primaryKey The item's primary key.
	 * @param secondaryKey The item's secondary key.
	 */
	public <P, S> void removeItemPolicy(P primaryKey, S secondaryKey) {
		ItemPolicy itemPolicy = new ItemPolicy();
		itemPolicy.setPrimaryKey(primaryKey);
		itemPolicy.setSecondaryKey(secondaryKey);
		items.remove(itemPolicy);		
	}
}
