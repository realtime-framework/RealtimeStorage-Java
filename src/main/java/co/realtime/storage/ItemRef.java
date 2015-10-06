package co.realtime.storage;

import java.util.LinkedHashMap;
import java.util.Map;

import co.realtime.storage.Rest.RestType;
import co.realtime.storage.StorageRef.StorageEvent;
import co.realtime.storage.entities.TableMetadata;
import co.realtime.storage.ext.OnError;
import co.realtime.storage.ext.OnItemSnapshot;
import co.realtime.storage.ext.OnPresence;
import co.realtime.storage.ext.OnSetPresence;
import co.realtime.storage.ext.OnTableMetadata;

public class ItemRef {
	StorageContext context;
	TableRef table;
	ItemAttribute primaryKeyValue;
	ItemAttribute secondaryKeyValue;	
	String channel;
	
	ItemRef(StorageContext context, TableRef table, ItemAttribute primaryKeyValue, ItemAttribute secondaryKeyValue){
		this.context = context;
		this.table = table;
		this.primaryKeyValue = primaryKeyValue;
		this.secondaryKeyValue = secondaryKeyValue;
		this.channel = "rtcs_" + table.name() + ":" + primaryKeyValue.get().toString();
		
		if(secondaryKeyValue != null) {
			channel += secondaryKeyValue.get().toString();
		}
	}
	
	private void _del(OnItemSnapshot onItemSnapshot, OnError onError){
		TableMetadata tm = context.getTableMeta(this.table.name);
		PostBodyBuilder pbb = new PostBodyBuilder(context);		
		pbb.addObject("table", this.table.name);
		LinkedHashMap<String, Object> key = new LinkedHashMap<String, Object>();
		key.put("primary", this.primaryKeyValue);
		if(tm.getSecondaryKeyName() != null)
			key.put("secondary", this.secondaryKeyValue);
		pbb.addObject("key", key);
		Rest r = new Rest(context, RestType.DELETEITEM, pbb, this.table);
		r.onError = onError;
		r.onItemSnapshot = onItemSnapshot;
		context.processRest(r);		
	}
	
	/**
	 * Deletes an item specified by this reference
	 * 
	 * @param onItemSnapshot
	 * 		The callback to call with the snapshot of affected item as an argument, when the operation is completed.
	 * @param onError
	 * 		The callback to call if an exception occurred
	 */
	public void del(final OnItemSnapshot onItemSnapshot, final OnError onError){
		TableMetadata tm = context.getTableMeta(this.table.name);
		if(tm == null){
			this.table.meta(new OnTableMetadata(){
				@Override
				public void run(TableMetadata tableMetadata) {
					_del(onItemSnapshot, onError);
				}				
			}, onError);
		} else {
			this._del(onItemSnapshot, onError);
		}
		return;
	}
	
	void _get(OnItemSnapshot onItemSnapshot, OnError onError){
		TableMetadata tm = context.getTableMeta(this.table.name);
		PostBodyBuilder pbb = new PostBodyBuilder(context);	
		pbb.addObject("table", this.table.name);
		LinkedHashMap<String, Object> key = new LinkedHashMap<String, Object>();
		key.put("primary", this.primaryKeyValue);
		if(tm.getSecondaryKeyName() != null)
			key.put("secondary", this.secondaryKeyValue);
		pbb.addObject("key", key);
		Rest r = new Rest(context, RestType.GETITEM, pbb, this.table);
		r.onError = onError;
		r.onItemSnapshot = onItemSnapshot;
		context.processRest(r);
	
	}
	
	/**
	 * Gets an item snapshot specified by this item reference.
	 * 
	 * @param onItemSnapshot
	 * 		The callback to call with the snapshot of affected item as an argument, when the operation is completed.
	 * @param onError
	 * 		The callback to call if an exception occurred
	 * @return
	 * 		Current item reference
	 */
	public ItemRef get(final OnItemSnapshot onItemSnapshot, final OnError onError){
		TableMetadata tm = context.getTableMeta(this.table.name);
		if(tm == null){
			this.table.meta(new OnTableMetadata(){
				@Override
				public void run(TableMetadata tableMetadata) {
					_get(onItemSnapshot, onError);
				}				
			}, onError);
		} else {
			this._get(onItemSnapshot, onError);
		}
		return this;
	}
	
	
	void _set(LinkedHashMap<String, ItemAttribute> item, OnItemSnapshot onItemSnapshot, OnError onError) {
		TableMetadata tm = context.getTableMeta(this.table.name);
		String primaryKeyName = null;
		String secondaryKeyName = null;
		primaryKeyName = tm.getPrimaryKeyName();
		secondaryKeyName = tm.getSecondaryKeyName();
		PostBodyBuilder pbb = new PostBodyBuilder(context);	
		pbb.addObject("table", this.table.name);
		LinkedHashMap<String, Object> key = new LinkedHashMap<String, Object>();
		key.put("primary", this.primaryKeyValue);
		if(tm.getSecondaryKeyName() != null)
			key.put("secondary", this.secondaryKeyValue);
		pbb.addObject("key", key);
		LinkedHashMap<String, ItemAttribute> itemToPut = new LinkedHashMap<String, ItemAttribute>();
		for(Map.Entry<String, ItemAttribute> entry : item.entrySet()){
			String eKey = entry.getKey();
			ItemAttribute eValue = entry.getValue();
			if(!eKey.equals(primaryKeyName) && (!eKey.equals(secondaryKeyName))){
				itemToPut.put(eKey, eValue);
			}
		}
		pbb.addObject("item", itemToPut);
		Rest r = new Rest(context, RestType.UPDATEITEM, pbb, this.table);
		r.onError = onError;
		r.onItemSnapshot = onItemSnapshot;
		context.processRest(r);
	
	}	
	
	/**
	 * Updates the stored item specified by this item reference.
	 * 
	 * @param item
	 * 		The new properties of item to be updated.
	 * @param onItemSnapshot
	 * 		The callback to call with the snapshot of affected item as an argument, when the operation is completed.
	 * @param onError
	 * 		The callback to call if an exception occurred
	 * @return
	 * 		Current item reference
	 */
	public ItemRef set(final LinkedHashMap<String, ItemAttribute> item, final OnItemSnapshot onItemSnapshot, final OnError onError) {
		TableMetadata tm = context.getTableMeta(this.table.name);
		if(tm == null){
			this.table.meta(new OnTableMetadata(){
				@Override
				public void run(TableMetadata tableMetadata) {
					_set(item, onItemSnapshot, onError);
				}				
			}, onError);
		} else {
			this._set(item, onItemSnapshot, onError);
		}
		return this;
	}
	
	/**
	 * Attach a listener to run the callback every time the event type occurs for this item.
	 * 
	 * @param eventType
	 * 		The type of the event to listen. Possible values: put, update, delete.
	 * @param onItemSnapshot
	 * 		The callback to run when the event occurs. The function is called with the snapshot of affected item as argument.
	 * @param onError 
	 * 		Response if client side validation failed or if an error was returned from the server.
	 * @return
	 * 		Current item reference
	 */
	public ItemRef on(StorageEvent eventType, OnItemSnapshot onItemSnapshot, final OnError onError) {
		if(eventType == StorageEvent.PUT) {
			this.get(onItemSnapshot, onError);
		}
		
		Event ev = new Event(eventType, this.table.name, this.primaryKeyValue, this.secondaryKeyValue, false, onItemSnapshot);
		context.addEvent(ev);
		return this;
	}
	
	/**
	 * Attach a listener to run the callback every time the event type occurs for this item.
	 * 
	 * @param eventType
	 * 		The type of the event to listen. Possible values: put, update, delete.
	 * @param onItemSnapshot
	 * 		The callback to run when the event occurs. The function is called with the snapshot of affected item as argument.
	 * @return
	 * 		Current item reference
	 */
	public ItemRef on(StorageEvent eventType, OnItemSnapshot onItemSnapshot) {
		return on(eventType, onItemSnapshot, null);
	}
	
	/**
	 *  Attach a listener to run the callback only one the event type occurs for this item.
	 * 
	 * @param eventType
	 * 		The type of the event to listen. Possible values: put, update, delete.
	 * @param onItemSnapshot
	 * 		The callback to run when the event occurs. The function is called with the snapshot of affected item as argument.
	 * @param onError 
	 * 		Response if client side validation failed or if an error was returned from the server.
	 * @return
	 * 		Current item reference
	 */
	public ItemRef once(StorageEvent eventType, OnItemSnapshot onItemSnapshot, final OnError onError) {
		if(eventType == StorageEvent.PUT) {
			this.get(onItemSnapshot, onError);
		}
		Event ev = new Event(eventType, this.table.name, this.primaryKeyValue, this.secondaryKeyValue, true, onItemSnapshot);
		context.addEvent(ev);
		return this;
	}
	
	/**
	 *  Attach a listener to run the callback only one the event type occurs for this item.
	 * 
	 * @param eventType
	 * 		The type of the event to listen. Possible values: put, update, delete.
	 * @param onItemSnapshot
	 * 		The callback to run when the event occurs. The function is called with the snapshot of affected item as argument.
	 * @return
	 * 		Current item reference
	 */
	public ItemRef once(StorageEvent eventType, OnItemSnapshot onItemSnapshot) {
		return once(eventType, onItemSnapshot, null);
	}
	
	/**
	 * Remove an event listener
	 * 
	 * @param eventType
	 * 		The type of the event to listen. Possible values: put, update, delete.
	 * @param onItemSnapshot
	 * 		The callback previously attached.
	 * @return
	 * 		Current item reference
	 */
	public ItemRef off(StorageEvent eventType, OnItemSnapshot onItemSnapshot){
		Event ev = new Event(eventType, this.table.name, this.primaryKeyValue, this.secondaryKeyValue, false, onItemSnapshot);
		context.removeEvent(ev);
		return this;
	}
	
	void _in_de_cr(String property, Number value, boolean isIncr, OnItemSnapshot onItemSnapshot, OnError onError){
		TableMetadata tm = context.getTableMeta(this.table.name);
		PostBodyBuilder pbb = new PostBodyBuilder(context);	
		pbb.addObject("table", this.table.name);
		LinkedHashMap<String, Object> key = new LinkedHashMap<String, Object>();
		key.put("primary", this.primaryKeyValue);
		if(tm.getSecondaryKeyName() != null)
			key.put("secondary", this.secondaryKeyValue);
		pbb.addObject("key", key);
		pbb.addObject("property", property);
		if(value != null){
			pbb.addObject("value", value);
		}
		Rest r = new Rest(context, isIncr ? RestType.INCR : RestType.DECR, pbb, this.table);
		r.onError = onError;
		r.onItemSnapshot = onItemSnapshot;
		context.processRest(r);
	}
	
	/**
	 * Increments a given attribute of an item. If the attribute doesn't exist, it is set to zero before the operation.
	 * 
	 * @param property The name of the item attribute.
	 * @param value The number to add
	 * @param onItemSnapshot The callback invoked once the attribute has been incremented successfully. The callback is called with the snapshot of the item as argument.
	 * @param onError The callback invoked if an error occurred. Called with the error description.
	 * @return Current item reference
	 */
	public ItemRef incr(final String property, final Number value, final OnItemSnapshot onItemSnapshot, final OnError onError){
		TableMetadata tm = context.getTableMeta(this.table.name);
		if(tm == null){
			this.table.meta(new OnTableMetadata(){
				@Override
				public void run(TableMetadata tableMetadata) {
					_in_de_cr(property, value, true, onItemSnapshot, onError);
				}				
			}, onError);
		} else {
			this._in_de_cr(property, value, true, onItemSnapshot, onError);
		}
		return this;		
	}
	
	/**
	 * Increments by one a given attribute of an item. If the attribute doesn't exist, it is set to zero before the operation.
	 * 
	 * @param property The name of the item attribute.
	 * @param onItemSnapshot The callback invoked once the attribute has been incremented successfully. The callback is called with the snapshot of the item as argument.
	 * @param onError The callback invoked if an error occurred. Called with the error description.
	 * @return Current item reference
	 */
	public ItemRef incr(final String property, final OnItemSnapshot onItemSnapshot, final OnError onError){
		return this.incr(property, null, onItemSnapshot, onError);
	}
	
	/**
	 * Decrements the value of an items attribute. If the attribute doesn't exist, it is set to zero before the operation.
	 * 
	 * @param property The name of the item attribute.
	 * @param value The number to subtract
	 * @param onItemSnapshot The callback invoked once the attribute has been decremented successfully. The callback is called with the snapshot of the item as argument.
	 * @param onError The callback invoked if an error occurred. Called with the error description.
	 * @return Current item reference
	 */
	public ItemRef decr(final String property, final Number value, final OnItemSnapshot onItemSnapshot, final OnError onError) {
		TableMetadata tm = context.getTableMeta(this.table.name);
		if(tm == null){
			this.table.meta(new OnTableMetadata(){
				@Override
				public void run(TableMetadata tableMetadata) {
					_in_de_cr(property, value, false, onItemSnapshot, onError);
				}				
			}, onError);
		} else {
			this._in_de_cr(property, value, false, onItemSnapshot, onError);
		}
		return this;		
	}
	
	/**
	 * Decrements the value by one of an items attribute. If the attribute doesn't exist, it is set to zero before the operation.
	 * 
	 * @param property The name of the item attribute.
	 * @param onItemSnapshot The callback invoked once the attribute has been decremented successfully. The callback is called with the snapshot of the item as argument.
	 * @param onError The callback invoked if an error occurred. Called with the error description.
	 * @return Current item reference
	 */
	public ItemRef decr(final String property, final OnItemSnapshot onItemSnapshot, final OnError onError) {
		return this.decr(property, null, onItemSnapshot, onError);
	}
	
	/**
	 * Enables/Disables the Messaging's presence feature. This operation requires the private key which should be assigned while declaring a storage reference.
	 * 
	 * @param enabled 
	 * 		Flag that enables or disables the presence feature.
	 * @param metadata 
	 * 		Indicates if the connection metadata of a subscription is included in a presence request. Defaults to false.
	 * @param onSetPresence 
	 * 		Response from the server when the request was completed successfully.
	 * @param onError 
	 * 		Response if client side validation failed or if an error was returned from the server.
	 * @return
	 */
	public ItemRef setPresence(Boolean enabled, Boolean metadata, final OnSetPresence onSetPresence, final OnError onError) {
		if(enabled) {
			if(metadata == null) metadata = false;
			context.enablePresence(channel, metadata, onSetPresence, onError);
		}
		else {
			context.disablePresence(channel, onSetPresence, onError);
		}
		
		return this;
	}	
	
	/**
	 * Enables/Disables the Messaging's presence feature. This operation requires the private key which should be assigned while declaring a storage reference.
	 * 
	 * @param enabled 
	 * 		Flag that enables or disables the presence feature.
	 * @param onSetPresence 
	 * 		Response from the server when the request was completed successfully.
	 * @param onError 
	 * 		Response if client side validation failed or if an error was returned from the server.
	 * @return
	 */
	public ItemRef setPresence(Boolean enabled, final OnSetPresence onSetPresence, final OnError onError) {
		setPresence(enabled, false, onSetPresence, onError);
		return this;
	}
	
	/**
	 * Retrieves the number of the table subscriptions and their respective connection metadata (limited to the first 100 subscriptions). Each subscriber is notified of changes made to the table.
	 * 
	 * @param onPresence
	 * 		Response from the server when the request was completed successfully.
	 * @param onError
	 * 		Response if client side validation failed or if an error was returned from the server.
	 * @return
	 */
	public ItemRef presence(final OnPresence onPresence, final OnError onError) {
		context.presence(channel, onPresence, onError);
		return this;
	}
}
