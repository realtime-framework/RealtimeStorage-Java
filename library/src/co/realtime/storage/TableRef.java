package co.realtime.storage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import co.realtime.storage.Filter.StorageFilter;
import co.realtime.storage.Rest.RestType;
import co.realtime.storage.StorageRef.StorageDataType;
import co.realtime.storage.StorageRef.StorageEvent;
import co.realtime.storage.StorageRef.StorageOrder;
import co.realtime.storage.StorageRef.StorageProvisionLoad;
import co.realtime.storage.StorageRef.StorageProvisionType;
import co.realtime.storage.entities.Key;
import co.realtime.storage.entities.TableMetadata;
import co.realtime.storage.entities.Throughput;
import co.realtime.storage.ext.OnBooleanResponse;
import co.realtime.storage.ext.OnError;
import co.realtime.storage.ext.OnItemSnapshot;
import co.realtime.storage.ext.OnPresence;
import co.realtime.storage.ext.OnSetPresence;
import co.realtime.storage.ext.OnTableCreation;
import co.realtime.storage.ext.OnTableMetadata;
import co.realtime.storage.ext.OnTableUpdate;
import co.realtime.storage.ext.StorageException;

public class TableRef {
	StorageContext context;
	String name;
	Long limit;
	StorageOrder order;
	Set<Filter> filters;

	String channel;

	TableRef(StorageContext context, String name) {
		this.context = context;
		this.name = name;
		this.limit = null;
		this.order = StorageOrder.NULL;
		this.filters = new HashSet<Filter>();
		this.channel = "rtcs_" + this.name;
	}
	
	/**
	 * Adds a new table with primary key to the user’s application. Take into account that, even though this operation completes, the table stays in a ‘creating’ state. While in this state, all operations done over this table will fail with a ResourceInUseException.
	 * 
	 * @param primaryKeyName
	 * 		The primary key
	 * @param primaryKeyDataType
	 * 		The primary key data type
	 * @param provisionType
	 * 		The provision type
	 * @param provisionLoad
	 * 		The provision load
	 * @param onTableCreation
	 * 		The callback to call when the operation is completed 
	 * @param onError
	 * 		The callback to call if an exception occurred
	 * @return Table reference
	 * 
	 * @deprecated use {@link create( co.realtime.storage.entities.Key key, StorageProvisionType provisionType, StorageProvisionLoad provisionLoad, co.realtime.storage.ext.OnTableCreation onTableCreation, co.realtime.storage.ext.OnError onError)} instead.
	 */
	@Deprecated
	public TableRef create(String primaryKeyName, StorageDataType primaryKeyDataType, StorageProvisionType provisionType,
			StorageProvisionLoad provisionLoad, OnTableCreation onTableCreation, OnError onError){
		
		create(primaryKeyName, primaryKeyDataType, null, null, provisionType, provisionLoad, onTableCreation, onError);
		return this;
	}

	/**
	 * Adds a new table with primary and secondary keys to the user’s application. Take into account that, even though this operation completes, the table stays in a ‘creating’ state. While in this state, all operations done over this table will fail with a ResourceInUseException.
	 * 
	 * @param primaryKeyName
	 * 		The primary key
	 * @param primaryKeyDataType
	 * 		The primary key data type
	 * @param secondaryKeyName
	 * 		The secondary key
	 * @param secondaryKeyDataType
	 * 		The secondary key data type
	 * @param provisionType
	 * 		The provision type
	 * @param provisionLoad
	 * 		The provision load
	 * @param onTableCreation
	 * 		The callback to call when the operation is completed
	 * @param onError
	 * 		The callback to call if an exception occurred
	 * @return Table reference
	 * 
	 * @deprecated use {@link create( co.realtime.storage.entities.Key key, StorageProvisionType provisionType, StorageProvisionLoad provisionLoad, co.realtime.storage.ext.OnTableCreation onTableCreation, co.realtime.storage.ext.OnError onError)} instead.
	 */
	@Deprecated
	public TableRef create(String primaryKeyName, StorageDataType primaryKeyDataType, String secondaryKeyName, 
			StorageDataType secondaryKeyDataType, StorageProvisionType provisionType,
			StorageProvisionLoad provisionLoad, OnTableCreation onTableCreation, OnError onError){
		
		PostBodyBuilder pbb = new PostBodyBuilder(context);
		pbb.addObject("table", this.name);
		pbb.addObject("provisionLoad", provisionLoad.getValue());
		pbb.addObject("provisionType", provisionType.getValue());
		Map <String, Object> key = new HashMap<String, Object>();
		Map <String, Object> primary = new HashMap<String, Object>();
		primary.put("name", primaryKeyName);
		primary.put("dataType", primaryKeyDataType.toString());
		key.put("primary", primary);
		if(secondaryKeyName != null && secondaryKeyDataType != null){
			Map <String, Object> secondary = new HashMap<String, Object>();
			secondary.put("name", secondaryKeyName);
			secondary.put("dataType", secondaryKeyDataType.toString());
			key.put("secondary", secondary);
		}			
		pbb.addObject("key", key);
		Rest r = new Rest(context, RestType.CREATETABLE, pbb, null);
		r.onError = onError;
		r.onTableCreation = onTableCreation;
		context.processRest(r);
		return this;
	}
	
	/**
	 * Creates a table with a custom throughput. The provision type is Custom and the provision load is ignored.
	 * @param key The schema of the primary and secondary (optional) keys.
	 * @param throughput The number of read and write operations per second.
	 * @param onTableCreation
	 * 		Response if the operation ended successfully.
	 * @param onError
	 * 		Response if client side validation failed or if an error was returned from the server.
	 * @return Table reference
	 */
	public TableRef create(Key key, Throughput throughput, OnTableCreation onTableCreation, OnError onError) {
		PostBodyBuilder pbb = new PostBodyBuilder(context);
		pbb.addObject("table", this.name);
		pbb.addObject("provisionType", StorageProvisionType.CUSTOM.getValue());
		pbb.addObject("key", key.map());
		pbb.addObject("throughput", throughput.map());
		Rest r = new Rest(context, RestType.CREATETABLE, pbb, null);
		r.onError = onError;
		r.onTableCreation = onTableCreation;
		context.processRest(r);
		
		return this;		
	}
	
	/**
	 * Creates a table with a predefined throughput by selecting the provision type and provision load.
	 * @param key The schema of the primary and secondary (optional) keys.
	 * @param provisionType The amount of throughput units.
	 * @param provisionLoad The way the throughput units will be allocated between the read and write throughput of the table.
	 * @param onTableCreation
	 * 		Response if the operation ended successfully.
	 * @param onError
	 * 		Response if client side validation failed or if an error was returned from the server.
	 * @return Table reference
	 * @throws co.realtime.storage.ext.StorageException
	 */
	public TableRef create(Key key, StorageProvisionType provisionType, StorageProvisionLoad provisionLoad, OnTableCreation onTableCreation, OnError onError) throws StorageException {
		if(provisionLoad == StorageProvisionLoad.CUSTOM || provisionType == StorageProvisionType.CUSTOM) {
			throw new StorageException("Must define the throughput when assigning a custom provision load or type. Use the other create method instead.");
		}
		
		
		PostBodyBuilder pbb = new PostBodyBuilder(context);
		pbb.addObject("table", this.name);
		pbb.addObject("provisionLoad", provisionLoad.getValue());
		pbb.addObject("provisionType", provisionType.getValue());
		pbb.addObject("key", key.map());
		Rest r = new Rest(context, RestType.CREATETABLE, pbb, null);
		r.onError = onError;
		r.onTableCreation = onTableCreation;
		context.processRest(r);
		
		return this;		
	}
	
	/**
	 * Delete this table.
	 * 
	 * @param onBooleanResponse
	 * 		The callback to run once the table is deleted
	 * @param onError
	 * 		The callback to call if an exception occurred
	 */
	public void del(OnBooleanResponse onBooleanResponse, OnError onError){
		PostBodyBuilder pbb = new PostBodyBuilder(context);		
		pbb.addObject("table", this.name);
		Rest r = new Rest(context, RestType.DELETETABLE, pbb, null);
		r.onError = onError;
		r.onBooleanResponse = onBooleanResponse;
		context.processRest(r);			
	}
	
	/**
	 * Gets the metadata of the table reference.
	 * 
	 * @param onTableMetadata
	 * 		The callback to run once the metadata is retrieved
	 * @param onError
	 * 		The callback to call if an exception occurred
	 * @return Current table reference
	 */
	public TableRef meta(OnTableMetadata onTableMetadata, OnError onError){
		PostBodyBuilder pbb = new PostBodyBuilder(context);		
		pbb.addObject("table", this.name);
		Rest r = new Rest(context, RestType.DESCRIBETABLE, pbb, null);
		r.onError = onError;
		r.onTableMetadata = onTableMetadata;
		context.processRest(r);
		return this;		
	}
	
	/**
	 * Return the name of the referred table.
	 * 
	 * @return The name of the table
	 */
	public String name(){
		return this.name;	
	}
	
	/**
	 * Define if the items will be retrieved in ascendent order.
	 * 
	 * @return Current table reference
	 */
	public TableRef asc(){
		this.order = StorageOrder.ASC;
		return this;
	}
	
	/**
	 * Define if the items will be retrieved in descendant order.
	 * 
	 * @return Current table reference
	 */
	public TableRef desc(){
		this.order = StorageOrder.DESC;
		return this;
	}
	
	/**
	 * Adds a new item to the table.
	 * 
	 * @param item
	 * 		The item to add
	 * @param onItemSnapshot
	 * 		The callback to run once the insertion is done.
	 * @param onError
	 * 		The callback to call if an exception occurred
	 * @return Current table reference
	 */
	public TableRef push(LinkedHashMap<String, ItemAttribute> item, OnItemSnapshot onItemSnapshot, OnError onError){
		PostBodyBuilder pbb = new PostBodyBuilder(context);
		pbb.addObject("table", this.name);
		pbb.addObject("item", item);
		Rest r = new Rest(context, RestType.PUTITEM, pbb, this);
		r.onError = onError;
		r.onItemSnapshot = onItemSnapshot;
		context.processRest(r);
		return this;
	}
	
	/**
	 * Applies a limit to this reference confining the number of items.
	 * 
	 * @param value
	 * 		The limit to apply.
	 * @return Current table reference
	 */
	public TableRef limit(Long value){
		this.limit = value;
		return this;
	}
	
	private final void _update(StorageProvisionLoad provisionLoad, StorageProvisionType provisionType, OnTableUpdate onTableUpdate, OnError onError){
		TableMetadata tm = context.getTableMeta(this.name);
		if((Math.abs(tm.getProvisionLoad().value - provisionLoad.value) + Math.abs(tm.getProvisionType().value - provisionType.value)) > 1){
			if(onError != null){
				onError.run(1001, "You can not make such a radical change to throughput");
			}
			return;
		}		
		PostBodyBuilder pbb = new PostBodyBuilder(context);		
		pbb.addObject("table", this.name);
		pbb.addObject("provisionLoad", provisionLoad.getValue());
		pbb.addObject("provisionType", provisionType.getValue());
		Rest r = new Rest(context, RestType.UPDATETABLE, pbb, null);
		r.onError = onError;
		r.onTableUpdate = onTableUpdate;
		context.processRest(r);
	}

	/**
	 * Updates the provision type and provision load of the referenced table.
	 * 
	 * @param provisionLoad
	 * 		The new provision load
	 * @param provisionType
	 * 		The new provision type
	 * @param onTableUpdate
	 * 		The callback to run once the table is updated
	 * @param onError
	 * 		The callback to call if an exception occurred
	 * @return Current table reference
	 */
	public TableRef update(final StorageProvisionLoad provisionLoad, final StorageProvisionType provisionType, final OnTableUpdate onTableUpdate, final OnError onError){
		TableMetadata tm = context.getTableMeta(this.name);
		if(tm == null){
			this.meta(new OnTableMetadata(){
				@Override
				public void run(TableMetadata tableMetadata) {
					_update(provisionLoad, provisionType, onTableUpdate, onError);
				}				
			}, onError);
		} else {
			this._update(provisionLoad, provisionType, onTableUpdate, onError);
		}
		return this;
	}
	
	/**
	 * Applies a filter to the table. When fetched, it will return the items that match the filter property value.
	 * 
	 * @param attributeName
	 * 		The name of the property to filter.
	 * @param value
	 * 		The value of the property to filter.
	 * @return Current table reference
	 */
	public TableRef equals(String attributeName, ItemAttribute value){
		filters.add(new Filter(StorageFilter.EQUALS, attributeName, value, null));
		return this;
	}
	/**
	 * Applies a filter to the table. When fetched, it will return the items that does not match the filter property value.
	 * 
	 * @param attributeName
	 * 		The name of the property to filter.
	 * @param value
	 * 		The value of the property to filter.
	 * @return Current table reference
	 */
	public TableRef notEqual(String attributeName, ItemAttribute value){
		filters.add(new Filter(StorageFilter.NOTEQUAL, attributeName, value, null));
		return this;
	}
	/**
	 * Applies a filter to the table. When fetched, it will return the items greater or equal to filter property value.
	 * 
	 * @param attributeName
	 * 		The name of the property to filter.
	 * @param value
	 * 		The value of the property to filter.
	 * @return Current table reference
	 */
	public TableRef greaterEqual(String attributeName, ItemAttribute value){
		filters.add(new Filter(StorageFilter.GREATEREQUAL, attributeName, value, null));
		return this;
	}
	/**
	 * Applies a filter to the table. When fetched, it will return the items greater than the filter property value.
	 * 
	 * @param attributeName
	 * 		The name of the property to filter.
	 * @param value
	 * 		The value of the property to filter.
	 * @return Current table reference
	 */
	public TableRef greaterThan(String attributeName, ItemAttribute value){
		filters.add(new Filter(StorageFilter.GREATERTHAN, attributeName, value, null));
		return this;
	}
	/**
	 * Applies a filter to the table. When fetched, it will return the items lesser or equals to the filter property value.
	 * 
	 * @param attributeName
	 * 		The name of the property to filter.
	 * @param value
	 * 		The value of the property to filter.
	 * @return Current table reference
	 */
	public TableRef lessEqual(String attributeName, ItemAttribute value){
		filters.add(new Filter(StorageFilter.LESSEREQUAL, attributeName, value, null));
		return this;
	}
	/**
	 * Applies a filter to the table. When fetched, it will return the items lesser than the filter property value.
	 * 
	 * @param attributeName
	 * 		The name of the property to filter.
	 * @param value
	 * 		The value of the property to filter.
	 * @return Current table reference
	 */
	public TableRef lessThan(String attributeName, ItemAttribute value){
		filters.add(new Filter(StorageFilter.LESSERTHAN, attributeName, value, null));
		return this;
	}
	/**
	 * Applies a filter to the table reference. When fetched, it will return the non null values.
	 * 
	 * @param attributeName
	 * 		The name of the property to filter.
	 * @return Current table reference
	 */
	public TableRef notNull(String attributeName){
		filters.add(new Filter(StorageFilter.NOTNULL, attributeName, null, null));
		return this;
	}
	/**
	 * Applies a filter to the table. When fetched, it will return the null values.
	 * 
	 * @param attributeName
	 * 		The name of the property to filter.
	 * @return Current table reference
	 */
	public TableRef isNull(String attributeName){
		filters.add(new Filter(StorageFilter.NULL, attributeName, null, null));
		return this;
	}
	/**
	 * Applies a filter to the table. When fetched, it will return the items that contains the filter property value.
	 * 
	 * @param attributeName
	 * 		The name of the property to filter.
	 * @param value
	 * 		The value of the property to filter.
	 * @return Current table reference
	 */
	public TableRef contains(String attributeName, ItemAttribute value){
		filters.add(new Filter(StorageFilter.CONTAINS, attributeName, value, null));
		return this;
	}
	/**
	 * Applies a filter to the table. When fetched, it will return the items that does not contains the filter property value.
	 * 
	 * @param attributeName
	 * 		The name of the property to filter.
	 * @param value
	 * 		The value of the property to filter.
	 * @return Current table reference
	 */
	public TableRef notContains(String attributeName, ItemAttribute value){
		filters.add(new Filter(StorageFilter.NOTCONTAINS, attributeName, value, null));
		return this;
	}
	/**
	 * Applies a filter to the table. When fetched, it will return the items that begins with the filter property value.
	 * 
	 * @param attributeName
	 * 		The name of the property to filter.
	 * @param value
	 * 		The value of the property to filter.
	 * @return Current table reference
	 */
	public TableRef beginsWith(String attributeName, ItemAttribute value){
		filters.add(new Filter(StorageFilter.BEGINSWITH, attributeName, value, null));
		return this;
	}
	/**
	 * Applies a filter to the table. When fetched, it will return the items in range of the filter property value.
	 * 
	 * @param attributeName
	 * 		The name of the property to filter.
	 * @param startValue
	 * 		The value of property indicates the beginning of range.
	 * @param endValue
	 * 		The value of property indicates the end of range.
	 * @return Current table reference
	 */
	public TableRef between(String attributeName, ItemAttribute startValue, ItemAttribute endValue){
		filters.add(new Filter(StorageFilter.BETWEEN, attributeName, startValue, endValue));
		return this;
	}
	
	private void _getItems(OnItemSnapshot onItemSnapshot, OnError onError){
		TableMetadata tm = context.getTableMeta(this.name);
		LinkedHashMap<String, Object> lhm = _tryConstructKey(tm);
		RestType rt = (lhm == null) ? RestType.LISTITEMS : RestType.QUERYITEMS;
		PostBodyBuilder pbb = new PostBodyBuilder(context);
		pbb.addObject("table", this.name);
		if(rt==RestType.QUERYITEMS){
			pbb.addObject("key", lhm);				
			if(this.limit != null)
				pbb.addObject("limit", this.limit);
		}
		if(filters.size()>0)
			pbb.addObject("filter", getFiltersForJSON(rt));
		Rest r = new Rest(context, rt, pbb, this);
		r.onError = onError;
		r.onItemSnapshot = onItemSnapshot;
		r.order = this.order;		
		if(this.limit != null){
			r.limit = this.limit;
		}
		context.processRest(r);

	}
		
	private Object getFiltersForJSON(RestType rt) {
		if(this.filters.size() == 1){
			Iterator<Filter> itr = filters.iterator();
			Filter f = itr.next();
			if(rt==RestType.LISTITEMS){
				ArrayList<Object> ar = new ArrayList<Object>();
				ar.add(f.prepareForJSON());
				return ar;
			} else {
				return f.prepareForJSON();				
			}
		} else {
			ArrayList<Object> ar = new ArrayList<Object>();
			for(Filter f : filters){
				ar.add(f.prepareForJSON());
			}
			return ar;
		}		
	}

	//if returns null the rest type is listItems, otherwise queryItems
	private LinkedHashMap<String, Object> _tryConstructKey(TableMetadata tm) {
		for(Filter f : filters)
			if(f.operator==StorageFilter.NOTEQUAL || f.operator==StorageFilter.NOTNULL || f.operator==StorageFilter.NULL ||
					f.operator==StorageFilter.CONTAINS || f.operator==StorageFilter.NOTCONTAINS)
				return null; //because queryItems do not support notEqual, notNull, null, contains and notContains
		
		if(tm.getSecondaryKeyName()!=null && filters.size() == 1){
			Iterator<Filter> itr = filters.iterator();
			Filter f = itr.next(); 
			if(f.itemName.equals(tm.getPrimaryKeyName()) && f.operator == StorageFilter.EQUALS) {
				LinkedHashMap<String, Object> ret = new LinkedHashMap<String, Object>();
				ret.put("primary", f.value);
				filters.clear();
				return ret;
			}			
		} else if (tm.getSecondaryKeyName()!=null && filters.size() == 2) {
			Object tValue = null;
			Filter tFilter = null;
			for(Filter f : filters){
				if(f.itemName.equals(tm.getPrimaryKeyName()) && f.operator == StorageFilter.EQUALS) {
					tValue = f.value;
				}
				if(f.itemName.equals(tm.getSecondaryKeyName())){
					tFilter = f;
				}				
			}
			if(tValue!=null && tFilter!=null){
				filters.clear();
				filters.add(tFilter);
				LinkedHashMap<String, Object> ret = new LinkedHashMap<String, Object>();
				ret.put("primary", tValue);
				return ret;
			} else {
				return null;				
			}
		}
		return null;
	}

	/**
	 * Get the items of this tableRef.
	 * 
	 * @param onItemSnapshot
	 * 		The callback to call once the items are available. The success function will be called for each existent item. The argument is an item snapshot. In the end, when all calls are done, the success function will be called with null as argument to signal that there are no more items.
	 * @param onError
	 * 		The callback to call if an exception occurred
	 * @return Current table reference
	 */
	public TableRef getItems(final OnItemSnapshot onItemSnapshot, final OnError onError) {
		TableMetadata tm = context.getTableMeta(this.name);
		if(tm == null){
			this.meta(new OnTableMetadata(){
				@Override
				public void run(TableMetadata tableMetadata) {
					_getItems(onItemSnapshot, onError);
				}				
			}, onError);
		} else {
			this._getItems(onItemSnapshot, onError);
		}
		return this;
	}
	
	/**
	 * Creates a new item reference.
	 * 
	 * @param primaryKeyValue
	 * 		The primary key. Must match the table schema.
	 * @return Current table reference
	 */
	public ItemRef item(ItemAttribute primaryKeyValue){
		return new ItemRef(context, this, primaryKeyValue, null);
	}
	
	/**
	 * Creates a new item reference.
	 * 
	 * @param primaryKeyValue
	 * 		The primary key. Must match the table schema.
	 * @param secondaryKeyValue
	 * 		The secondary key. Must match the table schema.
	 * @return Current table reference
	 */
	public ItemRef item(ItemAttribute primaryKeyValue, ItemAttribute secondaryKeyValue) {
		return new ItemRef(context, this, primaryKeyValue, secondaryKeyValue);
	}

	/**
	 * Attach a listener to run every time the eventType occurs.
	 * 
	 * @param eventType
	 * 		The type of the event to listen. Possible values: put, update, delete
	 * @param onItemSnapshot
	 * 		The function to run whenever the event occurs. The function is called with the snapshot of affected item as argument.
	 * @param onError 
	 * 		Response if client side validation failed or if an error was returned from the server.
	 * @return Current table reference
	 */
	public TableRef on(StorageEvent eventType, final OnItemSnapshot onItemSnapshot, final OnError onError) {
		if(eventType == StorageEvent.PUT) {
			getItems(onItemSnapshot, onError);
		}
		Event ev = new Event(eventType, this.name, false, onItemSnapshot);
		context.addEvent(ev);
		return this;
	}

	/**
	 * Attach a listener to run every time the eventType occurs.
	 * 
	 * @param eventType
	 * 		The type of the event to listen. Possible values: put, update, delete
	 * @param onItemSnapshot
	 * 		The function to run whenever the event occurs. The function is called with the snapshot of affected item as argument.
	 * @return Current table reference
	 */
	public TableRef on(StorageEvent eventType, final OnItemSnapshot onItemSnapshot) {
		return on(eventType, onItemSnapshot, null);
	}
	
	private final Boolean filterExists(StorageFilter filterType, String itemName) {
			Boolean filterExists = false;
			// see if equals filter exists over the primary key
			for(Filter filter : filters) {
				if(filter.itemName == itemName && filter.operator == filterType) {
					filterExists = true;
					break;
				}
			}	
			
			return filterExists;
	}
	
	/**
	 * Attach a listener to run every time the eventType occurs for specific primary key.
	 * 
	 * @param eventType
	 * 		The type of the event to listen. Possible values: put, update, delete
	 * @param primary
	 * 		The primary key of the items to listen. The callback will run every time an item with the primary key is affected.
	 * @param onItemSnapshot
	 * 		The function to run whenever the event occurs. The function is called with the snapshot of affected item as argument.
	 * @param onError 
	 * 		Response if client side validation failed or if an error was returned from the server.
	 * @return Current table reference
	 */
	public TableRef on(StorageEvent eventType, final ItemAttribute primary, final OnItemSnapshot onItemSnapshot, final OnError onError) {
		if(eventType == StorageEvent.PUT) {			
			final TableRef self = this;
			TableMetadata tm = context.getTableMeta(this.name);
			if(tm == null) {
				this.meta(new OnTableMetadata(){
					@Override
					public void run(TableMetadata tableMetadata) {
						// see if equals filter exists over the primary key
						if(!filterExists(StorageFilter.EQUALS, tableMetadata.getPrimaryKeyName())) {
							self.equals(tableMetadata.getPrimaryKeyName(), primary);
						}						
						_getItems(onItemSnapshot, onError);
					}			
				}, onError);
				
			}
			else {
				// see if equals filter exists over the primary key
				if(!filterExists(StorageFilter.EQUALS, tm.getPrimaryKeyName())) {
					equals(tm.getPrimaryKeyName(), primary);
				}
				_getItems(onItemSnapshot, onError);			
			}
		}
		
		Event ev = new Event(eventType, this.name, primary, false, onItemSnapshot);
		context.addEvent(ev);
		return this;
	}
	
	/**
	 * Attach a listener to run every time the eventType occurs for specific primary key.
	 * 
	 * @param eventType
	 * 		The type of the event to listen. Possible values: put, update, delete
	 * @param primary
	 * 		The primary key of the items to listen. The callback will run every time an item with the primary key is affected.
	 * @param onItemSnapshot
	 * 		The function to run whenever the event occurs. The function is called with the snapshot of affected item as argument.
	 * @return Current table reference
	 */
	public TableRef on(StorageEvent eventType, final ItemAttribute primary, final OnItemSnapshot onItemSnapshot) {
		return on(eventType, primary, onItemSnapshot);
	}
	
	/**
	 * Attach a listener to run only once the event type occurs.
	 * 
	 * @param eventType
	 * 		The type of the event to listen. Possible values: put, update, delete
	 * @param onItemSnapshot
	 * 		The function to run when the event occurs. The function is called with the snapshot of affected item as argument.
	 * @param onError 
	 * 		Response if client side validation failed or if an error was returned from the server.
	 * @return Current table reference
	 */
	public TableRef once(StorageEvent eventType, final OnItemSnapshot onItemSnapshot, final OnError onError) {
		if(eventType == StorageEvent.PUT) {
			getItems(onItemSnapshot, onError);
		}
		Event ev = new Event(eventType, this.name, true, onItemSnapshot);
		context.addEvent(ev);
		return this;
	}

	/**
	 * Attach a listener to run only once the event type occurs for specific primary key.
	 * 
	 * @param eventType
	 * 		The type of the event to listen. Possible values: put, update, delete
	 * @param primary
	 * 		The primary key of the items to listen. The callback will run when item with the primary key is affected.
	 * @param onItemSnapshot
	 * 		The function to run when the event occurs. The function is called with the snapshot of affected item as argument.
	 * @param onError 
	 * 		Response if client side validation failed or if an error was returned from the server.
	 * @return Current table reference
	 */
	public TableRef once(StorageEvent eventType, final ItemAttribute primary, final OnItemSnapshot onItemSnapshot, final OnError onError) {
		if(eventType == StorageEvent.PUT) {
			final TableRef self = this;
			TableMetadata tm = context.getTableMeta(this.name);
			if(tm == null) {
				this.meta(new OnTableMetadata() {
					@Override
					public void run(TableMetadata tableMetadata) {
						// see if equals filter exists over the primary key
						if(!filterExists(StorageFilter.EQUALS, tableMetadata.getPrimaryKeyName())) {
							self.equals(tableMetadata.getPrimaryKeyName(), primary);
						}
						_getItems(onItemSnapshot, onError);
					}
				}, onError);
				
			}
			else {
				// see if equals filter exists over the primary key
				if(!filterExists(StorageFilter.EQUALS, tm.getPrimaryKeyName())) {
					equals(tm.getPrimaryKeyName(), primary);
				}
				_getItems(onItemSnapshot, onError);			
			}
		}
		
		Event ev = new Event(eventType, this.name, primary, true, onItemSnapshot);
		context.addEvent(ev);
		return this;
	}

	/**
	 * Remove an event handler.
	 * 
	 * @param eventType
	 * 		The type of the event to listen. Possible values: put, update, delete
	 * @param onItemSnapshot
	 * 		The callback previously attached.
	 * @return Current table reference
	 */
	public TableRef off(StorageEvent eventType, final OnItemSnapshot onItemSnapshot) {	
		Event ev = new Event(eventType, this.name, false, onItemSnapshot);
		context.removeEvent(ev);
		return this;
	}

	/**
	 * Remove an event handler.
	 * 
	 * @param eventType
	 * 		The type of the event to listen. Possible values: put, update, delete
	 * @param primary
	 * 		The primary key of the items to stop listen.
	 * @param onItemSnapshot
	 * 		The callback previously attached.
	 * @return Current table reference
	 */
	public TableRef off(StorageEvent eventType, ItemAttribute primary, final OnItemSnapshot onItemSnapshot){
		Event ev = new Event(eventType, this.name, primary, false, onItemSnapshot);
		context.removeEvent(ev);
		return this;
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
	public TableRef setPresence(Boolean enabled, Boolean metadata, final OnSetPresence onSetPresence, final OnError onError) {
		if(enabled) {
			if(metadata == null) metadata = false;
			final Boolean meta = metadata;			
			context.enablePresence(channel, metadata, new OnSetPresence() {				
				@Override
				public void run(String result) {
					context.enablePresence(channel + ":*", meta, onSetPresence, onError);					
				}
			}, onError);
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
	public TableRef setPresence(Boolean enabled, final OnSetPresence onSetPresence, final OnError onError) {
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
	public TableRef presence(final OnPresence onPresence, final OnError onError) {
		context.presence(channel, onPresence, onError);
		return this;
	}
}
