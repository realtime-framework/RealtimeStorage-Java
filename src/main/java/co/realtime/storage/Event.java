package co.realtime.storage;

import co.realtime.storage.StorageRef.StorageEvent;
import co.realtime.storage.ext.OnItemSnapshot;

class Event {
	StorageEvent type;
	String tableName;
	ItemAttribute primary;
	ItemAttribute secondary;
	Boolean isOnce;
	OnItemSnapshot onItemSnapshot;
	
	Event(StorageEvent type, String tableName, ItemAttribute primary, ItemAttribute secondary, Boolean isOnce, OnItemSnapshot onItemSnapshot) {
		this.type = type;
		this.tableName = tableName;
		this.primary = primary;
		this.secondary = secondary;
		this.isOnce = isOnce;
		this.onItemSnapshot = onItemSnapshot;				
	}
	
	Event(StorageEvent type, String tableName, Boolean isOnce, OnItemSnapshot onItemSnapshot) {
		this(type, tableName, null, null, isOnce, onItemSnapshot);
	}
	
	Event(StorageEvent type, String tableName, ItemAttribute primary, Boolean isOnce, OnItemSnapshot onItemSnapshot) {
		this(type, tableName, primary, null, isOnce, onItemSnapshot);			
	}
	
	public void fire(ItemSnapshot item) {
		if(this.onItemSnapshot != null)
			this.onItemSnapshot.run(item);
	}
}
