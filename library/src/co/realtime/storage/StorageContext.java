package co.realtime.storage;

import ibt.ortc.api.OnDisablePresence;
import ibt.ortc.api.OnEnablePresence;
import ibt.ortc.api.Ortc;
import ibt.ortc.api.Presence;
import ibt.ortc.extensibility.OnConnected;
import ibt.ortc.extensibility.OnDisconnected;
import ibt.ortc.extensibility.OnException;
import ibt.ortc.extensibility.OnMessage;
import ibt.ortc.extensibility.OnReconnected;
import ibt.ortc.extensibility.OnReconnecting;
import ibt.ortc.extensibility.OnSubscribed;
import ibt.ortc.extensibility.OnUnsubscribed;
import ibt.ortc.extensibility.OrtcClient;
import ibt.ortc.extensibility.OrtcFactory;
import ibt.ortc.extensibility.exception.OrtcNotConnectedException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import co.realtime.storage.Rest.RestType;
import co.realtime.storage.StorageRef.StorageEvent;
import co.realtime.storage.entities.TableMetadata;
import co.realtime.storage.ext.OnError;
import co.realtime.storage.ext.OnPresence;
import co.realtime.storage.ext.OnSetPresence;
import co.realtime.storage.ext.OnTableMetadata;
import co.realtime.storage.ext.StorageException;

import com.fasterxml.jackson.databind.ObjectMapper;

class StorageContext {
	StorageRef storage;
	String applicationKey;
	String privateKey;
	String authenticationToken;
	boolean isCluster;
	boolean isSecure;
	String lastBalancerResponse;
	String url;
	String ortcUrl;
	public ObjectMapper mapper;
	
	public boolean bufferIsActive;
	private OrtcClient ortcClient;
	private HashMap<String, TableMetadata> metas;
	EventCollection evCollection;
	OnMessage onMessage;
	Set<String> toSubscribe;
	Set<String> unsubscribing;
	ArrayList<Rest> offlineBuffer;
	boolean isOffline;
	
	co.realtime.storage.ext.OnReconnected onStorageReconnected = null;
	co.realtime.storage.ext.OnReconnecting onStorageReconnecting = null;
	
	StorageContext(final StorageRef storage, String applicationKey, String privateKey, String authenticationToken, boolean isCluster, boolean isSecure, String url) throws StorageException{
		this.storage = storage;
		this.applicationKey = applicationKey;
		this.authenticationToken = authenticationToken == null ? null : authenticationToken.isEmpty() ? null : authenticationToken;
		this.privateKey = privateKey == null ? null : privateKey.isEmpty() ? null : privateKey;
		this.isCluster = isCluster;
		this.isSecure = isSecure;
		if(!url.contains("http")) {
			this.url = isSecure ? "https://" + url : "http://" + url;	
			this.ortcUrl = "https://ortc-storage.realtime.co/server/ssl/2.1";
		}
		else {
			this.url = url;
			this.ortcUrl = "http://ortc-storage.realtime.co/server/2.1";
		}
		this.metas = new HashMap<String, TableMetadata>();
		this.lastBalancerResponse = null;
		this.evCollection = new EventCollection();
		this.toSubscribe = new HashSet<String>();
		this.unsubscribing = new HashSet<String>();
		this.isOffline = false;
		this.offlineBuffer = new ArrayList<Rest>();
		
		bufferIsActive = true;
		mapper = new ObjectMapper();
		
		try {
			Ortc ortc = new Ortc();
			OrtcFactory factory = ortc.loadOrtcFactory("IbtRealtimeSJ");
			ortcClient = factory.createClient();
			if(isSecure){
				ortcClient.setClusterUrl(ortcUrl);
			} 
			else {
				ortcClient.setClusterUrl(ortcUrl);
			}
			ortcClient.onException = new OnException(){
				public void run(OrtcClient ortcClient, Exception ex) {
					
				}
			};
			
			ortcClient.onReconnected = new OnReconnected(){
				public void run(OrtcClient oc) {
					//System.out.println("::reconnected");
					isOffline = false;
					if(onStorageReconnected != null)
						onStorageReconnected.run(storage);
					callRestFromBuffer();
				}

				private void callRestFromBuffer() {
					if(offlineBuffer.size()>0){
						Rest r = offlineBuffer.get(0);
						if(r!=null){
							offlineBuffer.remove(0);
							r.onRestCompleted = new OnRestCompleted(){
								@Override
								public void run() {
									callRestFromBuffer();							
								}
							};
							r.process();
						}
					}
				}				
			};
			
			ortcClient.onReconnecting = new OnReconnecting(){
				public void run(OrtcClient oc){
					isOffline = true;
					if(onStorageReconnecting != null)
						onStorageReconnecting.run(storage);
				}
			};
			
			ortcClient.onConnected = new OnConnected() {
				public void run(OrtcClient oc) {
					//System.out.println("::connected");
					for(String channel : toSubscribe){
						ortcClient.subscribe(channel, true, onMessage);
					}
					//toSubscribe.clear();
				}				
			};
			
			ortcClient.onDisconnected = new OnDisconnected(){
				public void run(OrtcClient oc) {
					//System.out.println("::disconnected");
				}				
			};
			
			ortcClient.onException = new OnException(){
				public void run(OrtcClient oc, Exception ex) {
					//ex.printStackTrace();
					//System.out.println(String.format("::exception: %s", ex.toString() ));
				}				
			};
			
			ortcClient.onSubscribed = new OnSubscribed(){
				@Override
				public void run(OrtcClient client, String channel) {
					//System.out.println(String.format(":: subscribed to %s", channel));
					toSubscribe.remove(channel);
				}				
			};
			
			ortcClient.onUnsubscribed = new OnUnsubscribed(){
				@Override
				public void run(OrtcClient client, String channel) {
					//System.out.println(String.format(":: unsubscribed from %s", channel));
					unsubscribing.remove(channel);
					if(toSubscribe.contains(channel)){
						client.subscribe(channel, true, onMessage);
					}
				}				
			};
			
			this.onMessage = new OnMessage(){
				@SuppressWarnings("unchecked")
				@Override
				public void run(OrtcClient client, String channel, String messageJson) {
					//System.out.println(String.format(":: mess (%s): %s", channel, messageJson));
					final String tableName = channel.substring(5); //remove rtcs_
					Map<String, Object> message;
					try {
						message = mapper.readValue(messageJson, Map.class);
					} catch (Exception e) {						
						e.printStackTrace();
						return;
					}
					final String type = (String) message.get("type");
					final LinkedHashMap<String, Object> item = (LinkedHashMap<String, Object>) message.get("data");
					//TableMetadata tm = getTableMeta(tableName);
					if(metas.containsKey(tableName)){
						parseNotificationMessage(tableName, type, item);
					} else {
						storage.table(tableName).meta(new OnTableMetadata(){
							@Override
							public void run(TableMetadata tableMetadata) {
								parseNotificationMessage(tableName, type, item);
							}}, null);
					}
				}				
			};
			
					
			ortcClient.connect(applicationKey, authenticationToken);
			
		} catch (Exception e) {
			throw new StorageException(e.toString());
		}	
	}
	
	void parseNotificationMessage(String tableName, String type, LinkedHashMap<String, Object> item){
		TableMetadata tm = getTableMeta(tableName);
		LinkedHashMap<String, ItemAttribute> itemMap = ProcessRestResponse.convertItemMap(item);
		ItemAttribute primary = itemMap.get(tm.getPrimaryKeyName());
		String secondaryKeyName = tm.getSecondaryKeyName();
		ItemAttribute secondary = null;
		if(secondaryKeyName != null)
			secondary = itemMap.get(secondaryKeyName);
		ItemSnapshot is =  new ItemSnapshot(storage.table(tableName), itemMap, primary, secondary);
		Boolean unsubscribe = evCollection.fireEvents(tableName, StorageEvent.fromString(type), primary, secondary, is);
		if(unsubscribe){
			String channelName = String.format("rtcs_%s", tableName);
			ortcClient.unsubscribe(channelName);
			this.unsubscribing.add(channelName);
		}
	}
	
	void setOnReconnected(co.realtime.storage.ext.OnReconnected callback, StorageRef storage){
		this.storage = storage;
		this.onStorageReconnected = callback;
	}
	
	void setOnReconnecting(co.realtime.storage.ext.OnReconnecting callback, StorageRef storage){
		this.storage = storage;
		this.onStorageReconnecting = callback;
	}
	
	void processRest(Rest r){
		if(this.isOffline){
			if((r.type == RestType.PUTITEM || r.type == RestType.UPDATEITEM || r.type == RestType.DELETEITEM) && this.bufferIsActive){
				this.offlineBuffer.add(r);
			} else {
				if(r.onError != null){
					r.onError.run(1007, "Can not establish connection with storage!");
				}
			}
		} else {
			r.process();
		}
	}
	
	void addTableMeta(TableMetadata tm){
		String name = tm.getName();
		metas.put(name, tm);
	}
	
	TableMetadata getTableMeta(String name){
		return metas.get(name);
	}

	public void enablePresence(String channel, Boolean metadata, final OnSetPresence onSetPresence, final OnError onError) {
		OnEnablePresence onEnablePresence = new OnEnablePresence() {
			@Override
			public void run(Exception error, String result) {
				if(error == null) {
					onSetPresence.run(result);
				}
				else {
					onError.run(1009, error.getMessage());						
				}
			}			
		};
		
		try {
			if(ortcClient.getIsConnected()) {
				ortcClient.enablePresence(privateKey, channel, metadata, onEnablePresence);
			}
			else {
				Ortc.enablePresence(ortcUrl, isCluster, applicationKey, privateKey, channel, metadata, onEnablePresence);
			}
		} 
		catch (OrtcNotConnectedException e) {
			onError.run(1008, e.getMessage());
		}		
	}
	
	public void disablePresence(String channel, final OnSetPresence onSetPresence, final OnError onError) {
		
		OnDisablePresence onDisablePresence = new OnDisablePresence() {
			@Override
			public void run(Exception error, String result) {
				if(error == null) {
					onSetPresence.run(result);
				}
				else {
					onError.run(1010, error.getMessage());
				}
			}				
		};
		
		try {
			if(ortcClient.getIsConnected()) {
				ortcClient.disablePresence(privateKey, channel, onDisablePresence);
			}
			else {
				Ortc.disablePresence(ortcUrl, isCluster, applicationKey, privateKey, channel, onDisablePresence);				
			}
		} 
		catch (OrtcNotConnectedException e) {
			onError.run(1008, e.getMessage());
		}		
	}
	
	public void presence(String channel, final OnPresence onPresence, final OnError onError) {		
		ibt.ortc.api.OnPresence onOrtcPresence = new ibt.ortc.api.OnPresence() {
			@Override
			public void run(Exception error, Presence presence) {
				if(error == null) {
					onPresence.run(presence);
				}
				else {
					onError.run(1011, error.getMessage());						
				}				
			}
		};
		
		try {
			if(ortcClient.getIsConnected()) {
				ortcClient.presence(channel, onOrtcPresence);
			}
			else {
				Ortc.presence(ortcUrl, isCluster, applicationKey, authenticationToken, channel, onOrtcPresence);
			}
		} 
		catch (OrtcNotConnectedException e) {
			onError.run(1008, e.getMessage());	
		}
	}
	
	public void addEvent(Event ev) {
		if(ev.onItemSnapshot == null) return;
		Boolean doSubscription = evCollection.add(ev);
		if(doSubscription){
			String channelName = String.format("rtcs_%s", ev.tableName);
			if(ortcClient.getIsConnected() && !this.unsubscribing.contains(channelName)){				
				ortcClient.subscribe(channelName, true, this.onMessage);
			} else {
				this.toSubscribe.add(channelName);
			}
		}
	}

	public void removeEvent(Event ev) {
		Boolean unsubscribe = evCollection.remove(ev);
		if(unsubscribe){
			String channelName = String.format("rtcs_%s", ev.tableName);
			ortcClient.unsubscribe(channelName);
			this.unsubscribing.add(channelName);
		}
	}
}
