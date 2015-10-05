package co.realtime.storage;


import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import co.realtime.storage.Rest.RestType;
import co.realtime.storage.ext.*;
import co.realtime.storage.security.Policies;
import co.realtime.storage.security.Role;

/**
 * Class with the definition of a storage reference.
 */
public class StorageRef {
	StorageContext context;

	enum StorageOrder { NULL, ASC, DESC };
	
	/**
	 * Storage provision load
	 */
	public enum StorageProvisionLoad {
		/**
		 * Assign more read capacity than write capacity.
		 */
		READ(1), 
		/**
		 * Assign more write capacity than read capacity.
		 */
		WRITE(2),
		/**
		 * Assign similar read an write capacity.
		 */
		BALANCED(3),
		/**
		 * Assign custom read an write capacity.
		 */
		CUSTOM(4);
		int value;
		private StorageProvisionLoad(int value){
			this.value = value;
		}
		public int getValue(){
			return this.value;
		}
	}
	
	/**
	 * Storage provision type
	 */
	public enum StorageProvisionType {
		/**
		 * 26 operations per second
		 */
		LIGHT(1), 
		/**
		 * 50 operations per second 
		 */
		MEDIUM(2),
		/**
		 * 100 operations per second
		 */
		INTERMEDIATE(3),
		/**
		 * 200 operations per second
		 */
		HEAVY(4),
		/**
		 * customized read and write throughput 
		 */
		CUSTOM(5);
		int value;
		private StorageProvisionType(int value){
			this.value = value;
		}
		public int getValue(){
			return this.value;
		}
	}
	/**
	 * Storage data type, used for definitions of primary and secondary keys
	 */
	public enum StorageDataType {
		/**
		 * Instance of String
		 */
		STRING("string"),
		/**
		 * Instance of Number
		 */
		NUMBER("number");
		private final String dataType;		
		private StorageDataType(String s){
			dataType = s;
		}		
		public String toString() {
			return dataType;
		}
		public static StorageDataType fromString(String s){
			if(s.equals("string")) return StorageDataType.STRING;
			if(s.equals("number")) return StorageDataType.NUMBER;
			return null;		
		}
	};
	
	/**
	 * Storage event types, used for define notifications types
	 */
	public enum StorageEvent {
		/**
		 * On new storage item
		 */
		PUT,
		/**
		 * When storage item is being updated
		 */
		UPDATE,
		/**
		 * When storage item is being deleted
		 */
		DELETE;
		public static StorageEvent fromString(String s){
			if(s.equals("put")) return StorageEvent.PUT;
			if(s.equals("delete")) return StorageEvent.DELETE;
			if(s.equals("update")) return StorageEvent.UPDATE;
			return null;
		}
	};
	
	/**
	 * Creates a new Storage reference.
	 * 
	 * @param applicationKey 
	 * 		The application key
	 * @param privateKey
	 * 		The private key
	 * @param authenticationToken
	 * 		The authentication token.
	 * @param isCluster
	 * 		Specifies if url is cluster.
	 * @param isSecure
	 * 		Defines if connection use ssl.
	 * @param url
	 * 		The url of the storage server.
	 *  
	 */
	public StorageRef(String applicationKey, String privateKey, String authenticationToken, boolean isCluster, boolean isSecure, String url) throws StorageException {
		context = new StorageContext(this, applicationKey, privateKey, authenticationToken, isCluster, isSecure, url);
	}
	
	/**
	 * Creates a new Storage reference.
	 * 
	 * @param applicationKey 
	 * 		The application key
	 * @param privateKey
	 * 		The private key
	 * @param authenticationToken
	 * 		The authentication token.
	 */
	public StorageRef(String applicationKey, String privateKey, String authenticationToken) throws StorageException {
		this(applicationKey, privateKey, authenticationToken, true, true, "storage-balancer.realtime.co/server/ssl/1.0");
	}	

	/**
	 * Creates a new Storage reference.
	 * 
	 * @param applicationKey
	 * 		The application key
	 * @param privateKey
	 * 		The private key
	 * @throws StorageException
	 */
	public StorageRef(String applicationKey, String privateKey) throws StorageException {
		this(applicationKey, privateKey, null);
	}

	/**
	 * Event fired when a connection is established
	 *
	 * @return Current storage reference
	 */
	public StorageRef onConnected(OnConnected onConnected){
		context.setOnConnected(onConnected,this);
		return this;
	}
	
	/**
	 * Event fired when a connection is reestablished after being closed unexpectedly
	 * 
	 * @return Current storage reference
	 */
	public StorageRef onReconnected(OnReconnected onReconnected){
		context.setOnReconnected(onReconnected, this);
		return this;
	}
	
	/**
	 * Event fired when a connection is trying to be reestablished after being closed unexpectedly
	 * 
	 * @return Current storage reference
	 */
	public StorageRef onReconnecting(OnReconnecting onReconnecting){
		context.setOnReconnecting(onReconnecting, this);
		return this;
	}
	
	/**
	 * Activate offline buffering, which buffers item’s modifications and applies them when connection reestablish. The offline buffering is activated by default.
	 * 
	 * @return Current storage reference
	 */
	public StorageRef activateOfflineBuffering(){
		context.bufferIsActive = true;
		return this;
	}
	
	/**
	 * Deactivate offline buffering, which buffers item’s modifications and applies them when connection reestablish. The offline buffering is activated by default.
	 * 
	 * @return Current storage reference
	 */
	public StorageRef deactivateOfflineBuffering() {
		context.bufferIsActive = false;
		return this;
	}
	
	/**
	 * Creates new table reference
	 * 
	 * @param name
	 * 		The table name
	 * 
	 * @return The table reference
	 */
	public TableRef table(String name){
		return new TableRef(context, name);
	}

	/**
	 * Authenticate a token with the given permissions.
	 * 
	 * @param authenticationToken 
	 * 		The token to authenticate.
	 * @param timeout 
	 * 		The time (in seconds) that the token is valid.
	 * @param roles 
	 * 		The list of roles assigned.
	 * @param policies 
	 * 		Additional policies particular to this token.
	 * @param onAuthenticate
	 * 		The callback to call when the operation is completed, with an argument as a result of verification.
	 * @param onError
	 * 		Response if client side validation failed or if an error was returned from the server.
	 * @return Current storage reference
	 */
	public StorageRef authenticate(String authenticationToken, int timeout, HashSet<String> roles, Policies policies, OnBooleanResponse onAuthenticate, OnError onError ) {
		Map<String, Object> payload = new HashMap<String, Object>();
		payload.put("authenticationToken", authenticationToken);
		payload.put("timeout", timeout);
		payload.put("roles", roles);
		payload.put("policies", policies.map());
		
		PostBodyBuilder pbb = new PostBodyBuilder(context, payload);
		Rest r = new Rest(context, RestType.AUTHENTICATE, pbb, null);
		r.onError = onError;
		r.onBooleanResponse = onAuthenticate;
		context.processRest(r);
		
		return this;
	}
	
	/**
	 * Authenticate a token with the given permissions.
	 * 
	 * @param authenticationToken
	 * 		The token to authenticate.
	 * @param timeout
	 * 		The time (in seconds) that the token is valid.
	 * @param policies
	 * 		Additional policies particular to this token.
	 * @param onAuthenticate
	 * 		The callback to call when the operation is completed, with an argument as a result of verification.
	 * @param onError
	 * 		Response if client side validation failed or if an error was returned from the server.
	 * @return
	 */
	public StorageRef authenticate(String authenticationToken, int timeout, Policies policies, OnBooleanResponse onAuthenticate, OnError onError) {
		authenticate(authenticationToken, timeout, null, policies, onAuthenticate, onError);
		return this;
	}
	
	/**
	 * Checks if a specified authentication token is authenticated.
	 * 
	 * @param authenticationToken
	 * 		The token to verify.
	 * @param onBooleanResponse
	 * 		The callback to call when the operation is completed, with an argument as a result of verification.
	 * @param onError
	 * 		The callback to call if an exception occurred
	 * @return Current storage reference
	 */
	public StorageRef isAuthenticated(String authenticationToken, OnBooleanResponse onBooleanResponse, OnError onError) {
		Rest r = new Rest(context, RestType.ISAUTHENTICATED, null, null);
		r.onError = onError;
		r.onBooleanResponse = onBooleanResponse;
		r.rawBody = "{\"applicationKey\":\""+context.applicationKey+"\", \"authenticationToken\":\""+authenticationToken+"\"}";
		context.processRest(r);
		return this;		
	}
	
	/**
	 * Retrieves a list of the names of all tables created by the user’s subscription.
	 * 
	 * @param onTableSnapshot
	 * 		The callback to call once the values are available. The function will be called with a table snapshot as argument, as many times as the number of tables existent. In the end, when all calls are done, the success function will be called with null as argument to signal that there are no more tables.
	 * @param onError
	 * 		The callback to call if an exception occurred
	 * @return Current storage reference
	 */
	public StorageRef getTables(OnTableSnapshot onTableSnapshot, OnError onError) {
		PostBodyBuilder pbb = new PostBodyBuilder(context);		
		Rest r = new Rest(context, RestType.LISTTABLES, pbb, null);
		r.onError = onError;
		r.onTableSnapshot = onTableSnapshot;
		context.processRest(r);	
		return this;
	}
	
	/**
	 * Retrieves a list of the names of the roles created by the user�s application.
	 * 
	 * @param onRoleName
	 * 		Function called, for each existing role name, when the operation completes successfully.
	 * @param onError
	 * 		Response if client side validation failed or if an error was returned from the server.
	 * @return Current storage reference
	 */
	public StorageRef listRoles(OnRoleName onRoleName, OnError onError) {
		PostBodyBuilder pbb = new PostBodyBuilder(context);		
		Rest r = new Rest(context, RestType.LISTROLES, pbb, null);
		r.onError = onError;
		r.onRoleName = onRoleName;
		context.processRest(r);
		
		return this;
	}
	
	/**
	 * Retrieves the specified roles policies associated with the subscription.
	 * 
	 * @param roles 
	 * 		The names of the roles to retrieve.
	 * @param onRole
	 * 		Function called, for each existing role, when the operation completes successfully.
	 * @param onError
	 * 		Response if client side validation failed or if an error was returned from the server.
	 * @return Current storage reference
	 */
	public StorageRef getRoles(HashSet<String> roles, OnRole onRole, OnError onError) {
		PostBodyBuilder pbb = new PostBodyBuilder(context);
		pbb.addObject("roles", roles);
		Rest r = new Rest(context, RestType.GETROLES, pbb, null);
		r.onError = onError;
		r.onRole = onRole;
		context.processRest(r);	
		return this;
	}
	
	/**
	 * Retrieves the policies that compose the role.
	 * 
	 * @param role
	 * 		The name of the role to retrieve.
	 * @param onRole
	 * 		Function called when the operation completes successfully.
	 * @param onError
	 * 		Response if client side validation failed or if an error was returned from the server.
	 * @return Current storage reference
	 */
	public StorageRef getRole(String role, OnRole onRole, OnError onError) {
		PostBodyBuilder pbb = new PostBodyBuilder(context);
		pbb.addObject("role", role);
		Rest r = new Rest(context, RestType.GETROLE, pbb, null);
		r.onError = onError;
		r.onRole = onRole;
		context.processRest(r);	
		
		return this;
	}
	
	/**
	 * Removes a role associated with the subscription.
	 * 
	 * @param role 
	 * 		The name of the role to delete.
	 * @param onRoleDelete
	 * 		Function called when the operation completes successfully.
	 * @param onError
	 * 		Response if client side validation failed or if an error was returned from the server.
	 * @return Current storage reference
	 */
	public StorageRef deleteRole(String role, OnBooleanResponse onRoleDelete, OnError onError) {
		Map<String, Object> payload = new HashMap<String, Object>();
		payload.put("role", role);
		
		PostBodyBuilder pbb = new PostBodyBuilder(context, payload);
		Rest r = new Rest(context, RestType.DELETEROLE, pbb, null);
		r.onError = onError;
		r.onBooleanResponse = onRoleDelete;
		context.processRest(r);
		
		return this;
	}
	
	/**
	 * Stores a set of rules that control access to the Storage database.
	 * 
	 * @param role
	 * 		The role to be set.
	 * @param onRoleSet
	 * 		Function called when the operation completes successfully.
	 * @param onError
	 * 		Response if client side validation failed or if an error was returned from the server.
	 * @return Current storage reference
	 */
	public StorageRef setRole(Role role, OnBooleanResponse onRoleSet, OnError onError) {
		PostBodyBuilder pbb = new PostBodyBuilder(context, role.map());
		Rest r = new Rest(context, RestType.SETROLE, pbb, null);
		r.onError = onError;
		r.onBooleanResponse = onRoleSet;
		context.processRest(r);
		
		return this;
	}
	
	/**
	 * Modifies a set of existing rules that control access to the Storage database.
	 *  
	 * @param role
	 * 		The role to be updated.
	 * @param onRoleUpdate
	 * 		Function called when the operation completes successfully.
	 * @param onError
	 * 		Response if client side validation failed or if an error was returned from the server.
	 * @return Current storage reference
	 */
	public StorageRef updateRole(Role role, OnBooleanResponse onRoleUpdate, OnError onError) {
		PostBodyBuilder pbb = new PostBodyBuilder(context, role.map());
		Rest r = new Rest(context, RestType.UPDATEROLE, pbb, null);
		r.onError = onError;
		r.onBooleanResponse = onRoleUpdate;
		context.processRest(r);
		
		return this;
	}	
}
