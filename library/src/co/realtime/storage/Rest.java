package co.realtime.storage;

import ibt.ortc.api.OnRestWebserviceResponse;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.net.ssl.HttpsURLConnection;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import co.realtime.storage.StorageRef.StorageOrder;
import co.realtime.storage.entities.TableMetadata;
import co.realtime.storage.ext.OnBooleanResponse;
import co.realtime.storage.ext.OnError;
import co.realtime.storage.ext.OnItemSnapshot;
import co.realtime.storage.ext.OnRole;
import co.realtime.storage.ext.OnRoleName;
import co.realtime.storage.ext.OnTableCreation;
import co.realtime.storage.ext.OnTableMetadata;
import co.realtime.storage.ext.OnTableSnapshot;
import co.realtime.storage.ext.OnTableUpdate;
import co.realtime.storage.ext.StorageException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

class Rest {
	enum RestType {
		AUTHENTICATE("authenticate"),
		ISAUTHENTICATED("isAuthenticated"),
		LISTROLES("listRoles"),
		GETROLES("getRoles"),
		GETROLE("getRole"),
		DELETEROLE("deleteRole"),
		SETROLE("setRole"),
		UPDATEROLE("updateRole"),
		LISTITEMS("listItems"), 
		QUERYITEMS("queryItems"), 
		GETITEM("getItem"), 
		PUTITEM("putItem"), 
		UPDATEITEM("updateItem"), 
		DELETEITEM("deleteItem"), 
		CREATETABLE("createTable"), 
		UPDATETABLE("updateTable"), 
		DELETETABLE("deleteTable"), 
		LISTTABLES("listTables"), 
		DESCRIBETABLE("describeTable"), 
		INCR("incr"), 
		DECR("decr");

		private final String restName;

		private RestType(String s) {
			restName = s;
		}

		public String toString() {
			return restName;
		}
	};

	StorageContext context;
	RestType type;
	TableRef table;
	URL requestUrl;
	PostBodyBuilder bodyBuilder;
	private LinkedHashMap<String, Object> lastStopKey;
	private String lastStopTable;
	private ArrayList<LinkedHashMap<String, Object>> allItems;
	
	public OnError onError = null;
	public OnTableSnapshot onTableSnapshot = null;
	public OnBooleanResponse onBooleanResponse = null;
	public OnItemSnapshot onItemSnapshot = null;
	public OnTableMetadata onTableMetadata = null;
	public OnTableCreation onTableCreation = null;
	public OnTableUpdate onTableUpdate = null;
	public OnRole onRole = null;
	public OnRoleName onRoleName = null;
	OnRestCompleted onRestCompleted = null;
	public String rawBody = null;
	public StorageOrder order = StorageOrder.NULL;
	public Long limit = null;

	Rest(StorageContext context, RestType type, PostBodyBuilder bodyBuilder, TableRef table) {
		this.context = context;
		this.type = type;
		this.bodyBuilder = bodyBuilder;
		this.table = table;
		this.requestUrl = null;
		this.lastStopKey = null;
		this.lastStopTable = null;
		this.allItems = new ArrayList<LinkedHashMap<String, Object>>();
		this.limit = (Long) bodyBuilder.getObject("limit");
	}

	void process() {
		try {
			resolveUrl();
		} catch (Exception e) {
			if (this.onError != null)
			this.onError.run(1002, e.getMessage());
			return;
		}
		if (this.requestUrl == null) {
			this.context.lastBalancerResponse = null;
			if (this.onError != null)
				this.onError.run(1003, "Can not get response from balancer!");
			return;
		}
		if (lastStopKey != null)
			bodyBuilder.addObject("startKey", lastStopKey);

		if(lastStopTable!=null){
			bodyBuilder.addObject("startTable", lastStopTable);
		}

		if (this.type == RestType.QUERYITEMS && this.order == StorageOrder.DESC){
			bodyBuilder.addObject("searchForward",false);
		}

		String rBody;
		try {
			rBody = (this.rawBody != null ? this.rawBody : this.bodyBuilder.getBody());
		} catch (JsonProcessingException e) {
			if (this.onError != null)
				this.onError.run(1004, e.getMessage());
			return;
		}

		RestWebservice.postAsync(this.requestUrl, rBody,
				new OnRestWebserviceResponse() {
					@SuppressWarnings("unchecked")
					@Override
					public void run(Exception e, String r) {
						if (onRestCompleted != null)
							onRestCompleted.run();
						if (e != null) {
							if (context.isCluster && context.lastBalancerResponse != null) {
								context.lastBalancerResponse = null;
								process();
							} else {
								if (onError != null)
									onError.run(1005, e.getMessage());
							}
						} else {
							ObjectMapper mapper = new ObjectMapper();

							Map<String, Object> data;
							try {
								data = mapper.readValue(r, Map.class);
							} catch (Exception ex) {
								if (onError != null)
									onError.run(1006, ex.getMessage());
								return;
							}
							LinkedHashMap<String, Object> error = (LinkedHashMap<String, Object>) data.get("error");
							if (error != null) {
								if (onError != null)
									onError.run((Integer) error.get("code"),(String) error.get("message"));
							} else {
								if (type == RestType.LISTITEMS || type == RestType.QUERYITEMS) {
									LinkedHashMap<String, Object> rdata = (LinkedHashMap<String, Object>) data.get("data");
									LinkedHashMap<String, Object> stopKey = (LinkedHashMap<String, Object>) rdata.get("stopKey");
									ArrayList<LinkedHashMap<String, Object>> items = (ArrayList<LinkedHashMap<String, Object>>) rdata.get("items");
									allItems.addAll(items);

									if ((type != RestType.QUERYITEMS || (limit != null && limit > allItems.size())) && stopKey != null) {
										lastStopKey = stopKey;
										process();
										return;
									}
								}

								if(type == RestType.LISTTABLES){
									LinkedHashMap<String, Object> rData = (LinkedHashMap<String, Object>)data.get("data");
									String stopTable = (String) rData.get("stopTable");
									ArrayList<String> tables = (ArrayList<String>) rData.get("tables");
									if(!stopTable.isEmpty() && tables.isEmpty()){
										lastStopTable = stopTable;
										process();
										return;
									}
								}

								switch (type) {
								case AUTHENTICATE:
									ProcessRestResponse.processAuthenticate(
											data, onBooleanResponse);
									break;
								case ISAUTHENTICATED:
									ProcessRestResponse.processIsAuthenticated(
											data, onBooleanResponse);
									break;		
								case LISTROLES:
									ProcessRestResponse.processListRoles(
											data, onRoleName);
									break;
								case GETROLES:
									ProcessRestResponse.processGetRoles(
											data, onRole);
									break;
								case GETROLE:
									ProcessRestResponse.processGetRole(
											data, onRole);
									break;
								case DELETEROLE:
									ProcessRestResponse.processDeleteRole(
											data, onBooleanResponse);
									break;
								case SETROLE:
									ProcessRestResponse.processSetRole(
											data, onBooleanResponse);
									break;
								case UPDATEROLE:
									ProcessRestResponse.processUpdateRole(
											data, onBooleanResponse);
									break;
								case LISTITEMS:
									String sortKey = null;
									if (order != StorageOrder.NULL) {
										TableMetadata tm = context.getTableMeta(table.name());
										sortKey = tm.getSecondaryKeyName();
										if (sortKey == null)
											sortKey = tm.getPrimaryKeyName();
									}
									ProcessRestResponse.processListItems(allItems, table, onItemSnapshot, order, sortKey, limit);
									break;
								case QUERYITEMS:
									ProcessRestResponse.processQueryItems(allItems, table, onItemSnapshot);
									break;
								case GETITEM:
									ProcessRestResponse.processGetItem(data, table, onItemSnapshot);
									break;
								case PUTITEM:
									ProcessRestResponse.processPutItem(data, table, onItemSnapshot);
									break;
								case UPDATEITEM:
									ProcessRestResponse.processUpdateItem(data, table, onItemSnapshot);
									break;
								case DELETEITEM:
									ProcessRestResponse.processDelItem(data, table, onItemSnapshot);
									break;
								case CREATETABLE:
									ProcessRestResponse.processCreateTable(data, onTableCreation);
									break;
								case UPDATETABLE:
									ProcessRestResponse.processUpdateTable(data, onTableUpdate);
									break;
								case DELETETABLE:
									ProcessRestResponse.processDeleteTable(data, onBooleanResponse);
									break;
								case LISTTABLES:
									ProcessRestResponse.processListTables(data, context, onTableSnapshot);
									break;
								case DESCRIBETABLE:
									ProcessRestResponse.processDescribeTable(data, context, onTableMetadata);
									break;
								case INCR:
								case DECR:
									ProcessRestResponse.processInDeCrResponse(data, table, onItemSnapshot);
									break;
								}
							}
						}
					}
				});
	}

	// will put the server url with rest path to this.requestUrl
	void resolveUrl() throws Exception {
		String tempUrl;
		if (context.isCluster) {
			if (this.context.lastBalancerResponse != null) {
				tempUrl = this.context.lastBalancerResponse;
			} 
                        else {
				String urlString = context.url + "?appkey=" + context.applicationKey;
				URL url = new URL(urlString);
				String balancerResponse = urlString.startsWith("https:") ? secureBalancerRequest(url) : unsecureBalancerRequest(url);
				if (balancerResponse == null) {
					throw new StorageException("Cannot get response from balancer!");
				}
				JSONObject obj = (JSONObject) JSONValue.parse(balancerResponse);
				tempUrl = (String) obj.get("url");
				this.context.lastBalancerResponse = tempUrl;
			}
		} else {
			tempUrl = context.url;
		}
		tempUrl += tempUrl.substring(tempUrl.length() - 1).equals("/") ? this.type.toString() : "/" + this.type.toString();
		this.requestUrl = new URL(tempUrl);
	}

	private String unsecureBalancerRequest(URL url) throws IOException {
		HttpURLConnection connection = (HttpURLConnection) url.openConnection();
		connection.setRequestProperty("user-agent", "storage-java-client");
		BufferedReader rd = new BufferedReader(new InputStreamReader(connection.getInputStream()));
		String result = "";
		String line;
		while ((line = rd.readLine()) != null) {
			result += line;
		}
		rd.close();
		return result;
	}

	private String secureBalancerRequest(URL url) throws Exception { // throws
													// UnknownHostException,
													// IOException {
		HttpsURLConnection connection = null;
		StringBuilder result = new StringBuilder(16);

		// connection.setDoOutput(true);

		try {
			connection = (HttpsURLConnection) url.openConnection();
			BufferedReader rd = null;

			try {
				if (connection.getResponseCode() != 200) {
					// CAUSE: Reliance on default encoding
					rd = new BufferedReader(new InputStreamReader(
							connection.getErrorStream(), "UTF-8"));
					String line = rd.readLine();
					while (line != null) {
						result.append(line);
						line = rd.readLine();
					}
					rd.close();
					throw new Exception(result.toString());
				} else {
					rd = new BufferedReader(new InputStreamReader(
							connection.getInputStream(), "UTF-8"));
					String line = rd.readLine();
					while (line != null) {
						result.append(line);
						line = rd.readLine();
					}

				}
				// CAUSE: Method may fail to close stream on exception
			} finally {
				if (rd != null) {
					rd.close();
				}
			}
			// CAUSE: Method may fail to close connection on exception
		} finally {
			if (connection != null) {
				connection.disconnect();
			}
		}

		return result.toString();
	}
}
