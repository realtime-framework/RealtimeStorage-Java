package co.realtime.storage;


import ibt.ortc.api.OnRestWebserviceResponse;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

class RestWebservice {
	
	protected static void getAsync(URL url,OnRestWebserviceResponse callback){
		requestAsync(url,"GET", null, callback);
	}
	
	protected static void postAsync(URL url,String content,OnRestWebserviceResponse callback){
		requestAsync(url,"POST", content, callback);
	}
	
	private static void requestAsync(final URL url,final String method, final String content,final OnRestWebserviceResponse callback){
		Runnable task = new Runnable() {
			
			@Override
			public void run() {
				if(method.equals("GET")){
					try {
						String result = "https".equals(url.getProtocol()) ? secureGetRequest(url) : unsecureGetRequest(url);
						
						callback.run(null, result);
					} catch (Exception error) {
						
						callback.run(error, null);						
					}
				}else if(method.equals("POST")){
					String result = null;
					try {
						result = "https".equals(url.getProtocol()) ? securePostRequest(url,content) : unsecurePostRequest(url,content);
					} catch (Exception e) {
						callback.run(e, null);
					}
					if(result != null)
						callback.run(null, result);
				}else{					
					callback.run(new Exception("Invalid request method - " + method), null);
				}
			}
		};
		
		new Thread(task).start();		
	}
	
	private static String unsecureGetRequest(URL url) throws Exception {
		HttpURLConnection connection = (HttpURLConnection) url.openConnection();		
		
		//connection.setDoOutput(true);

		String result = "";
		
		InputStream responseBody = null;
		if(connection.getResponseCode() != 200 && connection.getResponseCode() != -1){			
			responseBody = connection.getErrorStream();			
			
			result = readResponseBody(responseBody);
			
			throw new Exception(result);
		}else{
			responseBody = connection.getInputStream();
			if(responseBody == null){
				responseBody = connection.getErrorStream();				
				
				result = readResponseBody(responseBody);
				
				throw new Exception(result);
			}else{
				result = readResponseBody(responseBody);
			}
		}

		return result;
	}
	
	private static String secureGetRequest(URL url) throws Exception {
		HttpsURLConnection connection = (HttpsURLConnection) url.openConnection();
		
		//connection.setDoOutput(true);

		String result = "";
		
		if(connection.getResponseCode() != 200){
			BufferedReader rd = new BufferedReader(new InputStreamReader(connection.getErrorStream()));
			String line;
			while ((line = rd.readLine()) != null) {
				result += line;
			}
			rd.close();
			
			throw new Exception(result);
		}else{
			BufferedReader rd = new BufferedReader(new InputStreamReader(connection.getInputStream()));
			String line;
			while ((line = rd.readLine()) != null) {
				result += line;
			}
			rd.close();
		}

		return result;
	}	

	private static String unsecurePostRequest(URL url, String postBody) throws Exception {
		HttpURLConnection connection = (HttpURLConnection) url.openConnection();		
		connection.setRequestMethod("POST");
		connection.setRequestProperty("Content-Type", "application/json");
		connection.setRequestProperty("Accept", "application/json");
		connection.setDoOutput(true);
		OutputStreamWriter wr = new OutputStreamWriter(connection.getOutputStream());

		wr.write(postBody);

		wr.flush();
		
		String result = "";
		
		if(connection.getResponseCode() != 200){
			BufferedReader rd = new BufferedReader(new InputStreamReader(connection.getErrorStream()));
			String line;
			while ((line = rd.readLine()) != null) {
				result += line;
			}
			rd.close();
			
			throw new Exception(result);
		}else{
			BufferedReader rd = new BufferedReader(new InputStreamReader(connection.getInputStream()));
			String line;
			while ((line = rd.readLine()) != null) {
				result += line;
			}
			rd.close();
		}
		wr.close();

		return result;
	}
	
	private static String securePostRequest(URL url, String postBody) throws Exception {			
		SSLSocketFactory sslsocketfactory =  (SSLSocketFactory) SSLSocketFactory.getDefault();			
		SSLSocket sslsocket = (SSLSocket) sslsocketfactory.createSocket(url.getHost(), url.getPort());				
		sslsocket.setEnabledProtocols(new String[] {"TLSv1"});
		
		OutputStream outputstream = sslsocket.getOutputStream();
		OutputStreamWriter outputstreamwriter = new OutputStreamWriter(outputstream);
		BufferedWriter bufferedwriter = new BufferedWriter(outputstreamwriter);

		BufferedReader stdIn = new BufferedReader(new InputStreamReader(sslsocket.getInputStream()));
		
		String request = String.format("POST %s HTTP/1.1\r\n",url.getFile()) + 
						"Content-Type: application/json\r\n" +
						"Content-Length: "+String.valueOf(postBody.length())+"\r\n\r\n" + postBody;
		
		bufferedwriter.write(request);
		bufferedwriter.flush();
		
		String response = "";
		String line;
		int responseLenght = 0;
		while ((line = stdIn.readLine()) != null) {
			if(line.startsWith("Content-Length")){
				String l = line.replaceAll("Content-Length: ", "");
				responseLenght = Integer.parseInt(l);
			}
			if(line.isEmpty()) break;//headerEnded = true;
		}
		if(responseLenght >0){
			char[] buffer = new char[responseLenght];
			stdIn.read(buffer, 0, responseLenght);
			//int i=stdIn.read(buffer, 0, responseLenght);
			//if(i != responseLenght)
			//	throw new StorageException("Could not read all server response!");
			response = new String(buffer);			
		}
		outputstream.close();
		stdIn.close();
		sslsocket.close();
		bufferedwriter.close();
		
		//System.out.println(String.format("::response: %s", response));
		/*
		HttpsURLConnection connection = (HttpsURLConnection) url.openConnection();
		connection.setRequestMethod("POST");
		connection.setRequestProperty("Content-Type", "application/json");
		String postLength = String.valueOf(postBody.length());
		connection.setRequestProperty("Content-Length", postLength);
		connection.setDoOutput(true);
		OutputStreamWriter wr = new OutputStreamWriter(connection.getOutputStream());

		wr.write(postBody);

		wr.flush();
		
		String result = "";
		if(connection.getResponseCode() != 200){
			BufferedReader rd = new BufferedReader(new InputStreamReader(connection.getErrorStream()));
			String line;
			while ((line = rd.readLine()) != null) {
				result += line;
			}
			rd.close();
			
			throw new Exception(result);
		}else{
			BufferedReader rd = new BufferedReader(new InputStreamReader(connection.getInputStream()));
			String line;
			while ((line = rd.readLine()) != null) {
				result += line;
			}
			rd.close();
		}
		wr.close();

		return result;
		*/
		return response;
	}
	
	
	private static String readResponseBody(InputStream responseBody){
		String result = "";
		
		if(responseBody != null){
			BufferedReader rd = new BufferedReader(new InputStreamReader(responseBody));
			String line;
			try {
				while ((line = rd.readLine()) != null) {
					result += line;
				}
				rd.close();
			} catch (Exception e) { 
				result = e.getMessage();
			} 
		}
		
		return result;
	}
}
