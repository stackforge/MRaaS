package org.hpswift.service;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Formatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;
import java.util.TimeZone;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;

public class Swift {

    private Credentials credentials = null;
    private HttpClient client = null;

    private String authToken = "";
    private String storageUrl = "";

    private static JsonNodeFactory fact = JsonNodeFactory.instance;
    private static ObjectMapper m = new ObjectMapper();


    private void getAuth() throws Exception {
        JsonNode auth = credentials.getAuth("object-store");
        authToken = auth.get("token").getTextValue();
        storageUrl = auth.get("url").getTextValue();
    }


    /*
    public byte[] get(String container, String key) throws Exception {
        getAuth();
        HttpGet cmd = new HttpGet(storageUrl + "/" + container +"/" + key);
        cmd.setHeader("X-Auth-Token", authToken);
        HttpResponse response = client.execute(cmd);
        int code = response.getStatusLine().getStatusCode();
        credentials.checkCode(response,code,"GET:" + container +":" + key);
        int len = (int) response.getEntity().getContentLength();
        byte buff[] = new byte[len];
        InputStream r = response.getEntity().getContent();
        int off = 0;
        while (true) {
            int len1 = r.read(buff, off, len);
            if (len1 <= 0) break;
            off += len1;
            len -= len1;
        }
        r.close();
        //cmd.abort();    
        return buff;
    }
     */

    public InputStream getS(String container, String key) throws Exception {
        HttpGet cmd = new HttpGet(storageUrl + "/" + container +"/" + key);
        cmd.setHeader("X-Auth-Token", authToken);

        HttpResponse response = client.execute(cmd);
        int code = response.getStatusLine().getStatusCode();
        if (code == 404) {
            return null;
        }

        credentials.checkCode(response,code,"GET:" + container +":" + key);
        InputStream r = response.getEntity().getContent();
        return r;
    }


    public InputStream getS(String container, String key,long start) throws Exception {
        HttpGet cmd = new HttpGet(storageUrl + "/" + container +"/" + key);
        cmd.setHeader("X-Auth-Token", authToken);
        cmd.setHeader("Range","bytes="+start+"-");

        HttpResponse response = client.execute(cmd);
        int code = response.getStatusLine().getStatusCode();
        if (code == 404) {
            return null;
        }

        credentials.checkCode(response,code,"GET:" + container +":" + key);
        InputStream r = response.getEntity().getContent();
        return r;
    }

    public SwiftMetadata getMetaData(String container, String key) throws Exception {
        getAuth();
        HttpHead cmd = new HttpHead(storageUrl + "/" + container +"/" + key);
        cmd.setHeader("X-Auth-Token", authToken);

        HttpResponse response = client.execute(cmd);
        int code = response.getStatusLine().getStatusCode();
        if (code == 404) {
            cmd.abort();
            return null; //not found
        }

        credentials.checkCode(response,code,"HEAD:" + container +":" + key);
        String slen = response.getHeaders("Content-Length")[0].getValue();
        String smod = response.getHeaders("Last-Modified")[0].getValue();

        // TODO decode len and mod
        long len = slen.length();
        SimpleDateFormat format = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss Z");
        format.setTimeZone(TimeZone.getTimeZone("GMT"));
        Date date = format.parse(smod);
        long mod =  date.getTime();  
        cmd.abort();

        return new SwiftMetadata(len, mod);
    }

    public void put(String container, String key, byte val[]) throws Exception {
        getAuth();
        HttpPut cmd = new HttpPut(storageUrl + "/" + container +"/" + key);
        cmd.setHeader("X-Auth-Token", authToken);

        HttpEntity e = new ByteArrayEntity(val);
        cmd.setEntity(e);

        HttpResponse response = client.execute(cmd);
        int code = response.getStatusLine().getStatusCode();

        credentials.checkCode(response,code,"PUT:" + container +":" + key);
        cmd.abort();    
    }

    public void copy(String container, String srcKey, String dstKey) throws Exception {
        getAuth();
        HttpPut cmd = new HttpPut(storageUrl + "/" + container +"/" + dstKey);
        cmd.setHeader("X-Auth-Token", authToken);
        cmd.setHeader("X-Copy-From", "/" + container +"/" + srcKey);
        HttpEntity e = new StringEntity("");
        cmd.setEntity(e);

        HttpResponse response = client.execute(cmd);
        int code = response.getStatusLine().getStatusCode();

        credentials.checkCode(response,code,"COPY:" + container +":" + srcKey +":" + dstKey);
        cmd.abort();    
    }

    private static String bytesToHexString(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        Formatter formatter = new Formatter(sb);
        for (byte b : bytes) {
            formatter.format("%02x", b);
        }
        return sb.toString();
    }

    public void putStream(String container, String key, InputStream in,byte[] md5) throws Exception {
        // Note: value size must be less than 5GB
        getAuth();
        HttpPut cmd = new HttpPut(storageUrl + "/" + container +"/" + key);
        cmd.setHeader("X-Auth-Token", authToken);
        cmd.setHeader("ETag", bytesToHexString(md5));
        HttpEntity e = new InputStreamEntity(in,-1);
        cmd.setEntity(e);

        HttpResponse response = client.execute(cmd);
        int code = response.getStatusLine().getStatusCode();

        credentials.checkCode(response,code,"PUT:" + container +":" + key);
        cmd.abort();    
    }

    public void delete(String container, String key) throws Exception {
        getAuth();
        HttpDelete cmd = new HttpDelete(storageUrl + "/" + container +"/" + key);
        cmd.setHeader("X-Auth-Token", authToken);

        HttpResponse response = client.execute(cmd);
        int code = response.getStatusLine().getStatusCode();

        credentials.checkCode(response,code,"DELETE:" + container +":" + key);
        cmd.abort();    
    }

    public JsonNode list(String container,String prefix, String delimiter, int limit, String marker) throws Exception {
        String q = "?format=json";
        if (limit > 0) {
            q = q + ("&limit=" + limit);
        }

        if (prefix != null) {
            q = q + ("&prefix=" + URLEncoder.encode(prefix,"UTF-8"));
        }

        if (delimiter != null) {
            q = q + ("&delimiter=" + URLEncoder.encode(delimiter,"UTF-8"));
        }

        if (marker != null) {
            q = q + ("&marker=" + marker);
        }

        getAuth();
        HttpGet cmd = new HttpGet(storageUrl + "/" + container + q);
        cmd.setHeader("X-Auth-Token", authToken);

        HttpResponse response = client.execute(cmd);
        int len = 0;
        HttpEntity ent = response.getEntity();
        if (ent != null) {
            len = (int) ent.getContentLength();
        }

        if (len == 0) {
            return fact.arrayNode();
        }

        StringBuilder sb = new StringBuilder();
        BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
        String line = null;
        while ((line = rd.readLine()) != null) {
            sb.append(line + "\n");
        }
        rd.close();

        String rs = sb.toString();  
        ArrayNode result = (ArrayNode) m.readValue(rs,JsonNode.class);
        return result;
    }


    public List<String> allContainers() throws Exception {
        getAuth();
        HttpGet cmd = new HttpGet(storageUrl);
        cmd.setHeader("X-Auth-Token", authToken);

        HttpResponse response = client.execute(cmd);
        int len = 0;
        HttpEntity ent = response.getEntity();
        if (ent != null) {
            len = (int) ent.getContentLength();
        }
        if (len == 0) {
            return new ArrayList<String>();
        }

        StringBuilder sb = new StringBuilder();
        BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
        String line = null;
        while ((line = rd.readLine()) != null) {
            sb.append(line + "\n");
        }
        rd.close();
        cmd.abort();    

        String result = sb.toString();   
        ArrayList<String> al = new ArrayList<String>(Arrays.asList(result.split("\n")));
        return al;
    }


    public Swift(Credentials credentials) throws Exception {
        this.credentials = credentials;
        client = credentials.client;
    }
}
