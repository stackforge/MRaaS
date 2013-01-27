package org.hpswift.service;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.params.ConnRoutePNames;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

public class Credentials {
    private String accessKey;
    private String secretKey;
    private String tenantId;
    private String authUri;

    public HttpClient client = null;

    private static JsonNodeFactory fact = JsonNodeFactory.instance;
    private static ObjectMapper m = new ObjectMapper();


    public Credentials(String aKey, String sKey, String tId, String aUri){
        accessKey = aKey;
        secretKey  =  sKey;
        tenantId = tId;
        authUri = aUri;
        ThreadSafeClientConnManager cm = new ThreadSafeClientConnManager();
        cm.setMaxTotal(500);
        cm.setDefaultMaxPerRoute(500);
        client = new DefaultHttpClient(cm);
        HttpHost proxy = new HttpHost("proxy-ccy.houston.hp.com", 8080);
        client.getParams().setParameter(ConnRoutePNames.DEFAULT_PROXY, proxy);
    }

    public void checkCode(HttpResponse response,int code,String msg) throws Exception {
        if (code >= 200 && code < 300) {
            return;
        }

        int len = 0;
        HttpEntity ent = response.getEntity();
        if (ent != null) {
            len = (int) ent.getContentLength();
        }

        if (len != 0) {
            StringBuilder sb = new StringBuilder();
            BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
            String line = null;
            while ((line = rd.readLine()) != null) {
                sb.append(line + "\n");
            }
            rd.close();
            String s = sb.toString();

            // System.out.println(s);
        }
        throw new Exception(code + " - " + msg);
    }

    public JsonNode getAuth(String type) throws Exception {
        // TODO return same token if not expired
        ObjectNode info = fact.objectNode();
        ObjectNode auth = fact.objectNode();
        ObjectNode cred = fact.objectNode();

        info.put("auth", auth);
        auth.put("apiAccessKeyCredentials", cred);
        cred.put("accessKey", accessKey);
        cred.put("secretKey", secretKey);
        auth.put("tenantId", tenantId);

        StringWriter w = new StringWriter();
        m.writeValue(w, info);
        String infos = w.toString();

        HttpPost post = new HttpPost(authUri + "tokens");
        HttpEntity e = new StringEntity(infos);
        post.setEntity(e);
        post.setHeader("Accept", "application/json");
        post.setHeader("Content-Type", "application/json");

        HttpResponse response = client.execute(post);
        int code = response.getStatusLine().getStatusCode();
        checkCode(response, code, "AUTH");

        InputStream r = response.getEntity().getContent();
        ObjectNode n = m.readValue(r, ObjectNode.class);
        String authToken = n.get("access").get("token").get("id").getTextValue();
        String storageUrl = "";
        JsonNode cat = n.get("access").get("serviceCatalog");

        for (int i = 0; i < cat.size(); i++) {
            JsonNode service = cat.get(i);
            if (service.get("type").getTextValue().equals(type)) {
                JsonNode ep = service.get("endpoints");
                JsonNode ep0 = ep.get(0);
                JsonNode puburl = ep0.get("publicURL");
                storageUrl = puburl.getTextValue();
                break;
            }
        }

        r.close();
        post.abort();   

        ObjectNode result = fact.objectNode();
        result.put("token",authToken);
        result.put("url",storageUrl);
        return result;
    }

    public String getAccessKey(){
        return accessKey;
    }

    public String getSecretKey(){
        return secretKey;
    }

    public String getTenantId(){
        return tenantId;
    }

    String getAuthUri(){
        return authUri;
    }
}
