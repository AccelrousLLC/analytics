package com.nr8.analytics.r8port.config.impl;


import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.Maps;
import com.nr8.analytics.r8port.JsonUtils;
import com.nr8.analytics.r8port.config.ConfigLoader;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.UsernamePasswordCredentials;
import org.apache.commons.httpclient.auth.AuthScope;
import org.apache.commons.httpclient.methods.GetMethod;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Using Consul (www.consul.io) as the data source, grab the configuration at the
 * specified Key (combination of the environment and the provided key).
 *
 * URI is expected to be the URL of the Consul server with appropriate auth configuration.
 *
 * For example:  consul://consul.nr8.com:8500?username=blah&password=meanioio?secure=false
 *
 * Options:
 *   - username (String): Basic Auth Username
 *   - password (String): Basic Auth Password
 *   - secure (Boolean): Use SSL?
 *
 */
public class ConsulConfigLoader implements ConfigLoader {

  public static final String URI_PREFIX = "consul://";

  protected final String uri;

  protected final HttpClient client;

  protected Map<String, String> uriParams;

  /**
   *
   * @param uri
   */
  public ConsulConfigLoader(String uri){
    this.uri = uri;
    this.uriParams = parseURI(uri);
    this.client = createHttpClient(this.uriParams);
  }

  @Override
  public <T> Optional<T> getConfig(String environment, String key, Class<T> clazz) {
    String path = convertDotsToSlashes(key);
    GetMethod request = createRequestContext(this.uriParams, String.format("%s/%s", environment, path));
    try {
      Optional<String> body = getResponseBody(this.client, request);
      String bodyString = body.get();
      T configObject = parseResponseBody(bodyString, clazz);
      return Optional.of(configObject);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return Optional.absent();
  }

  public static <T> T parseResponseBody(String body, Class<T> objType){
    ConsulKVRecord[] records = JsonUtils.deserialize(body, ConsulKVRecord[].class);
    String decodedRecord = new String(Base64.decodeBase64(records[0].getValue()));
    return JsonUtils.deserialize(decodedRecord, objType);
  }

  public static Optional<String> getResponseBody(HttpClient client, HttpMethod method) throws IOException {
    int status = client.executeMethod(method);
    if (status < 400){
      return Optional.of(method.getResponseBodyAsString());
    }
    return Optional.absent();
  }

  static String convertDotsToSlashes(String key){
    return key.replace('.', '/');
  }

  static HttpClient createHttpClient(Map<String, String> params){
    HttpClient client = new HttpClient();
    if (params.containsKey("username") && params.containsKey("password")){
      String realm = params.getOrDefault("realm", "nr8");
      client.getState().setCredentials(
          new AuthScope(params.get("hostname"), Integer.parseInt(params.get("port")), realm),
          new UsernamePasswordCredentials(params.get("username"), params.get("password"))
      );
    }
    else if (params.containsKey("username") || params.containsKey("password")){
      throw new RuntimeException("Cannot provide a username or password without the other parameter.");
    }
    return client;
  }

  static GetMethod createRequestContext(Map<String, String> params, String relativePath){
    String protocol = (params.containsKey("secure"))? "https://" : "http://";
    String url = protocol + params.get("url");
    String fullUrl = String.format("%s/v1/kv/%s", url, relativePath);
    return new GetMethod(fullUrl);
  };

  static Map<String, String> parseURI(String uri){
    String[] splits = uri.split("[?]");
    Map<String, String> queryParams = Maps.newHashMap();

    if (splits.length > 1){
      queryParams = Splitter.on('&').withKeyValueSeparator("=").split(splits[1]);
    }

    HashMap<String, String> params = Maps.newHashMap(queryParams);
    int typeIndex = uri.indexOf(URI_PREFIX) + URI_PREFIX.length();
    String url = splits[0].substring(typeIndex);
    String hostname = (url.contains(":"))? url.split(":")[0] : url;
    params.put("hostname", hostname);
    String port = (url.contains(":"))? url.split(":")[1] : "80";
    params.put("port", port);
    params.put("url", url);
    return params;
  }
}
