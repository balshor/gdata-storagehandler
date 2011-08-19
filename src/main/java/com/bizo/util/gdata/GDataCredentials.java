package com.bizo.util.gdata;

/** Simple bean containing credentials necessary for connecting to a Google spreadsheet. */
public class GDataCredentials {

  private final String user;
  private final String consumerKey;
  private final String consumerSecret;

  public GDataCredentials(String user, String consumerKey, String consumerSecret) {
    this.user = user;
    this.consumerKey = consumerKey;
    this.consumerSecret = consumerSecret;
  }

  public String getUser() {
    return user;
  }

  public String getConsumerKey() {
    return consumerKey;
  }

  public String getConsumerSecret() {
    return consumerSecret;
  }

}
