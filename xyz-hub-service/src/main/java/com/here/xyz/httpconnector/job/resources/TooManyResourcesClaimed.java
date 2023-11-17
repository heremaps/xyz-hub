package com.here.xyz.httpconnector.job.resources;

public class TooManyResourcesClaimed extends Exception {

  public TooManyResourcesClaimed(String message) {
    super(message);
  }
}
