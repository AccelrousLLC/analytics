package com.nr8.analytics.r8port.services;


import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.nr8.analytics.r8port.R8port;

import java.util.List;
import java.util.concurrent.Future;

public interface R8portStorageService {

  Future appendToStorage(List<R8port> r8ports);

  Future<List<R8port>> get(String sessionID);
}
