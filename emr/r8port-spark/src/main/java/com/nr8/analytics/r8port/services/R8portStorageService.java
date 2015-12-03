package com.nr8.analytics.r8port.services;


import com.nr8.analytics.r8port.R8port;
import com.nr8.analytics.r8port.SessionLog;

import java.util.List;
import java.util.concurrent.Future;

public interface R8portStorageService {

  Future appendToStorage(List<R8port> r8ports);

  SessionLog get(String sessionID);
}
