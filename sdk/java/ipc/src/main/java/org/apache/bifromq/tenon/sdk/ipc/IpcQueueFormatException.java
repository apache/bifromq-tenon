/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.bifromq.tenon.sdk.ipc;

import java.io.IOException;

/**
 * Stable checked failure while validating IPC Queue bytes or arithmetic.
 *
 * <p>A Queue is a mapped file shared with its peer process, so a malformed Queue is an input/output
 * failure of that mapping that carries the language-neutral contract's code.
 */
public final class IpcQueueFormatException extends IOException {
  private static final long serialVersionUID = 1L;

  private final String code;

  IpcQueueFormatException(String code, String message) {
    super(message);
    this.code = code;
  }

  /** Returns the stable machine-readable failure code. */
  public String code() {
    return code;
  }
}
