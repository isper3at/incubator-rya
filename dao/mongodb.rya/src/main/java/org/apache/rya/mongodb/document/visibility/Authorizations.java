/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rya.mongodb.document.visibility;

import static com.google.common.base.Charsets.UTF_8;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import com.google.common.base.Charsets;

/**
 * A collection of authorization strings.
 */
public class Authorizations implements Iterable<byte[]>, Serializable, AuthorizationContainer {

  private static final long serialVersionUID = 1L;

  private final Set<ByteSequence> auths = new HashSet<ByteSequence>();
  private final List<byte[]> authsList = new ArrayList<byte[]>(); // sorted order

  /**
   * An empty set of authorizations.
   */
  public static final Authorizations EMPTY = new Authorizations();

  private static final boolean[] validAuthChars = new boolean[256];

  /**
   * A special header string used when serializing instances of this class.
   *
   * @see #serialize()
   */
  public static final String HEADER = "!AUTH1:";

  static {
    for (int i = 0; i < 256; i++) {
      validAuthChars[i] = false;
    }

    for (int i = 'a'; i <= 'z'; i++) {
      validAuthChars[i] = true;
    }

    for (int i = 'A'; i <= 'Z'; i++) {
      validAuthChars[i] = true;
    }

    for (int i = '0'; i <= '9'; i++) {
      validAuthChars[i] = true;
    }

    validAuthChars['_'] = true;
    validAuthChars['-'] = true;
    validAuthChars[':'] = true;
    validAuthChars['.'] = true;
    validAuthChars['/'] = true;
  }

  static final boolean isValidAuthChar(final byte b) {
    return validAuthChars[0xff & b];
  }

  private void checkAuths() {
    final Set<ByteSequence> sortedAuths = new TreeSet<ByteSequence>(auths);

    for (final ByteSequence bs : sortedAuths) {
      if (bs.length() == 0) {
        throw new IllegalArgumentException("Empty authorization");
      }

      authsList.add(bs.toArray());
    }
  }

  /**
   * Constructs an authorization object from a collection of string authorizations that have each already been encoded as UTF-8 bytes. Warning: This method does
   * not verify that each encoded string is valid UTF-8.
   *
   * @param authorizations
   *          collection of authorizations, as strings encoded in UTF-8
   * @throws IllegalArgumentException
   *           if authorizations is null
   * @see #Authorizations(String...)
   */
  public Authorizations(final Collection<byte[]> authorizations) {
    ArgumentChecker.notNull(authorizations);
    for (final byte[] auth : authorizations) {
		auths.add(new ArrayByteSequence(auth));
	}
    checkAuths();
  }

  /**
   * Constructs an authorization object from a list of string authorizations that have each already been encoded as UTF-8 bytes. Warning: This method does not
   * verify that each encoded string is valid UTF-8.
   *
   * @param authorizations
   *          list of authorizations, as strings encoded in UTF-8 and placed in buffers
   * @throws IllegalArgumentException
   *           if authorizations is null
   * @see #Authorizations(String...)
   */
  public Authorizations(final List<ByteBuffer> authorizations) {
    ArgumentChecker.notNull(authorizations);
    for (final ByteBuffer buffer : authorizations) {
      auths.add(new ArrayByteSequence(ByteBufferUtil.toBytes(buffer)));
    }
    checkAuths();
  }

  /**
   * Constructs an authorizations object from a serialized form. This is NOT a constructor for a set of authorizations of size one. Warning: This method does
   * not verify that the encoded serialized form is valid UTF-8.
   *
   * @param authorizations
   *          a serialized authorizations string produced by {@link #getAuthorizationsArray()} or {@link #serialize()}, converted to UTF-8 bytes
   * @throws IllegalArgumentException
   *           if authorizations is null
   */
  public Authorizations(final byte[] authorizations) {

    ArgumentChecker.notNull(authorizations);

    String authsString = new String(authorizations, UTF_8);
    if (authsString.startsWith(HEADER)) {
      // it's the new format
      authsString = authsString.substring(HEADER.length());
      if (authsString.length() > 0) {
        for (final String encAuth : authsString.split(",")) {
          final byte[] auth = Base64.decodeBase64(encAuth.getBytes(UTF_8));
          auths.add(new ArrayByteSequence(auth));
        }
        checkAuths();
      }
    } else {
      // it's the old format
      if (authorizations.length > 0) {
		setAuthorizations(authsString.split(","));
	}
    }
  }

  /**
   * Constructs an empty set of authorizations.
   *
   * @see #Authorizations(String...)
   */
  public Authorizations() {}

  /**
   * Constructs an authorizations object from a set of human-readable authorizations.
   *
   * @param authorizations
   *          array of authorizations
   * @throws IllegalArgumentException
   *           if authorizations is null
   */
  public Authorizations(final String... authorizations) {
    setAuthorizations(authorizations);
  }

  private void setAuthorizations(final String... authorizations) {
    ArgumentChecker.notNull(authorizations);
    auths.clear();
    for (String str : authorizations) {
      str = str.trim();
      auths.add(new ArrayByteSequence(str.getBytes(UTF_8)));
    }

    checkAuths();
  }

  /**
   * Returns a serialized form of these authorizations.
   *
   * @return serialized form of these authorizations, as a string encoded in UTF-8
   * @see #serialize()
   */
  public byte[] getAuthorizationsArray() {
    return serialize().getBytes(UTF_8);
  }

  /**
   * Gets the authorizations in sorted order. The returned list is not modifiable.
   *
   * @return authorizations, each as a string encoded in UTF-8
   * @see #Authorizations(Collection)
   */
  public List<byte[]> getAuthorizations() {
    final ArrayList<byte[]> copy = new ArrayList<byte[]>(authsList.size());
    for (final byte[] auth : authsList) {
      final byte[] bytes = new byte[auth.length];
      System.arraycopy(auth, 0, bytes, 0, auth.length);
      copy.add(bytes);
    }
    return Collections.unmodifiableList(copy);
  }

  /**
   * Gets the authorizations in sorted order. The returned list is not modifiable.
   *
   * @return authorizations, each as a string encoded in UTF-8 and within a buffer
   */
  public List<ByteBuffer> getAuthorizationsBB() {
    final ArrayList<ByteBuffer> copy = new ArrayList<ByteBuffer>(authsList.size());
    for (final byte[] auth : authsList) {
      final byte[] bytes = new byte[auth.length];
      System.arraycopy(auth, 0, bytes, 0, auth.length);
      copy.add(ByteBuffer.wrap(bytes));
    }
    return Collections.unmodifiableList(copy);
  }

  /**
   * Gets the authorizations in sorted order. The returned list is not modifiable.
   *
   * @return authorizations, each as a string encoded in UTF-8
   */
  public List<String> getAuthorizationsStrings() {
    final List<String> copy = new ArrayList<>(authsList.size());
    for (final byte[] auth : authsList) {
      copy.add(new String(auth, Charsets.UTF_8));
    }
    return Collections.unmodifiableList(copy);
  }

  /**
   * Gets the authorizations in sorted order.
   *
   * @return authorizations, each as a string encoded in UTF-8
   */
  public String[] getAuthorizationsStringArray() {
    return getAuthorizationsStrings().toArray(new String[0]);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    String sep = "";
    for (final ByteSequence auth : auths) {
      sb.append(sep);
      sep = ",";
      sb.append(new String(auth.toArray(), UTF_8));
    }

    return sb.toString();
  }

  /**
   * Checks whether this object contains the given authorization.
   *
   * @param auth
   *          authorization, as a string encoded in UTF-8
   * @return true if authorization is in this collection
   */
  public boolean contains(final byte[] auth) {
    return auths.contains(new ArrayByteSequence(auth));
  }

  /**
   * Checks whether this object contains the given authorization. Warning: This method does not verify that the encoded string is valid UTF-8.
   *
   * @param auth
   *          authorization, as a string encoded in UTF-8
   * @return true if authorization is in this collection
   */
  @Override
  public boolean contains(final ByteSequence auth) {
    return auths.contains(auth);
  }

  /**
   * Checks whether this object contains the given authorization.
   *
   * @param auth
   *          authorization
   * @return true if authorization is in this collection
   */
  public boolean contains(final String auth) {
    return auths.contains(new ArrayByteSequence(auth));
  }

  @Override
  public boolean equals(final Object o) {
    if (o == null) {
      return false;
    }

    if (o instanceof Authorizations) {
      final Authorizations ao = (Authorizations) o;

      return auths.equals(ao.auths);
    }

    return false;
  }

  @Override
  public int hashCode() {
    int result = 0;
    for (final ByteSequence b : auths) {
		result += b.hashCode();
	}
    return result;
  }

  /**
   * Gets the size of this collection of authorizations.
   *
   * @return collection size
   */
  public int size() {
    return auths.size();
  }

  /**
   * Checks if this collection of authorizations is empty.
   *
   * @return true if this collection contains no authorizations
   */
  public boolean isEmpty() {
    return auths.isEmpty();
  }

  @Override
  public Iterator<byte[]> iterator() {
    return getAuthorizations().iterator();
  }

  /**
   * Returns a serialized form of these authorizations. Convert the returned string to UTF-8 bytes to deserialize with {@link #Authorizations(byte[])}.
   *
   * @return serialized form of authorizations
   */
  public String serialize() {
    final StringBuilder sb = new StringBuilder(HEADER);
    String sep = "";
    for (final byte[] auth : authsList) {
      sb.append(sep);
      sep = ",";
      sb.append(Base64.encodeBase64String(auth));
    }

    return sb.toString();
  }
}
