/**
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
package org.apache.mahout.fpm;

import java.lang.reflect.Field;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({org.apache.mahout.fpm.bigfim.AllTests.class, org.apache.mahout.fpm.eclat.util.AllTests.class,
    org.apache.mahout.fpm.disteclat.DistEclatDriverTest.class})
public class AllTests {
  
  public static void setField(Object target, String fieldname, Object value) throws NoSuchFieldException,
      IllegalAccessException {
    Field field = findDeclaredField(target.getClass(), fieldname);
    field.setAccessible(true);
    field.set(target, value);
  }
  
  public static Field findDeclaredField(Class<?> inClass, String fieldname) throws NoSuchFieldException {
    while (!Object.class.equals(inClass)) {
      for (Field field : inClass.getDeclaredFields()) {
        if (field.getName().equalsIgnoreCase(fieldname)) {
          return field;
        }
      }
      inClass = inClass.getSuperclass();
    }
    throw new NoSuchFieldException();
  }
  
}
