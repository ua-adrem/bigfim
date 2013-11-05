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

import org.apache.mahout.fpm.bigfim.BigFIMDriver;
import org.apache.mahout.fpm.disteclat.DistEclatDriver;
import org.apache.mahout.fpm.util.Config;

/**
 * Driver class for BigData algorithms and tools
 * 
 * @author Sandy Moens & Emin Aksehirli
 */
public class FimDriver {
  
  public static enum FimVersion {
    DISTECLAT, BIGFIM
  }
  
  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      printHelp();
      return;
    }
    
    Config config = new Config();
    config.readConfig(args[0]);
    
    if (!config.isValid()) {
      System.out.println("Error in configuration file. Aborting!");
      return;
    }
    
    switch (config.getVersion()) {
      case DISTECLAT: {
        DistEclatDriver.main(new String[] {args[0]});
        break;
      }
      case BIGFIM: {
        BigFIMDriver.main(new String[] {args[0]});
        break;
      }
      default: {
        System.out.println("Invalid version specified!");
        printHelp();
      }
    }
  }
  
  private static void printHelp() {
    System.out.println("Please specify: [configFile]");
    System.out.println("For more information on config file please check example folder.");
  }
}
