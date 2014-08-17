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

package org.apache.solr.core;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.BeforeClass;
import org.junit.Test;
import org.xml.sax.SAXException;

public class TestCoreContainer extends SolrTestCaseJ4 {
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }


  public void testShareSchema() throws IOException, ParserConfigurationException, SAXException {
    
    final File solrHomeDirectory = new File(TEMP_DIR, this.getClass().getName()
        + "_shareSchema");

    if (solrHomeDirectory.exists()) {
      FileUtils.deleteDirectory(solrHomeDirectory);
    }
    assertTrue("Failed to mkdirs workDir", solrHomeDirectory.mkdirs());
    
    FileUtils.copyDirectory(new File(SolrTestCaseJ4.TEST_HOME()), solrHomeDirectory);
    
    File fconf = new File(solrHomeDirectory, "solr.xml");

    final CoreContainer cores = new CoreContainer(solrHomeDirectory.getAbsolutePath());
    System.setProperty("shareSchema", "true");
    cores.load(solrHomeDirectory.getAbsolutePath(), fconf);
    try {
      cores.setPersistent(false);
      assertTrue(cores.isShareSchema());
      
      CoreDescriptor descriptor1 = new CoreDescriptor(cores, "core1", "./collection1");
      SolrCore core1 = cores.create(descriptor1);
      
      CoreDescriptor descriptor2 = new CoreDescriptor(cores, "core2", "./collection1");
      SolrCore core2 = cores.create(descriptor2);
      
      assertSame(core1.getSchema(), core2.getSchema());
      
      core1.close();
      core2.close();
    } finally {
      cores.shutdown();
      System.clearProperty("shareSchema");
    }
  }
  
  @Test
  public void testReload() throws Exception {
    final CoreContainer cc = h.getCoreContainer();
    
    class TestThread extends Thread {
      @Override
      public void run() {
        cc.reload("collection1");
      }
    }
    
    List<Thread> threads = new ArrayList<Thread>();
    int numThreads = 4;
    for (int i = 0; i < numThreads; i++) {
      threads.add(new TestThread());
    }
    
    for (Thread thread : threads) {
      thread.start();
    }
    
    for (Thread thread : threads) {
      thread.join();
    }

  }

  @Test
  public void testPersist() throws Exception {
    final File workDir = new File(TEMP_DIR, this.getClass().getName()
        + "_persist");
    if (workDir.exists()) {
      FileUtils.deleteDirectory(workDir);
    }
    assertTrue("Failed to mkdirs workDir", workDir.mkdirs());
    
    final CoreContainer cores = h.getCoreContainer();
    cores.setPersistent(true); // is this needed since we make explicit calls?

    String instDir = null;
    {
      SolrCore template = null;
      try {
        template = cores.getCore("collection1");
        instDir = template.getCoreDescriptor().getInstanceDir();
      } finally {
        if (null != template) template.close();
      }
    }
    
    final File instDirFile = new File(instDir);
    assertTrue("instDir doesn't exist: " + instDir, instDirFile.exists());
    
    // sanity check the basic persistence of the default init
    
    final File oneXml = new File(workDir, "1.solr.xml");
    cores.persistFile(oneXml);

    assertXmlFile(oneXml, "/solr[@persistent='true']",
        "/solr/cores[@defaultCoreName='collection1' and not(@transientCacheSize)]",
        "/solr/cores/core[@name='collection1' and @instanceDir='" + instDir +
        "' and @transient='false' and @loadOnStartup='true' ]", "1=count(/solr/cores/core)");

    // create some new cores and sanity check the persistence
    
    final File dataXfile = new File(workDir, "dataX");
    final String dataX = dataXfile.getAbsolutePath();
    assertTrue("dataXfile mkdirs failed: " + dataX, dataXfile.mkdirs());
    
    final File instYfile = new File(workDir, "instY");
    FileUtils.copyDirectory(instDirFile, instYfile);
    
    // :HACK: dataDir leaves off trailing "/", but instanceDir uses it
    final String instY = instYfile.getAbsolutePath() + "/";
    
    final CoreDescriptor xd = new CoreDescriptor(cores, "X", instDir);
    xd.setDataDir(dataX);
    
    final CoreDescriptor yd = new CoreDescriptor(cores, "Y", instY);
    
    SolrCore x = null;
    SolrCore y = null;
    try {
      x = cores.create(xd);
      y = cores.create(yd);
      cores.register(x, false);
      cores.register(y, false);
      
      assertEquals("cores not added?", 3, cores.getCoreNames().size());
      
      final File twoXml = new File(workDir, "2.solr.xml");
      cores.transientCacheSize = 32;

      cores.persistFile(twoXml);

      assertXmlFile(twoXml, "/solr[@persistent='true']",
          "/solr/cores[@defaultCoreName='collection1' and @transientCacheSize='32']",
          "/solr/cores/core[@name='collection1' and @instanceDir='" + instDir
              + "']", "/solr/cores/core[@name='X' and @instanceDir='" + instDir
              + "' and @dataDir='" + dataX + "']",
          "/solr/cores/core[@name='Y' and @instanceDir='" + instY + "']",
          "3=count(/solr/cores/core)");

      // delete a core, check persistence again
      assertNotNull("removing X returned null", cores.remove("X"));
      
      final File threeXml = new File(workDir, "3.solr.xml");
      cores.persistFile(threeXml);
      
      assertXmlFile(threeXml, "/solr[@persistent='true']",
          "/solr/cores[@defaultCoreName='collection1']",
          "/solr/cores/core[@name='collection1' and @instanceDir='" + instDir
              + "']", "/solr/cores/core[@name='Y' and @instanceDir='" + instY
              + "']", "2=count(/solr/cores/core)");
      
      // sanity check that persisting w/o changes has no changes
      
      final File fourXml = new File(workDir, "4.solr.xml");
      cores.persistFile(fourXml);
      
      assertTrue("3 and 4 should be identical files",
          FileUtils.contentEquals(threeXml, fourXml));
      
    } finally {
      // y is closed by the container, but
      // x has been removed from the container
      if (x != null) {
        try {
          x.close();
        } catch (Exception e) {
          log.error("", e);
        }
      }
    }
  }
  
  public void assertXmlFile(final File file, String... xpath)
      throws IOException, SAXException {
    
    try {
      String xml = FileUtils.readFileToString(file, "UTF-8");
      String results = h.validateXPath(xml, xpath);
      if (null != results) {
        String msg = "File XPath failure: file=" + file.getPath() + " xpath="
            + results + "\n\nxml was: " + xml;
        fail(msg);
      }
    } catch (XPathExpressionException e2) {
      throw new RuntimeException("XPath is invalid", e2);
    }
  }

  public void testNoCores() throws IOException, ParserConfigurationException, SAXException {
    //create solrHome
    File solrHomeDirectory = new File(TEMP_DIR, this.getClass().getName()
        + "_noCores");
    if (solrHomeDirectory.exists()) {
      FileUtils.deleteDirectory(solrHomeDirectory);
    }
    assertTrue("Failed to mkdirs workDir", solrHomeDirectory.mkdirs());
    try {
      File solrXmlFile = new File(solrHomeDirectory, "solr.xml");
      BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(solrXmlFile), IOUtils.CHARSET_UTF_8));
      out.write(EMPTY_SOLR_XML);
      out.close();
    } catch (IOException e) {
      FileUtils.deleteDirectory(solrHomeDirectory);
      throw e;
    }
    
    //init
    System.setProperty("solr.solr.home", solrHomeDirectory.getAbsolutePath());
    CoreContainer.Initializer init = new CoreContainer.Initializer();
    CoreContainer cores = null;
    try {
      cores = init.initialize();
    }
    catch(Exception e) {
      fail("CoreContainer not created" + e.getMessage());
    }
    try {
      //assert cero cores
      assertEquals("There should not be cores", 0, cores.getCores().size());
      
      FileUtils.copyDirectory(new File(SolrTestCaseJ4.TEST_HOME(), "collection1"), solrHomeDirectory);
      //add a new core
      CoreDescriptor coreDescriptor = new CoreDescriptor(cores, "core1", solrHomeDirectory.getAbsolutePath());
      SolrCore newCore = cores.create(coreDescriptor);
      cores.register(newCore, false);
      
      //assert one registered core
      assertEquals("There core registered", 1, cores.getCores().size());
      
      newCore.close();
      cores.remove("core1");
      //assert cero cores
      assertEquals("There should not be cores", 0, cores.getCores().size());
    } finally {
      cores.shutdown();
      FileUtils.deleteDirectory(solrHomeDirectory);
    }

  }
  
  private static final String EMPTY_SOLR_XML ="<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n" +
      "<solr persistent=\"false\">\n" +
      "  <cores adminPath=\"/admin/cores\">\n" +
      "  </cores>\n" +
      "</solr>";
  
}
