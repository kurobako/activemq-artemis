/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.integration.management;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.api.core.management.QueueControl;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.core.settings.impl.AddressFullMessagePolicy;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.apache.activemq.artemis.utils.json.JSONArray;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 * This class contains tests for core management
 * functionalities that are affected by a server
 * in paging mode.
 */
public class ManagementWithPagingServerTest extends ManagementTestBase {

   private ActiveMQServer server;
   private ClientSession session1;
   private ClientSession session2;
   private ServerLocator locator;

   @Test
   public void testListMessagesAsJSON() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session1.createQueue(address, queue, null, true);

      QueueControl queueControl = createManagementControl(address, queue);

      int num = 1000;
      SenderThread sender = new SenderThread(address, num, 0);

      ReceiverThread receiver = new ReceiverThread(queue, num, 0);

      //kick off sender
      sender.start();

      //wait for all messages sent
      sender.join();
      assertNull(sender.getError());

      long count = queueControl.countMessages(null);

      assertEquals(num, count);

      String result = queueControl.listMessagesAsJSON(null);

      JSONArray array = new JSONArray(result);

      assertEquals(num, array.length());

      //kick off receiver
      receiver.start();
      receiver.join();
      assertNull(receiver.getError());

      result = queueControl.listMessagesAsJSON(null);

      array = new JSONArray(result);

      assertEquals(0, array.length());
   }

   @Test
   public void testListMessagesAsJSONWithFilter() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session1.createQueue(address, queue, null, true);

      QueueControl queueControl = createManagementControl(address, queue);

      int num = 1000;

      SimpleString key = new SimpleString("key");
      long matchingValue = RandomUtil.randomLong();
      long unmatchingValue = matchingValue + 1;
      String filter = key + " =" + matchingValue;

      byte[] body = new byte[64];
      ByteBuffer bb = ByteBuffer.wrap(body);
      for (int j = 1; j <= 64; j++) {
         bb.put(getSamplebyte(j));
      }

      ClientProducer producer = session1.createProducer(address);
      for (int i = 0; i < num; i++) {
         ClientMessage message = session1.createMessage(true);
         if (i % 2 == 0) {
            message.putLongProperty(key, matchingValue);
         }
         else {
            message.putLongProperty(key, unmatchingValue);
         }
         producer.send(message);
      }

      String jsonString = queueControl.listMessagesAsJSON(filter);
      Assert.assertNotNull(jsonString);
      JSONArray array = new JSONArray(jsonString);
      Assert.assertEquals(num / 2, array.length());
      Assert.assertEquals(matchingValue, array.getJSONObject(0).get("key"));

      long n = queueControl.countMessages(filter);
      assertEquals(num / 2, n);

      //drain out messages
      ReceiverThread receiver = new ReceiverThread(queue, num, 1);
      receiver.start();
      receiver.join();
   }

   //In this test, the management api listMessageAsJSon is called while
   //paging/depaging is going on. It makes sure that the implementation
   //of the api doesn't cause any exceptions during internal queue
   //message iteration.
   @Test
   public void testListMessagesAsJSONWhilePagingOnGoing() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session1.createQueue(address, queue, null, true);

      QueueControl queueControl = createManagementControl(address, queue);

      int num = 1000;
      SenderThread sender = new SenderThread(address, num, 1);

      ReceiverThread receiver = new ReceiverThread(queue, num, 2);

      ManagementThread console = new ManagementThread(queueControl);

      //kick off sender
      sender.start();

      //kick off jmx client
      console.start();

      //wait for all messages sent
      sender.join();
      assertNull(sender.getError());

      //kick off receiver
      receiver.start();

      receiver.join();
      assertNull(receiver.getError());

      console.exit();
      console.join();

      assertNull(console.getError());
   }

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      Configuration config = createDefaultInVMConfig().setJMXManagementEnabled(true);

      server = addServer(ActiveMQServers.newActiveMQServer(config, mbeanServer, true));

      AddressSettings defaultSetting = new AddressSettings().setPageSizeBytes(5120).setMaxSizeBytes(10240).setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE);

      server.getAddressSettingsRepository().addMatch("#", defaultSetting);

      server.start();

      locator = createInVMNonHALocator().setBlockOnNonDurableSend(false).setConsumerWindowSize(0);
      ClientSessionFactory sf = createSessionFactory(locator);
      session1 = sf.createSession(false, true, false);
      session1.start();
      session2 = sf.createSession(false, true, false);
      session2.start();
   }

   private class SenderThread extends Thread {

      private SimpleString address;
      private int num;
      private long delay;
      private volatile Exception error = null;

      public SenderThread(SimpleString address, int num, long delay) {
         this.address = address;
         this.num = num;
         this.delay = delay;
      }

      @Override
      public void run() {
         ClientProducer producer;

         byte[] body = new byte[128];
         ByteBuffer bb = ByteBuffer.wrap(body);
         for (int j = 1; j <= 128; j++) {
            bb.put(getSamplebyte(j));
         }

         try {
            producer = session1.createProducer(address);

            for (int i = 0; i < num; i++) {
               ClientMessage message = session1.createMessage(true);
               ActiveMQBuffer buffer = message.getBodyBuffer();
               buffer.writeBytes(body);
               producer.send(message);
               try {
                  Thread.sleep(delay);
               }
               catch (InterruptedException e) {
                  //ignore
               }
            }
         }
         catch (Exception e) {
            error = e;
         }
      }

      public Exception getError() {
         return this.error;
      }
   }

   private class ReceiverThread extends Thread {

      private SimpleString queue;
      private int num;
      private long delay;
      private volatile Exception error = null;

      public ReceiverThread(SimpleString queue, int num, long delay) {
         this.queue = queue;
         this.num = num;
         this.delay = delay;
      }

      @Override
      public void run() {
         ClientConsumer consumer;
         try {
            consumer = session2.createConsumer(queue);

            for (int i = 0; i < num; i++) {
               ClientMessage message = consumer.receive(5000);
               message.acknowledge();
               session2.commit();
               try {
                  Thread.sleep(delay);
               }
               catch (InterruptedException e) {
                  //ignore
               }
            }
         }
         catch (Exception e) {
            error = e;
         }
      }

      public Exception getError() {
         return this.error;
      }
   }

   private class ManagementThread extends Thread {

      private QueueControl queueControl;
      private volatile boolean stop = false;
      private Exception error = null;

      public ManagementThread(QueueControl queueControl) {
         this.queueControl = queueControl;
      }

      @Override
      public void run() {
         try {
            while (!stop) {
               queueControl.countMessages(null);
               queueControl.listMessagesAsJSON(null);
               try {
                  Thread.sleep(1000);
               }
               catch (InterruptedException e) {
                  //ignore
               }
            }
         }
         catch (Exception e) {
            error = e;
         }
      }

      public Exception getError() {
         return error;
      }

      public void exit() {
         stop = true;
      }
   }
}
