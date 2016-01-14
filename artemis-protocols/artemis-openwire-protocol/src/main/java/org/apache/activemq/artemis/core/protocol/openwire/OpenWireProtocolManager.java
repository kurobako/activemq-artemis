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
package org.apache.activemq.artemis.core.protocol.openwire;

import javax.jms.InvalidClientIDException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;

import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.BaseInterceptor;
import org.apache.activemq.artemis.api.core.Interceptor;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClusterTopologyListener;
import org.apache.activemq.artemis.api.core.client.TopologyMember;
import org.apache.activemq.artemis.api.core.management.CoreNotificationType;
import org.apache.activemq.artemis.api.core.management.ManagementHelper;
import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.postoffice.Bindings;
import org.apache.activemq.artemis.core.postoffice.QueueBinding;
import org.apache.activemq.artemis.core.protocol.openwire.amq.AMQConnectionContext;
import org.apache.activemq.artemis.core.protocol.openwire.amq.AMQConsumer;
import org.apache.activemq.artemis.core.protocol.openwire.amq.AMQProducerBrokerExchange;
import org.apache.activemq.artemis.core.protocol.openwire.amq.AMQSession;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyServerConnection;
import org.apache.activemq.artemis.core.security.CheckType;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.cluster.ClusterConnection;
import org.apache.activemq.artemis.core.server.cluster.ClusterManager;
import org.apache.activemq.artemis.core.server.management.ManagementService;
import org.apache.activemq.artemis.core.server.management.Notification;
import org.apache.activemq.artemis.core.server.management.NotificationListener;
import org.apache.activemq.artemis.spi.core.protocol.ConnectionEntry;
import org.apache.activemq.artemis.spi.core.protocol.MessageConverter;
import org.apache.activemq.artemis.spi.core.protocol.ProtocolManager;
import org.apache.activemq.artemis.spi.core.protocol.ProtocolManagerFactory;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.spi.core.remoting.Acceptor;
import org.apache.activemq.artemis.spi.core.remoting.Connection;
import org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManager;
import org.apache.activemq.artemis.utils.DataConstants;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.BrokerId;
import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.command.Command;
import org.apache.activemq.command.ConnectionControl;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerControl;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.DestinationInfo;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.RemoveSubscriptionInfo;
import org.apache.activemq.command.SessionId;
import org.apache.activemq.command.SessionInfo;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.command.TransactionInfo;
import org.apache.activemq.command.WireFormatInfo;
import org.apache.activemq.command.XATransactionId;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.openwire.OpenWireFormatFactory;
import org.apache.activemq.state.ConnectionState;
import org.apache.activemq.state.ProducerState;
import org.apache.activemq.state.SessionState;
import org.apache.activemq.util.IdGenerator;
import org.apache.activemq.util.InetAddressUtil;
import org.apache.activemq.util.LongSequenceGenerator;

public class OpenWireProtocolManager implements ProtocolManager<Interceptor>, NotificationListener, ClusterTopologyListener {

   private static final IdGenerator BROKER_ID_GENERATOR = new IdGenerator();
   private static final IdGenerator ID_GENERATOR = new IdGenerator();

   private final LongSequenceGenerator messageIdGenerator = new LongSequenceGenerator();
   private final ActiveMQServer server;

   private final OpenWireProtocolManagerFactory factory;

   private OpenWireFormatFactory wireFactory;

   private boolean tightEncodingEnabled = true;

   private boolean prefixPacketSize = true;

   private BrokerId brokerId;
   protected final ProducerId advisoryProducerId = new ProducerId();

   // from broker
   protected final Map<ConnectionId, OpenWireConnection> brokerConnectionStates = Collections.synchronizedMap(new HashMap<ConnectionId, OpenWireConnection>());

   private final CopyOnWriteArrayList<OpenWireConnection> connections = new CopyOnWriteArrayList<>();

   protected final ConcurrentMap<ConnectionId, ConnectionInfo> connectionInfos = new ConcurrentHashMap<>();
   // Clebert TODO: use ConcurrentHashMap, or maybe use the schema that's already available on Artemis upstream (unique-client-id)
   private final Map<String, AMQConnectionContext> clientIdSet = new HashMap<String, AMQConnectionContext>();

   private String brokerName;

   // Clebert TODO: Artemis already stores the Session. Why do we need a different one here
   private Map<SessionId, AMQSession> sessions = new ConcurrentHashMap<>();

   // Clebert: Artemis already has a Resource Manager. Need to remove this..
   //          The TransactionID extends XATransactionID, so all we need is to convert the XID here
   private Map<TransactionId, AMQSession> transactions = new ConcurrentHashMap<>();

   // Clebert: Artemis session has meta-data support, perhaps we could reuse it here
   private Map<String, SessionId> sessionIdMap = new ConcurrentHashMap<>();

   private final Map<String, TopologyMember> topologyMap = new ConcurrentHashMap<>();

   private final LinkedList<TopologyMember> members = new LinkedList<>();

   private final ScheduledExecutorService scheduledPool;

   public OpenWireProtocolManager(OpenWireProtocolManagerFactory factory, ActiveMQServer server) {
      this.factory = factory;
      this.server = server;
      this.wireFactory = new OpenWireFormatFactory();
      // preferred prop, should be done via config
      wireFactory.setCacheEnabled(false);
      advisoryProducerId.setConnectionId(ID_GENERATOR.generateId());
      ManagementService service = server.getManagementService();
      scheduledPool = server.getScheduledPool();
      if (service != null) {
         service.addNotificationListener(this);
      }

      final ClusterManager clusterManager = this.server.getClusterManager();
      ClusterConnection cc = clusterManager.getDefaultConnection(null);
      if (cc != null) {
         cc.addClusterTopologyListener(this);
      }
   }

   @Override
   public void nodeUP(TopologyMember member, boolean last) {
      if (topologyMap.put(member.getNodeId(), member) == null) {
         updateClientClusterInfo();
      }
   }

   public void nodeDown(long eventUID, String nodeID) {
      if (topologyMap.remove(nodeID) != null) {
         updateClientClusterInfo();
      }
   }

   private void updateClientClusterInfo() {

      synchronized (members) {
         members.clear();
         members.addAll(topologyMap.values());
      }

      for (OpenWireConnection c : this.connections) {
         ConnectionControl control = newConnectionControl(c.isRebalance());
         c.updateClient(control);
      }
   }

   @Override
   public boolean acceptsNoHandshake() {
      return false;
   }

   @Override
   public ProtocolManagerFactory<Interceptor> getFactory() {
      return factory;
   }

   @Override
   public void updateInterceptors(List<BaseInterceptor> incomingInterceptors,
                                  List<BaseInterceptor> outgoingInterceptors) {
      // NO-OP
   }

   @Override
   public ConnectionEntry createConnectionEntry(Acceptor acceptorUsed, Connection connection) {
      OpenWireFormat wf = (OpenWireFormat) wireFactory.createWireFormat();
      OpenWireConnection owConn = new OpenWireConnection(acceptorUsed, connection, this, wf);
      owConn.init();

      // TODO CLEBERT What is this constant here? we should get it from TTL initial pings
      return new ConnectionEntry(owConn, null, System.currentTimeMillis(), 1 * 60 * 1000);
   }

   @Override
   public MessageConverter getConverter() {
      return new OpenWireMessageConverter();
   }

   @Override
   public void removeHandler(String name) {
      // TODO Auto-generated method stub
   }

   @Override
   public void handleBuffer(RemotingConnection connection, ActiveMQBuffer buffer) {
   }

   @Override
   public void addChannelHandlers(ChannelPipeline pipeline) {
      // each read will have a full packet with this
      pipeline.addLast("packet-decipher", new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, DataConstants.SIZE_INT));
   }

   @Override
   public boolean isProtocol(byte[] array) {
      if (array.length < 8) {
         throw new IllegalArgumentException("Protocol header length changed " + array.length);
      }

      int start = this.prefixPacketSize ? 4 : 0;
      int j = 0;
      // type
      if (array[start] != WireFormatInfo.DATA_STRUCTURE_TYPE) {
         return false;
      }
      start++;
      WireFormatInfo info = new WireFormatInfo();
      final byte[] magic = info.getMagic();
      int remainingLen = array.length - start;
      int useLen = remainingLen > magic.length ? magic.length : remainingLen;
      useLen += start;
      // magic
      for (int i = start; i < useLen; i++) {
         if (array[i] != magic[j]) {
            return false;
         }
         j++;
      }
      return true;
   }

   @Override
   public void handshake(NettyServerConnection connection, ActiveMQBuffer buffer) {
      // TODO Auto-generated method stub

   }

   public void sendReply(final OpenWireConnection connection, final Command command) {
      server.getStorageManager().afterCompleteOperations(new IOCallback() {
         @Override
         public void onError(final int errorCode, final String errorMessage) {
            ActiveMQServerLogger.LOGGER.errorProcessingIOCallback(errorCode, errorMessage);
         }

         @Override
         public void done() {
            send(connection, command);
         }
      });
   }

   public boolean send(final OpenWireConnection connection, final Command command) {
      if (ActiveMQServerLogger.LOGGER.isTraceEnabled()) {
         ActiveMQServerLogger.LOGGER.trace("sending " + command);
      }
      synchronized (connection) {
         if (connection.isDestroyed()) {
            return false;
         }

         try {
            connection.physicalSend(command);
         }
         catch (Exception e) {
            return false;
         }
         catch (Throwable t) {
            return false;
         }
         return true;
      }
   }

   public void addConnection(OpenWireConnection connection, ConnectionInfo info) throws Exception {
      String username = info.getUserName();
      String password = info.getPassword();

      if (!this.validateUser(username, password)) {
         throw new SecurityException("User name [" + username + "] or password is invalid.");
      }

      String clientId = info.getClientId();
      if (clientId == null) {
         throw new InvalidClientIDException("No clientID specified for connection request");
      }

      synchronized (clientIdSet) {
         AMQConnectionContext context;
         context = clientIdSet.get(clientId);
         if (context != null) {
            if (info.isFailoverReconnect()) {
               OpenWireConnection oldConnection = context.getConnection();
               oldConnection.disconnect(true);
               connections.remove(oldConnection);
               connection.reconnect(context, info);
            }
            else {
               throw new InvalidClientIDException("Broker: " + getBrokerName() + " - Client: " + clientId + " already connected from " + context.getConnection().getRemoteAddress());
            }
         }
         else {
            //new connection
            context = connection.initContext(info);
            clientIdSet.put(clientId, context);
         }

         connections.add(connection);

         ActiveMQTopic topic = AdvisorySupport.getConnectionAdvisoryTopic();
         // do not distribute passwords in advisory messages. usernames okay
         ConnectionInfo copy = info.copy();
         copy.setPassword("");
         fireAdvisory(context, topic, copy);

         // init the conn
         addSessions(context.getConnection(), context.getConnectionState().getSessionIds());
      }
   }

   private void fireAdvisory(AMQConnectionContext context, ActiveMQTopic topic, Command copy) throws Exception {
      this.fireAdvisory(context, topic, copy, null);
   }

   public BrokerId getBrokerId() {
      // TODO: Use the Storage ID here...
      if (brokerId == null) {
         brokerId = new BrokerId(BROKER_ID_GENERATOR.generateId());
      }
      return brokerId;
   }

   /*
    * See AdvisoryBroker.fireAdvisory()
    */
   private void fireAdvisory(AMQConnectionContext context,
                             ActiveMQTopic topic,
                             Command command,
                             ConsumerId targetConsumerId) throws Exception {
      ActiveMQMessage advisoryMessage = new ActiveMQMessage();
      advisoryMessage.setStringProperty(AdvisorySupport.MSG_PROPERTY_ORIGIN_BROKER_NAME, getBrokerName());
      String id = getBrokerId() != null ? getBrokerId().getValue() : "NOT_SET";
      advisoryMessage.setStringProperty(AdvisorySupport.MSG_PROPERTY_ORIGIN_BROKER_ID, id);

      String url = context.getConnection().getLocalAddress();

      advisoryMessage.setStringProperty(AdvisorySupport.MSG_PROPERTY_ORIGIN_BROKER_URL, url);

      // set the data structure
      advisoryMessage.setDataStructure(command);
      advisoryMessage.setPersistent(false);
      advisoryMessage.setType(AdvisorySupport.ADIVSORY_MESSAGE_TYPE);
      advisoryMessage.setMessageId(new MessageId(advisoryProducerId, messageIdGenerator.getNextSequenceId()));
      advisoryMessage.setTargetConsumerId(targetConsumerId);
      advisoryMessage.setDestination(topic);
      advisoryMessage.setResponseRequired(false);
      advisoryMessage.setProducerId(advisoryProducerId);
      boolean originalFlowControl = context.isProducerFlowControl();
      final AMQProducerBrokerExchange producerExchange = new AMQProducerBrokerExchange();
      producerExchange.setConnectionContext(context);
      producerExchange.setMutable(true);
      producerExchange.setProducerState(new ProducerState(new ProducerInfo()));
      try {
         context.setProducerFlowControl(false);
         AMQSession sess = context.getConnection().getAdvisorySession();
         if (sess != null) {
            sess.send(producerExchange, advisoryMessage, false);
         }
      }
      finally {
         context.setProducerFlowControl(originalFlowControl);
      }
   }

   public String getBrokerName() {
      if (brokerName == null) {
         try {
            brokerName = InetAddressUtil.getLocalHostName().toLowerCase(Locale.ENGLISH);
         }
         catch (Exception e) {
            brokerName = "localhost";
         }
      }
      return brokerName;
   }

   protected ConnectionControl newConnectionControl(boolean rebalance) {
      ConnectionControl control = new ConnectionControl();

      String uri = generateMembersURI(rebalance);
      control.setConnectedBrokers(uri);

      control.setRebalanceConnection(rebalance);
      return control;
   }

   private String generateMembersURI(boolean flip) {
      String uri;
      StringBuffer connectedBrokers = new StringBuffer();
      String separator = "";

      synchronized (members) {
         if (members.size() > 0) {
            for (TopologyMember member : members) {
               connectedBrokers.append(separator).append(member.toURI());
               separator = ",";
            }

            // The flip exists to guarantee even distribution of URIs when sent to the client
            // in case of failures you won't get all the connections failing to a single server.
            if (flip && members.size() > 1) {
               members.addLast(members.removeFirst());
            }
         }
      }

      uri = connectedBrokers.toString();
      return uri;
   }

   public boolean isFaultTolerantConfiguration() {
      return false;
   }

   public void postProcessDispatch(MessageDispatch md) {
      // TODO Auto-generated method stub

   }

   public boolean isStopped() {
      // TODO Auto-generated method stub
      return false;
   }

   public void preProcessDispatch(MessageDispatch messageDispatch) {
      // TODO Auto-generated method stub

   }

   public boolean isStopping() {
      return false;
   }

   public void addProducer(OpenWireConnection theConn, ProducerInfo info) throws Exception {
      SessionId sessionId = info.getProducerId().getParentId();
      ConnectionId connectionId = sessionId.getParentId();
      ConnectionState cs = theConn.getState();
      if (cs == null) {
         throw new IllegalStateException("Cannot add a producer to a connection that had not been registered: " + connectionId);
      }
      SessionState ss = cs.getSessionState(sessionId);
      if (ss == null) {
         throw new IllegalStateException("Cannot add a producer to a session that had not been registered: " + sessionId);
      }
      // Avoid replaying dup commands
      if (!ss.getProducerIds().contains(info.getProducerId())) {

         AMQSession amqSession = sessions.get(sessionId);
         if (amqSession == null) {
            throw new IllegalStateException("Session not exist! : " + sessionId);
         }

         ActiveMQDestination destination = info.getDestination();
         if (destination != null && !AdvisorySupport.isAdvisoryTopic(destination)) {
            if (theConn.getProducerCount() >= theConn.getMaximumProducersAllowedPerConnection()) {
               throw new IllegalStateException("Can't add producer on connection " + connectionId + ": at maximum limit: " + theConn.getMaximumProducersAllowedPerConnection());
            }
            if (destination.isQueue()) {
               OpenWireUtil.validateDestination(destination, amqSession);
            }
            DestinationInfo destInfo = new DestinationInfo(theConn.getContext().getConnectionId(), DestinationInfo.ADD_OPERATION_TYPE, destination);
            this.addDestination(theConn, destInfo);
         }

         amqSession.createProducer(info);

         try {
            ss.addProducer(info);
         }
         catch (IllegalStateException e) {
            amqSession.removeProducer(info);
         }

      }

   }

   public void updateConsumer(OpenWireConnection theConn, ConsumerControl consumerControl) {
      SessionId sessionId = consumerControl.getConsumerId().getParentId();
      AMQSession amqSession = sessions.get(sessionId);
      amqSession.updateConsumerPrefetchSize(consumerControl.getConsumerId(), consumerControl.getPrefetch());
   }

   public void addConsumer(OpenWireConnection theConn, ConsumerInfo info) throws Exception {
      // Todo: add a destination interceptors holder here (amq supports this)
      SessionId sessionId = info.getConsumerId().getParentId();
      ConnectionId connectionId = sessionId.getParentId();
      ConnectionState cs = theConn.getState();
      if (cs == null) {
         throw new IllegalStateException("Cannot add a consumer to a connection that had not been registered: " + connectionId);
      }
      SessionState ss = cs.getSessionState(sessionId);
      if (ss == null) {
         throw new IllegalStateException(this.server + " Cannot add a consumer to a session that had not been registered: " + sessionId);
      }
      // Avoid replaying dup commands
      if (!ss.getConsumerIds().contains(info.getConsumerId())) {
         ActiveMQDestination destination = info.getDestination();
         if (destination != null && !AdvisorySupport.isAdvisoryTopic(destination)) {
            if (theConn.getConsumerCount() >= theConn.getMaximumConsumersAllowedPerConnection()) {
               throw new IllegalStateException("Can't add consumer on connection " + connectionId + ": at maximum limit: " + theConn.getMaximumConsumersAllowedPerConnection());
            }
         }

         AMQSession amqSession = sessions.get(sessionId);
         if (amqSession == null) {
            throw new IllegalStateException("Session not exist! : " + sessionId);
         }

         amqSession.createConsumer(info, amqSession);

         ss.addConsumer(info);
      }
   }

   public void addSessions(OpenWireConnection theConn, Set<SessionId> sessionSet) {
      Iterator<SessionId> iter = sessionSet.iterator();
      while (iter.hasNext()) {
         SessionId sid = iter.next();
         addSession(theConn, theConn.getState().getSessionState(sid).getInfo(), true);
      }
   }

   public AMQSession addSession(OpenWireConnection theConn, SessionInfo ss) {
      return addSession(theConn, ss, false);
   }

   public AMQSession addSession(OpenWireConnection theConn, SessionInfo ss, boolean internal) {
      AMQSession amqSession = new AMQSession(theConn.getState().getInfo(), ss, server, theConn, scheduledPool, this);
      amqSession.initialize();
      amqSession.setInternal(internal);
      sessions.put(ss.getSessionId(), amqSession);
      sessionIdMap.put(amqSession.getCoreSession().getName(), ss.getSessionId());
      return amqSession;
   }

   public void removeConnection(OpenWireConnection connection,
                                ConnectionInfo info,
                                Throwable error) throws InvalidClientIDException {
      synchronized (clientIdSet) {
         String clientId = info.getClientId();
         if (clientId != null) {
            AMQConnectionContext context = this.clientIdSet.get(clientId);
            if (context != null && context.decRefCount() == 0) {
               //connection is still there and need to close
               this.clientIdSet.remove(clientId);
               connection.disconnect(error != null);
               this.connections.remove(connection);//what's that for?
            }
         }
         else {
            throw new InvalidClientIDException("No clientID specified for connection disconnect request");
         }
      }
   }

   public void removeSession(AMQConnectionContext context, SessionInfo info) throws Exception {
      AMQSession session = sessions.remove(info.getSessionId());
      if (session != null) {
         session.close();
      }
   }

   public void removeProducer(ProducerId id) {
      SessionId sessionId = id.getParentId();
      AMQSession session = sessions.get(sessionId);
      session.removeProducer(id);
   }

   public AMQSession getSession(SessionId sessionId) {
      return sessions.get(sessionId);
   }

   public void removeDestination(OpenWireConnection connection, ActiveMQDestination dest) throws Exception {
      if (dest.isQueue()) {
         SimpleString qName = new SimpleString("jms.queue." + dest.getPhysicalName());
         this.server.destroyQueue(qName);
      }
      else {
         Bindings bindings = this.server.getPostOffice().getBindingsForAddress(SimpleString.toSimpleString("jms.topic." + dest.getPhysicalName()));
         Iterator<Binding> iterator = bindings.getBindings().iterator();

         while (iterator.hasNext()) {
            Queue b = (Queue) iterator.next().getBindable();
            if (b.getConsumerCount() > 0) {
               throw new Exception("Destination still has an active subscription: " + dest.getPhysicalName());
            }
            if (b.isDurable()) {
               throw new Exception("Destination still has durable subscription: " + dest.getPhysicalName());
            }
            b.deleteQueue();
         }
      }

      if (!AdvisorySupport.isAdvisoryTopic(dest)) {
         AMQConnectionContext context = connection.getContext();
         DestinationInfo advInfo = new DestinationInfo(context.getConnectionId(), DestinationInfo.REMOVE_OPERATION_TYPE, dest);

         ActiveMQTopic topic = AdvisorySupport.getDestinationAdvisoryTopic(dest);
         fireAdvisory(context, topic, advInfo);
      }
   }

   public void addDestination(OpenWireConnection connection, DestinationInfo info) throws Exception {
      ActiveMQDestination dest = info.getDestination();
      if (dest.isQueue()) {
         SimpleString qName = OpenWireUtil.toCoreAddress(dest);
         QueueBinding binding = (QueueBinding) server.getPostOffice().getBinding(qName);
         if (binding == null) {
            if (connection.getState().getInfo() != null) {

               CheckType checkType = dest.isTemporary() ? CheckType.CREATE_NON_DURABLE_QUEUE : CheckType.CREATE_DURABLE_QUEUE;
               server.getSecurityStore().check(qName, checkType, connection);

               server.checkQueueCreationLimit(connection.getUsername());
            }
            ConnectionInfo connInfo = connection.getState().getInfo();
            this.server.createQueue(qName, qName, null, connInfo == null ? null : SimpleString.toSimpleString(connInfo.getUserName()), false, dest.isTemporary());
         }
         if (dest.isTemporary()) {
            connection.registerTempQueue(dest);
         }
      }

      if (!AdvisorySupport.isAdvisoryTopic(dest)) {
         AMQConnectionContext context = connection.getContext();
         DestinationInfo advInfo = new DestinationInfo(context.getConnectionId(), DestinationInfo.ADD_OPERATION_TYPE, dest);

         ActiveMQTopic topic = AdvisorySupport.getDestinationAdvisoryTopic(dest);
         fireAdvisory(context, topic, advInfo);
      }
   }

   public void endTransaction(TransactionInfo info) throws Exception {
      AMQSession txSession = transactions.get(info.getTransactionId());

      if (txSession != null) {
         txSession.endTransaction(info);
      }
   }

   public void commitTransactionOnePhase(TransactionInfo info) throws Exception {
      AMQSession txSession = transactions.get(info.getTransactionId());

      if (txSession != null) {
         txSession.commitOnePhase(info);
      }
      transactions.remove(info.getTransactionId());
   }

   public void prepareTransaction(TransactionInfo info) throws Exception {
      XATransactionId xid = (XATransactionId) info.getTransactionId();
      AMQSession txSession = transactions.get(xid);
      if (txSession != null) {
         txSession.prepareTransaction(xid);
      }
   }

   public void commitTransactionTwoPhase(TransactionInfo info) throws Exception {
      XATransactionId xid = (XATransactionId) info.getTransactionId();
      AMQSession txSession = transactions.get(xid);
      if (txSession != null) {
         txSession.commitTwoPhase(xid);
      }
      transactions.remove(xid);
   }

   public void rollbackTransaction(TransactionInfo info) throws Exception {
      AMQSession txSession = transactions.get(info.getTransactionId());
      if (txSession != null) {
         txSession.rollback(info);
      }
      transactions.remove(info.getTransactionId());
   }

   public TransactionId[] recoverTransactions(Set<SessionId> sIds) {
      List<TransactionId> recovered = new ArrayList<>();
      if (sIds != null) {
         for (SessionId sid : sIds) {
            AMQSession s = this.sessions.get(sid);
            if (s != null) {
               s.recover(recovered);
            }
         }
      }
      return recovered.toArray(new TransactionId[0]);
   }

   public boolean validateUser(String login, String passcode) {
      boolean validated = true;

      ActiveMQSecurityManager sm = server.getSecurityManager();

      if (sm != null && server.getConfiguration().isSecurityEnabled()) {
         validated = sm.validateUser(login, passcode);
      }

      return validated;
   }

   public void forgetTransaction(TransactionId xid) throws Exception {
      AMQSession txSession = transactions.get(xid);
      if (txSession != null) {
         txSession.forget(xid);
      }
      transactions.remove(xid);
   }

   public void registerTx(TransactionId txId, AMQSession amqSession) {
      transactions.put(txId, amqSession);
   }

   //advisory support
   @Override
   public void onNotification(Notification notif) {
      try {
         if (notif.getType() instanceof CoreNotificationType) {
            CoreNotificationType type = (CoreNotificationType) notif.getType();
            switch (type) {
               case CONSUMER_SLOW:
                  fireSlowConsumer(notif);
                  break;
               default:
                  break;
            }
         }
      }
      catch (Exception e) {
         ActiveMQServerLogger.LOGGER.error("Failed to send notification " + notif, e);
      }
   }

   private void fireSlowConsumer(Notification notif) throws Exception {
      SimpleString coreSessionId = notif.getProperties().getSimpleStringProperty(ManagementHelper.HDR_SESSION_NAME);
      Long coreConsumerId = notif.getProperties().getLongProperty(ManagementHelper.HDR_CONSUMER_NAME);
      SessionId sessionId = sessionIdMap.get(coreSessionId.toString());
      AMQSession session = sessions.get(sessionId);
      AMQConsumer consumer = session.getConsumer(coreConsumerId);
      ActiveMQDestination destination = consumer.getDestination();

      if (!AdvisorySupport.isAdvisoryTopic(destination)) {
         ActiveMQTopic topic = AdvisorySupport.getSlowConsumerAdvisoryTopic(destination);
         ConnectionId connId = sessionId.getParentId();
         OpenWireConnection cc = this.brokerConnectionStates.get(connId);
         ActiveMQMessage advisoryMessage = new ActiveMQMessage();
         advisoryMessage.setStringProperty(AdvisorySupport.MSG_PROPERTY_CONSUMER_ID, consumer.getId().toString());

         fireAdvisory(cc.getContext(), topic, advisoryMessage, consumer.getId());
      }
   }

   public void removeSubscription(RemoveSubscriptionInfo subInfo) throws Exception {
      SimpleString subQueueName = new SimpleString(org.apache.activemq.artemis.jms.client.ActiveMQDestination.createQueueNameForDurableSubscription(true, subInfo.getClientId(), subInfo.getSubscriptionName()));
      server.destroyQueue(subQueueName);
   }

   public void sendBrokerInfo(OpenWireConnection connection) {
      BrokerInfo brokerInfo = new BrokerInfo();
      brokerInfo.setBrokerName(server.getIdentity());
      brokerInfo.setBrokerId(new BrokerId(server.getNodeID().toString()));
      brokerInfo.setPeerBrokerInfos(null);
      brokerInfo.setFaultTolerantConfiguration(false);
      brokerInfo.setBrokerURL(connection.getLocalAddress());

      //cluster support yet to support
      brokerInfo.setPeerBrokerInfos(null);
      connection.dispatchAsync(brokerInfo);
   }
}
