package com.allianceapps.BattlEyeClient;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.DatagramChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32;

public class BattlEyeClient
{
	private static final int MONITOR_INTERVAL = 1000;
	private static final int TIMEOUT_DELAY = 6000;
	private static final int KEEP_ALIVE_DELAY = 30000;
	private static final int RECONNECT_DELAY = 2000;
	private static final int TIMEOUT_TRIES = 3;

	private final CRC32 CRC = new CRC32();

	private InetSocketAddress host;
	private DatagramChannel datagramChannel;
	private ByteBuffer sendBuffer;
	private ByteBuffer receiveBuffer;

	private AtomicBoolean connected;
	private AtomicBoolean autoReconnect;

	private String password;
	private int sequenceNumber;
	
	private AtomicLong lastSent;
	private AtomicLong lastReceived;
	private AtomicLong lastSentTimeoutFirst;
	private AtomicInteger lastSentTimeoutCounter;

	private Thread receiveDataThread;
	private Thread monitorThread;

	private final Queue<Command> commandQueue;
	private AtomicBoolean emptyCommandQueueOnConnect;

	private final List<BattlEyeClientResponseHandler> battlEyeClientResponseHandlerList;

	public BattlEyeClient()
	{
		this.connected = new AtomicBoolean(false);
		this.autoReconnect = new AtomicBoolean(true);

		this.commandQueue = new ConcurrentLinkedQueue<>();
		this.emptyCommandQueueOnConnect = new AtomicBoolean(true);

		this.battlEyeClientResponseHandlerList = new ArrayList<>();
	}
	
	public void connect(String host, int port, String password) throws IOException
	{
		if(this.isConnected()) return;
		
		this.host = new InetSocketAddress(host, port);
		this.connect(password);
	}
	
	public void connect(InetSocketAddress host, String password) throws IOException
	{
		if(this.isConnected()) return;
		
		this.host = host;
		this.connect(password);
	}

	public void connect(String password) throws IOException
	{
		this.fireWrittenToLogHandler("Connecting to " + host);
		
		if(isConnected()) return;
		this.password = password;

		datagramChannel = DatagramChannel.open();

		sendBuffer = ByteBuffer.allocate(datagramChannel.getOption(StandardSocketOptions.SO_SNDBUF));
		sendBuffer.order(ByteOrder.LITTLE_ENDIAN); // ArmA 2 server uses little endian

		receiveBuffer = ByteBuffer.allocate(datagramChannel.getOption(StandardSocketOptions.SO_RCVBUF));
		receiveBuffer.order(sendBuffer.order());

		sequenceNumber = -1;
		long time = System.currentTimeMillis();
		
		lastSent = new AtomicLong(time);
		lastReceived = new AtomicLong(time);
		lastSentTimeoutCounter = new AtomicInteger(0);
		lastSentTimeoutFirst = new AtomicLong(time);

		if(emptyCommandQueueOnConnect.get()) commandQueue.clear();
		datagramChannel.connect(host);

		startReceivingData();
		startMonitorThread();

		createPacket(BattlEyePacketType.Login, -1, password, false);
		sendPacket();
	}

	public void reconnect() throws IOException
	{
		this.disconnect();
		this.connect(password);
	}

	public boolean isConnected()
	{
		return ((datagramChannel != null) && datagramChannel.isConnected() && connected.get());
	}

	public void disconnect() throws IOException
	{
		if(isConnected())
		{
			this.doDisconnect(DisconnectType.Manual);
		}
	}
	
	private void doDisconnect(DisconnectType disconnectType) throws IOException
	{
		this.fireWrittenToLogHandler("Disconnecting from " + host);
		this.connected.set(false);
		
		if(this.monitorThread != null)
		{
			this.monitorThread.interrupt();
			this.monitorThread = null;
		}
		
		if(this.receiveDataThread != null)
		{
			this.receiveDataThread.interrupt();
			this.receiveDataThread = null;
		}
		
		if(this.datagramChannel != null)
		{
			this.datagramChannel.disconnect();
			this.datagramChannel.close();
			this.datagramChannel = null;
		}
		
		this.sendBuffer = null;
		this.receiveBuffer = null;
		
		// fire ConnectionHandler.onDisconnected
		this.fireConnectionDisconnectedHandler(disconnectType);
		
		if(disconnectType == DisconnectType.ConnectionLost && this.autoReconnect.get())
		{
			// wait before reconnect
			new Thread(() -> {
				try {
					Thread.sleep(RECONNECT_DELAY);
					this.connect(password);
				} catch (InterruptedException e) {
					fireWrittenToLogHandler("Auto reconnect thread interrupted.");
				} catch (IOException e) {
					fireWrittenToLogHandler("Error while trying to reconnect.");
				}
			}).start();
		}
	}

	public boolean isAutoReconnect()
	{
		return this.autoReconnect.get();
	}

	public void setAutoReconnect(boolean autoReconnect)
	{
		this.autoReconnect.set(autoReconnect);
	}

	public int sendCommand(String command) throws IOException
	{
		this.fireWrittenToLogHandler("SendCommand: " + command);
		
		if(!isConnected()) return -1;
		Command cmd = new Command(command);
		
		if(commandQueue.offer(cmd))
		{
			cmd.id = getNextSequenceNumber();
		} else {
			this.fireWrittenToLogHandler("Command queue is full.");
			return -2;
		}
		
		if(commandQueue.size() == 1)
		{
			// enqueue and send this command immediately
			createPacket(BattlEyePacketType.Command, cmd.id, command, true);
			sendPacket();
		} else {
			// only enqueue this command
			this.fireWrittenToLogHandler("Command enqueued: " + cmd);
		}
		
		return cmd.id;
	}

	public int sendCommand(BattlEyeCommand command, String... params) throws IOException
	{
		StringBuilder commandBuilder = new StringBuilder(command.getCommandString());
		
		for(String param : params)
		{
			commandBuilder.append(' ');
			commandBuilder.append(param);
		}
		
		return sendCommand(commandBuilder.toString());
	}

	private void sendNextCommand(int id) throws IOException
	{
		Command command = commandQueue.poll();
		
		if(command == null)
		{
			this.fireWrittenToLogHandler("Command queue empty.");
			return;
		}
		
		if(command.id != id) this.fireWrittenToLogHandler("Invalid command id.");
		
		if(!commandQueue.isEmpty())
		{
			command = commandQueue.peek();
			this.fireWrittenToLogHandler("Send enqueued command: " + command);
			createPacket(BattlEyePacketType.Command, command.id, command.command, true);
			sendPacket();
		}
	}

	public boolean isEmptyCommandQueueOnConnect()
	{
		return emptyCommandQueueOnConnect.get();
	}

	public void setEmptyCommandQueueOnConnect(boolean b)
	{
		emptyCommandQueueOnConnect.set(b);
	}

	
	//HANDLERS BEGIN
	public List<BattlEyeClientResponseHandler> getAllBattleEyeClientResponseHandlers()
	{
		return this.battlEyeClientResponseHandlerList;
	}

	public boolean addBattleEyeClientResponseHandler(BattlEyeClientResponseHandler handler)
	{
		return this.battlEyeClientResponseHandlerList.add(handler);
	}

	public boolean removeBattleEyeClientResponseHandler(BattlEyeClientResponseHandler handler)
	{
		return this.battlEyeClientResponseHandlerList.remove(handler);
	}

	public void removeAllBattleEyeClientResponseHandlers()
	{
		this.battlEyeClientResponseHandlerList.clear();
	}

	private void fireCommandResponseHandler(String commandResponse, int id)
	{
		for (BattlEyeClientResponseHandler commandResponseHandler : battlEyeClientResponseHandlerList)
			commandResponseHandler.onCommandResponseReceived(commandResponse, id);
	}

	private void fireMessageHandler(String message)
	{
		if (message == null || message.isEmpty()) return;
		
		for (BattlEyeClientResponseHandler messageHandler : battlEyeClientResponseHandlerList)
			messageHandler.onMessageReceived(message);
	}
	
	private void fireConnectionConnectedHandler()
	{
		for (BattlEyeClientResponseHandler connectionConnectedHandler : battlEyeClientResponseHandlerList)
			connectionConnectedHandler.onConnected();
	}
	
	private void fireConnectionDisconnectedHandler(DisconnectType disTyp)
	{
		for (BattlEyeClientResponseHandler connectionDisconnectedHandler : battlEyeClientResponseHandlerList)
			connectionDisconnectedHandler.onDisconnected(disTyp);
	}
	
	private void fireWrittenToLogHandler(String message)
	{
		for (BattlEyeClientResponseHandler writtenToLogHandler : battlEyeClientResponseHandlerList)
			writtenToLogHandler.onWrittenToLog(message);
	}
	//HANDLERS END
	
	private void startReceivingData()
	{
		receiveDataThread = new Thread()
		{
			public void run()
			{
				fireWrittenToLogHandler("Start receive data thread.");
				String[] multiPacketCache = null; // use separate cache for every sequence number possible overlap
				int multiPacketCounter = 0;
				
				try
				{
					while (!isInterrupted())
					{
						if(!readPacket() || receiveBuffer.remaining() < 2)
						{
							fireWrittenToLogHandler("Invalid data received.");
							continue;
						}
						
						if(this != receiveDataThread)
						{
							fireWrittenToLogHandler("Instance thread changed (receive data thread).");
							break; // exit thread
						}
						
						byte packetType = receiveBuffer.get();
						switch (packetType)
						{
							case 0x00:
							{
								// login response
								// 0x00 | (0x01 (successfully logged in) OR 0x00
								// (failed))
									
								if(receiveBuffer.remaining() != 1)
								{
									fireWrittenToLogHandler("Unexpected login response received.");
									doDisconnect(DisconnectType.ConnectionFailed);
									return; // exit thread
								}
								
								connected.set(receiveBuffer.get() == 0x01);
								
								if(connected.get())
								{
									fireWrittenToLogHandler("Connected to " + host);
									fireConnectionConnectedHandler();
								} else {
									fireWrittenToLogHandler("Connection failed to " + host);
									doDisconnect(DisconnectType.ConnectionFailed);
									return; // exit thread
								}
								
								break;
							}
						
							case 0x01:
							{
								// command response
								// 0x01 | received 1-byte sequence number |
								// (possible header and/or response (ASCII string
								// without null-terminator) OR nothing)
								byte sn = receiveBuffer.get();
	
								if(receiveBuffer.hasRemaining())
								{
									if(receiveBuffer.get() == 0x00)
									{
										// multi packet response
										fireWrittenToLogHandler("Multi packet command response received: " + sn);
										
										// 0x00 | number of packets for this
										// response | 0-based index of the current packet
										
										byte packetCount = receiveBuffer.get();
										byte packetIndex = receiveBuffer.get();
										
										fireWrittenToLogHandler("PacketIndex/PacketCount: " + ((int) packetIndex) + "/" + ((int) packetCount));
										
										if(multiPacketCounter == 0) multiPacketCache = new String[packetCount];
										
										multiPacketCache[packetIndex] = new String(receiveBuffer.array(), receiveBuffer.position(), receiveBuffer.remaining());
										if(++multiPacketCounter == packetCount)
										{
											// last packet received
											// merge packet data
											StringBuilder sb = new StringBuilder(1024 * packetCount); // estimated size												
											for(String commandResponsePart : multiPacketCache) sb.append(commandResponsePart);
											
											multiPacketCache = null;
											multiPacketCounter = 0;
											
											fireCommandResponseHandler(sb.toString(), sn);
											sendNextCommand(sn);
										}
									} else {
										// single packet response
										fireWrittenToLogHandler("Single packet command response received: " + sn);
										
										// position -1 and remaining +1 because the call to receiveBuffer.get() increments the position!
										String commandResponse = new String(receiveBuffer.array(), receiveBuffer.position() - 1, receiveBuffer.remaining() + 1);
										fireCommandResponseHandler(commandResponse, sn);
										sendNextCommand(sn);
									}
								} else {
									fireWrittenToLogHandler("Empty command response received: " + sn);
									sendNextCommand(sn);
								}
								break;
							}
						
							case 0x02:
							{
								// server message
								// 0x02 | 1-byte sequence number (starting at 0) |
								// server message (ASCII string without
								// null-terminator)
								byte sn = receiveBuffer.get();
	
								fireWrittenToLogHandler("Server message received: " + sn);
								String message = new String(receiveBuffer.array(), receiveBuffer.position(), receiveBuffer.remaining());
								
								createPacket(BattlEyePacketType.Acknowledge, sn, null, true);
								sendPacket();
								
								fireMessageHandler(message);
								sendNextCommand(sn);
								
								break;
							}
						
							default:
							{
								// should not happen!
								fireWrittenToLogHandler("Invalid packet type received: " + packetType);
								break;
							}
						}
					}
				} catch (IOException e) {
					if(e instanceof ClosedByInterruptException) fireWrittenToLogHandler("Receive data thread interrupted.");
					else fireWrittenToLogHandler("Unhandled exception while receiving data. Error: " + e);
				}

				//EVTL EINEN HANDLER WENN ER ABKACKT ??
				fireWrittenToLogHandler("Exit receive data thread.");
			}
		};
		
		receiveDataThread.start();
	}

	private void startMonitorThread()
	{
		monitorThread = new Thread()
		{
			public void run()
			{
				fireWrittenToLogHandler("Start monitor thread.");
				
					while(!isInterrupted())
					{
						try {
							Thread.sleep(MONITOR_INTERVAL);
						} catch (Exception e) {
							/* Nothing to do here */
						}
												
						if(this != monitorThread)
						{
							fireWrittenToLogHandler("Instance thread changed (monitor thread).");
							break;
						}

						if(!connected.get() && ((lastSent.get() + TIMEOUT_DELAY) < System.currentTimeMillis()))
						{
							fireWrittenToLogHandler("A timeout occured so the connection to server is lost.");

							try {
								doDisconnect(DisconnectType.ConnectionFailed);
							} catch(Exception e) {
								fireWrittenToLogHandler("An Error occured while disconecting.");
							}

							break;
						}
						
						//Check Timeout
						if(((lastSent.get() - lastReceived.get()) > TIMEOUT_DELAY) && ((lastSentTimeoutCounter.get() == 0) || ((lastSentTimeoutCounter.get() > 0) && ((lastSentTimeoutFirst.get() + TIMEOUT_DELAY) < System.currentTimeMillis()))))
						{
							fireWrittenToLogHandler("A timeout occured so the connection to server is lost.");

							try {
								doDisconnect(DisconnectType.ConnectionLost);
							} catch(Exception e) {
								fireWrittenToLogHandler("An Error occured while disconecting.");
							}
							
							break;
						}
						
						//Keep Alive Pack
						if(((System.currentTimeMillis() - lastSent.get()) > KEEP_ALIVE_DELAY) || ((lastSentTimeoutCounter.get() > 0 && lastSentTimeoutCounter.get() < TIMEOUT_TRIES) && (System.currentTimeMillis() - lastSent.get()) >= (TIMEOUT_DELAY / TIMEOUT_TRIES)))
						{
							fireWrittenToLogHandler("Send keep alive packet.");
							createPacket(BattlEyePacketType.Command, getNextSequenceNumber(), null, true);
							lastSentTimeoutCounter.set(lastSentTimeoutCounter.get() + 1);
							
							try {
								sendPacket();
							} catch(Exception e) {
								fireWrittenToLogHandler("An Error occured while trying to send packets to the server (" + lastSentTimeoutCounter.get() + "/" + TIMEOUT_TRIES + ").");
							}
						}
					}
				
				fireWrittenToLogHandler("Exit monitor thread.");
			}
		};
		
		monitorThread.start();
	}

	private int getNextSequenceNumber()
	{
		sequenceNumber = sequenceNumber == 255 ? 0 : sequenceNumber + 1;
		return sequenceNumber;
	}

	// 'B'(0x42) | 'E'(0x45) | 4-byte CRC32 checksum of the subsequent bytes |
	// 0xFF
	private void createPacket(BattlEyePacketType type, int sequenceNumber, String command, boolean printSequenceNumber)
	{
		sendBuffer.clear();
		sendBuffer.put((byte) 'B');
		sendBuffer.put((byte) 'E');
		sendBuffer.position(6); // skip checksum
		sendBuffer.put((byte) 0xFF);
		sendBuffer.put(type.getType());

		if(printSequenceNumber) sendBuffer.put((byte) sequenceNumber);

		if(command != null && !command.isEmpty())
		{
			byte[] payload = command.getBytes();
			sendBuffer.put(payload);
		}

		CRC.reset();
		CRC.update(sendBuffer.array(), 6, sendBuffer.position() - 6);
		int checksum = (int) CRC.getValue();
		sendBuffer.putInt(2, checksum);

		sendBuffer.flip();
	}

	private void sendPacket() throws IOException
	{
		//TODO: Changed order of setting Varibales and sending Command so
		//TODO: Timeouts should now be detected better by the System
		lastSent.set(System.currentTimeMillis());
		if(lastSentTimeoutCounter.get() == 1) lastSentTimeoutFirst.set(System.currentTimeMillis());
		
		int write = datagramChannel.write(sendBuffer);
		
		fireWrittenToLogHandler(write + " bytes written to the channel.");
	}

	private boolean readPacket() throws IOException
	{
		receiveBuffer.clear();
		int read = datagramChannel.read(receiveBuffer);
		fireWrittenToLogHandler(read + " bytes read from the channel.");
		
		if(read < 7)
		{
			fireWrittenToLogHandler("Invalid header size.");
			return false;
		}
		
		receiveBuffer.flip();
		if(receiveBuffer.get() != (byte) 'B' || receiveBuffer.get() != (byte) 'E')
		{
			fireWrittenToLogHandler("Invalid header.");
			return false;
		}
		
		@SuppressWarnings("unused")
		int checksum = receiveBuffer.getInt();
		
		if (receiveBuffer.get() != (byte) 0xFF)
		{
			fireWrittenToLogHandler("Invalid header.");
			return false;
		}
		
		lastReceived.set(System.currentTimeMillis());
		lastSentTimeoutCounter.set(0);
		
		return true;
	}

	private static class Command
	{
		private final String command;
		private int id = -1;

		Command(String command)
		{
			this.command = command;
		}

		public String toString()
		{
			return "Command{" + "command='" + command + '\'' + ", id=" + id + '}';
		}
	}
}
