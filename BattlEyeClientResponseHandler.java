package com.allianceapps.BattlEyeClient;

public interface BattlEyeClientResponseHandler
{
	void onConnected();
    void onDisconnected(DisconnectType disconnectType);
    
    void onCommandResponseReceived(String commandResponse, int id);
    
    void onMessageReceived(String message);
    
    void onWrittenToLog(String message);
}
