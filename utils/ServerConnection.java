package com.ayscom.minetur.utils;

/**
 * Created by Christian on 11/8/15.
 */
public class ServerConnection {
    private String _ip;
    private int _port;

    public ServerConnection(String ip, int port) {
        _ip = ip;
        _port = port;
    }

    public String getIP() {
        return _ip;
    }

    public void setIP(String ip) {
        _ip = ip;
    }

    public int getPort() {
        return _port;
    }

    public void setPort(int port) {
        _port = port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ServerConnection that = (ServerConnection) o;

        if (_port != that._port) return false;
        return _ip.equals(that._ip);

    }

    @Override
    public int hashCode() {
        int result = _ip.hashCode();
        result = 31 * result + _port;
        return result;
    }
}
