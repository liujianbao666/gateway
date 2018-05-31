package example.webSocket.client;

import example.webSocket.server.Global;

public class TestThread {
    public static void main(String[] args) {
        new Thread(new Runnable() {
            @Override
            public void run() {
                Global.group.write("11111");
            }
        }).start();
    }
}
