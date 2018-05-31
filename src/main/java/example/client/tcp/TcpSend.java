package example.client.tcp;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

public class TcpSend {
	public static void main(String[] args) {
		String info = "102C00044D5154540402003C00203232323938646131633666383439333961613863383436646630303566346138";
		byte[] sendMessage = sendMessage("127.0.0.1",1883,strToHex(info));
		String a = "";
		for (byte b : sendMessage) {
			
		}
	}
	public static byte[] sendMessage(String url, int port, byte[] request) {
		byte[] res = null;
		Socket socket = null;
		InputStream is = null;
		OutputStream os = null;
		try {
			socket = new Socket(url, port);
			os = socket.getOutputStream();
			os.write(request);
			os.flush();
			is = socket.getInputStream();
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			byte[] buffer = new byte[1024];
			int count = 0;
			do {
				count = is.read(buffer);
				bos.write(buffer, 0, count);
			} while (is.available() != 0);

			res = bos.toByteArray();

			for (byte b : res) {
				Integer x = (int) b;
				System.out.print(x.toHexString(x));
			}
			System.out.println();
			os.close();
			is.close();
			socket.close();
		} catch (Exception ex) {
			try {
				if (is != null) {
					is.close();
				}
				if (socket != null)
					socket.close();
			} catch (Exception e) {
			}
		}
		return res;
	}

	public static byte[] strToHex(String in) {
		in = in.toUpperCase();
		int len = in.length();
		byte[] result = new byte[len / 2];
		int i = 0;
		if (in != null && len != 0) {
			int c = 0;
			for (i = 0; i < len; i++) {
				c = c << 4;
				if (in.charAt(i) > 'F' || in.charAt(i) < 'A' && in.charAt(i) > '9' || in.charAt(i) < '0')
					break;
				if (in.charAt(i) >= 'A')
					c += in.charAt(i) - 'A' + 10;
				else
					c += in.charAt(i) - '0';
				if (i % 2 == 1) {
					result[i / 2] = (byte) c;
					c = 0;
				}
			}
		}
		for (byte b : result) {
			System.out.print(b);
		}
		System.out.println();
		return result;
	}
}
