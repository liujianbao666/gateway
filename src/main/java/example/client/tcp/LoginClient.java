package example.client.tcp;

import java.io.BufferedReader;  
import java.io.IOException;  
import java.io.InputStream;  
import java.io.InputStreamReader;  
import java.io.OutputStream;  
import java.io.PrintWriter;  
import java.net.Socket;  
import java.net.UnknownHostException;  
  
public class LoginClient {  
    public static void main(String[] args) {  
        try {  
            //1.建立客户端socket连接，指定服务器位置及端口  
            Socket socket =new Socket("127.0.0.1",1883);  
            //2.得到socket读写流  
            OutputStream os=socket.getOutputStream();  
            PrintWriter pw=new PrintWriter(os);  
            //输入流  
            InputStream is=socket.getInputStream();  
            BufferedReader br=new BufferedReader(new InputStreamReader(is));  
            //3.利用流按照一定的操作，对socket进行读写操作  
            String info="102C00044D5154540402003C00203232323938646131633666383439333961613863383436646630303566346138";  
            pw.write(strToHex(info));  
            pw.flush();  
            socket.shutdownOutput();  
            //接收服务器的相应  
            String reply=null;  
            while(!((reply=br.readLine())==null)){  
            	byte[] a = reply.getBytes();
            	for(int i =0;i<a.length;i++)
                System.out.println("接收服务器的信息："+a[i]);  
            }  
            //4.关闭资源  
            br.close();  
            is.close();  
            pw.close();  
            os.close();  
            socket.close();  
        } catch (UnknownHostException e) {  
            e.printStackTrace();  
        } catch (IOException e) {  
            e.printStackTrace();  
        }  
    }  
	public static char[] strToHex(String in) {
		int len = in.length();
		char[] result = new char[len / 2];
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
					result[i / 2] = (char) c;
					c = 0;
				}
			}
		}
		return result;
	}
} 
