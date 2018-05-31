package com.kd.server.netty;

import static io.netty.util.internal.MathUtil.isOutOfBounds;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.internal.StringUtil;

public class ShowMessageHandler extends ChannelDuplexHandler {
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        StringBuilder dump = new StringBuilder();
        ByteBuf buf = (ByteBuf) msg;
        HexUtil.appendPrettyHexDump(dump, buf, buf.readerIndex(), buf.readableBytes());
        System.out.println("发送报文:" + dump.toString());
        ctx.fireChannelRead(msg);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        StringBuilder dump = new StringBuilder();
        ByteBuf buf = (ByteBuf) msg;
        HexUtil.appendPrettyHexDump(dump, buf, buf.readerIndex(), buf.readableBytes());
        System.out.println("返回报文:" + dump.toString());
//        SpingUtilSupport.getStringRedisTemplate().opsForValue().set("resposeBody", dump.toString());
        ctx.write(msg, promise);
    }
    
    private static final class HexUtil {

        private static final String[] BYTE2HEX = new String[256];

        private static void appendPrettyHexDump(StringBuilder dump, ByteBuf buf, int offset, int length) {
            for (int i = 0; i < BYTE2HEX.length; i++) {
                BYTE2HEX[i] = ' ' + StringUtil.byteToHexStringPadded(i);
            }
            if (isOutOfBounds(offset, length, buf.capacity())) {
                throw new IndexOutOfBoundsException(
                    "expected: " + "0 <= offset(" + offset + ") <= offset + length(" + length
                        + ") <= " + "buf.capacity(" + buf.capacity() + ')');
            }
            if (length == 0) {
                return;
            }

            final int startIndex = offset;
            final int fullRows = length >>> 4;
            final int remainder = length & 0xF;

            // Dump the rows which have 16 bytes.
            for (int row = 0; row < fullRows; row ++) {
                int rowStartIndex = (row << 4) + startIndex;

                int rowEndIndex = rowStartIndex + 16;
                for (int j = rowStartIndex; j < rowEndIndex; j ++) {
                    dump.append(BYTE2HEX[buf.getUnsignedByte(j)]);
                }
            }

            // Dump the last row which has less than 16 bytes.
            if (remainder != 0) {
                int rowStartIndex = (fullRows << 4) + startIndex;
                // Hex dump
                int rowEndIndex = rowStartIndex + remainder;
                for (int j = rowStartIndex; j < rowEndIndex; j ++) {
                    dump.append(BYTE2HEX[buf.getUnsignedByte(j)]);
                }
            }

        }

    }

}
