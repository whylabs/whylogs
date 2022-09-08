package com.whylogs.core.utils;

import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import lombok.experimental.UtilityClass;

import java.io.*;
import java.lang.reflect.InvocationTargetException;

@UtilityClass
public class ProtobufUtil {

    public static <T extends Message>  T readDelimitedProtobuf(InputStream input, Class<T> protoClass, Parser<T> parser) throws IOException {
        readDelimitedProtobuf(input, protoClass, parser, 0);
    }

    public static <T extends Message>  T readDelimitedProtobuf(InputStream input, Class<T> protoClass, Parser<T> parser, int offset) throws IOException {
        int size = parseFromDelimitedSize(input, offset);
        if(size == 0){
            try{
                return protoClass.getConstructor().newInstance();
            } catch (InvocationTargetException | InstantiationException | IllegalAccessException | NoSuchMethodException e) {
                e.printStackTrace();
            }
        }

        byte[] buffer = new byte[size];
        if(input.read(buffer) == -1){
            throw new EOFException();
        }

        return parser.parseDelimitedFrom(input);
    }

    public static int readVarint(InputStream input, int offset) throws IOException {
        if(offset > 0){
            input.read(new byte[offset]);
        }

        byte[] buffer = new byte[7];
        if(input.read(buffer) == -1){
            return 0;
        }

        int i = 0;
        while((buffer[i] & 0x80) >> 7 == 1){
            int new_byte = input.read();
            if(new_byte == -1){
                throw new EOFException("Unexpected EOF");
            }
            i += 1;
            buffer[i] = (byte) new_byte;
        }

        return parseDelimitedFrom(buffer, i);
    }

    public static void writeDelimitedProtobuf(OutputStream output, Message message) throws IOException {
        if(output == null){
            throw new IOException("Output stream is null");
        }
        EncodeVarint(output, message.getSerializedSize());
        output.write(message.toByteArray());
    }
}
