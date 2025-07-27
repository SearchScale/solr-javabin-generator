package com.searchscale.benchmarks;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.solr.common.util.JavaBinCodec;
import static org.apache.solr.common.util.JavaBinCodec.*;

/**
 * Optimized JavaBin writer that minimizes object creation and uses direct writing
 */
public class OptimizedJavaBinWriter extends JavaBinCodec {
    
    private static final int BUFFER_SIZE = 64 * 1024; // 64KB buffer
    private final ByteBuffer tempBuffer = ByteBuffer.allocate(4096);
    
    public OptimizedJavaBinWriter(OutputStream os) throws IOException {
        // Wrap with BufferedOutputStream if not already buffered
        super(os instanceof BufferedOutputStream ? os : new BufferedOutputStream(os, BUFFER_SIZE), null);
    }
    
    /**
     * Optimized method to write a vector document directly without MapWriter overhead
     */
    public void writeVectorDocument(String id, float[] vector) throws IOException {
        // Write MAP tag
        writeTag(MAP);
        
        // Write map size (2 entries: id and article_vector)
        writeVInt(2, daos);
        
        // Write "id" field
        writeExternString("id");
        writeStr(id);
        
        // Write "article_vector" field
        writeExternString("article_vector");
        
        // Write as List<Float> for Solr compatibility
        writeTag(ARR);
        writeVInt(vector.length, daos);
        for (float f : vector) {
            writeFloat(f);
        }
    }
    
    /**
     * Optimized float array writing
     */
    private void writeFloatArray(float[] arr) throws IOException {
        // Custom tag for float arrays (21)
        writeTag((byte) 21);
        writeTag(FLOAT);
        writeVInt(arr.length, daos);
        
        // Write floats in bulk using ByteBuffer for better performance
        tempBuffer.clear();
        for (int i = 0; i < arr.length; i++) {
            if (tempBuffer.remaining() < 4) {
                // Flush buffer when full
                daos.write(tempBuffer.array(), 0, tempBuffer.position());
                tempBuffer.clear();
            }
            tempBuffer.putFloat(arr[i]);
        }
        
        // Write remaining data
        if (tempBuffer.position() > 0) {
            daos.write(tempBuffer.array(), 0, tempBuffer.position());
        }
    }
    
    @Override
    public void writeVal(Object o) throws IOException {
        if (o instanceof float[] f) {
            writeFloatArray(f);
        } else {
            super.writeVal(o);
        }
    }
    
    /**
     * Batch write optimization - write multiple documents with single iterator tags
     */
    public void writeBatch(List<String> ids, List<float[]> vectors) throws IOException {
        writeTag(ITERATOR);
        
        for (int i = 0; i < ids.size(); i++) {
            writeVectorDocument(ids.get(i), vectors.get(i));
        }
        
        writeTag(END);
    }
    
    /**
     * Stream-based batch writing without intermediate collections
     */
    public void startBatch() throws IOException {
        writeTag(ITERATOR);
    }
    
    public void endBatch() throws IOException {
        writeTag(END);
        close();
    }
}