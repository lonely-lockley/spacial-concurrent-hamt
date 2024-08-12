package com.github.lonelylockley.spatial.ctrie;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

public class H3CellId<T> implements Serializable {

    public static final int BASE_OFFSET = 19;
    public static final int MAX_DEPTH = 16; // (base level - 0 and res 1-15)

    private final long address;
    private final int[] cells = new int[15];
    private final int resolution;
    private final int baseCell;
    private final T entityId;

    private int position = BASE_OFFSET; // starting offset for resolution layers

    public static String trimToResolution(String cellId, int res) {
        var address = Long.parseLong(cellId, 0, cellId.length(), 16);
        var resolution = extractSub(address, 8, 4);
        if (resolution < res) {
            throw new IllegalArgumentException("Cannot create cellId with resolution " + res + " out of cell with resolution " + resolution);
        }
        if (resolution < 0 || resolution > 16) {
            throw new IllegalArgumentException("Wrong resolution: " + resolution);
        }
        address = writeSub(address, 8, 4, res);
        for (int i = res; i < 15; i++) {
            address = writeSub(address, BASE_OFFSET + i * 3, 3, 7);
        }
        return Long.toHexString(address);
    }

    public H3CellId(String cellId, T businessEntityId) {
        this(Long.parseLong(cellId, 0, cellId.length(), 16), businessEntityId);
    }

    public H3CellId(long address, T businessEntityId) {
        this.address = address;
        if (readReserved() != 0) { // reserved bit
            throw new IllegalArgumentException("Unexpected value in the reserved bit");
        }
        if (readMode() != 1) { // cell mode
            throw new IllegalArgumentException("Only cell mode is supported");
        }
        this.resolution = readResolution();
        if (resolution < 0 || resolution > 16) {
            throw new IllegalArgumentException("Wrong resolution: " + resolution);
        }
        this.baseCell = readBaseCell();
        if (baseCell < 0 || baseCell > 121) {
            throw new IllegalArgumentException("Wrong base cell id: " + baseCell);
        }
        for (int i = 0; i < 15; i++) {
            cells[i] = readCell();
        }
        this.entityId = businessEntityId;
    }

    public long getAddress() {
        return address;
    }

    private int readReserved() {
        return (int) extractSub(address, 0, 1);
    }

    private int readMode() {
        return (int) extractSub(address, 1, 4);
    }

    private int readResolution() {
        return (int) extractSub(address, 8, 4);
    }

    private int readBaseCell() {
        return (int) extractSub(address, 12, 7);
    }

    private int readCell() {
        var res = extractSub(address, position, 3);
        position += 3;
        return (int) res;
    }

    private static long extractSub(final long source, final int offset, final int nrBits) {
        final long rightShifted = source >>> (64 - offset - nrBits);
        final long mask = (1L << nrBits) - 1L;
        return rightShifted & mask;
    }

    private static long writeSub(final long target, final int offset, final int nrBits, final int value) {
        long mask = (1L << nrBits) - 1;
        int pos = 64 - offset - nrBits;
        long bitsToWrite = (value & mask) << pos;
        mask <<= pos;
        return (target & ~mask) | bitsToWrite;
    }

    public int getResolution() {
        return resolution;
    }

    public int getBaseCell() {
        return baseCell;
    }

    public String getCellId() {
        return Long.toHexString(address);
    }

    public int getCell(int resolution) {
        if (resolution < 0 || resolution > 16) {
            throw new IllegalArgumentException("Wrong resolution: " + resolution);
        }
        if (resolution == 0) {
            return baseCell;
        }
        return cells[resolution - 1];
    }

    public T getBusinessEntityId() {
        return entityId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        H3CellId h3CellId = (H3CellId) o;
        return address == h3CellId.address && entityId.equals(h3CellId.entityId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(address, entityId);
    }

    @Override
    public String toString() {
        return "H3CellId{" +
                "cellId=" + getCellId() +
                ", address=" + address +
                ", cells=" + Arrays.toString(cells) +
                ", resolution=" + resolution +
                ", baseCell=" + baseCell +
                ", entityId='" + entityId + '\'' +
                '}';
    }
}
