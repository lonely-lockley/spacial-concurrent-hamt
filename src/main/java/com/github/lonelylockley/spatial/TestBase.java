package com.github.lonelylockley.spatial;

import com.github.lonelylockley.spatial.ctrie.H3CellId;

import java.util.Random;

/**
 * Keeping test class in the main package is not clean,
 * but allows me to keep a single implementation both for tests and jmh
 */
public abstract class TestBase<T> {
    private final Random rng = new Random();

    protected H3CellId<T> generateRandomCell(T businessEntityId) {
        var address = 0L;
        var position = H3CellId.BASE_OFFSET;
        var res = rng.nextInt(15) + 1;
        address = writeSub(address,  0, 1,  0); // reserved bit
        address = writeSub(address,  1, 4,  1); // mode
        address = writeSub(address,  8, 4, res); // resolution
        address = writeSub(address, 12, 7, randomBaseCell());
        for (int i = 1; i < 16; i++) {
            if (i <= res) {
                address = writeSub(address, position, 3, randomCell());
            }
            else {
                address = writeSub(address, position, 3, 7);
            }
            position += 3;
        }
        return new H3CellId<>(address, businessEntityId);
    }

    protected H3CellId<T> generateNonRandomCell(int baseCell, int[] cells, int resolution, T businessEntityId) {
        assert cells.length == 15;
        var address = 0L;
        var position = H3CellId.BASE_OFFSET;
        address = writeSub(address,  0, 1,  0); // reserved bit
        address = writeSub(address,  1, 4,  1); // mode
        address = writeSub(address,  8, 4, resolution); // resolution
        address = writeSub(address, 12, 7, baseCell);
        for (int i = 0; i < 15; i++) {
            address = writeSub(address, position, 3, cells[i]);
            position += 3;
        }
        return new H3CellId<>(address, businessEntityId);
    }

    protected H3CellId<T> generateNonRandomCell(int baseCell, int[] cells, int fromResolution, int toResolution, T businessEntityId) {
        assert cells.length == 15;
        var address = 0L;
        var position = H3CellId.BASE_OFFSET;
        address = writeSub(address,  0, 1,  0); // reserved bit
        address = writeSub(address,  1, 4,  1); // mode
        address = writeSub(address,  8, 4, toResolution); // resolution
        address = writeSub(address, 12, 7, baseCell);
        for (int i = 0; i < fromResolution; i++) {
            address = writeSub(address, position, 3, cells[i]);
            position += 3;
        }
        for (int i = fromResolution; i < toResolution; i++) {
            address = writeSub(address, position, 3, randomCell());
            position += 3;
        }
        for (int i = toResolution; i < 15; i++) {
            address = writeSub(address, position, 3, 7);
            position += 3;
        }
        return new H3CellId<>(address, businessEntityId);
    }

    protected H3CellId<T> generateNonRandomCell(int baseCell, T businessEntityId) {
        var address = 0L;
        var position = H3CellId.BASE_OFFSET;
        var res = rng.nextInt(15) + 1;
        address = writeSub(address,  0, 1,  0); // reserved bit
        address = writeSub(address,  1, 4,  1); // mode
        address = writeSub(address,  8, 4, res); // resolution
        address = writeSub(address, 12, 7, baseCell);
        for (int i = 1; i < 16; i++) {
            if (i <= res) {
                address = writeSub(address, position, 3, randomCell());
            }
            else {
                address = writeSub(address, position, 3, 7);
            }
            position += 3;
        }
        return new H3CellId<>(address, businessEntityId);
    }

    protected H3CellId<T> generateRandomChildForCell(long baseAddress, int resolution, T businessEntityId) {
        var address = baseAddress;
        var src = new H3CellId<>(baseAddress, null);
        var position = H3CellId.BASE_OFFSET + (3 * src.getResolution());
        address = writeSub(address,  8, 4, resolution); // resolution
        for (int i = src.getResolution(); i < resolution; i++) {
            address = writeSub(address, position, 3, randomCell());
            position += 3;
        }
        return new H3CellId<>(address, businessEntityId);
    }

    protected H3CellId<T> generateNonRandomCellFullRes(int baseCell, T businessEntityId) {
        var address = 0L;
        var position = H3CellId.BASE_OFFSET;
        var res = 15;
        address = writeSub(address,  0, 1,  0); // reserved bit
        address = writeSub(address,  1, 4,  1); // mode
        address = writeSub(address,  8, 4, res); // resolution
        address = writeSub(address, 12, 7, baseCell);
        for (int i = 0; i < 15; i++) {
            address = writeSub(address, position, 3, randomCell());
            position += 3;
        }
        return new H3CellId<>(address, businessEntityId);
    }

    private long extractSub(final long address, final int offset, final int nrBits) {
        final long rightShifted = address >>> (64 - offset - nrBits);
        final long mask = (1L << nrBits) - 1L;
        return rightShifted & mask;
    }

    protected H3CellId<T> generateNonRandomCell(long address, T businessEntityId) {
        return new H3CellId<>(address, businessEntityId);
    }

    private int randomBaseCell() {
        return rng.nextInt(122);
    }

    private int randomCell() {
        return rng.nextInt(7);
    }

    private long writeSub(final long target, final int offset, final int nrBits, final int value) {
        long mask = (1L << nrBits) - 1;
        int pos = 64 - offset - nrBits;
        long bitsToWrite = (value & mask) << pos;
        mask <<= pos;
        return (target & ~mask) | bitsToWrite;
    }
}

