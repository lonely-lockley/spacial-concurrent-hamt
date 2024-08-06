package com.github.lonelylockley.spacial.ctrie.nodes;

import com.github.lonelylockley.spacial.ctrie.*;

import java.util.concurrent.ThreadLocalRandom;

public class BranchNode<T, V> extends BaseNode<T, V> {

    public final long bitmapLow;
    public final long bitmapHigh;
    public final BaseNode<T, V>[] array;
    public final Gen gen;
    public final int res;

    public BranchNode(final long bitmapLow, final long bitmapHigh, final BaseNode<T, V>[] array, final Gen gen, final int resolution) {
        this.bitmapLow = bitmapLow;
        this.bitmapHigh = bitmapHigh;
        this.array = array;
        this.gen = gen;
        this.res = resolution;
    }

    // this should only be called from within read-only snapshots
    @Override
    protected final int cachedSize(SpacialConcurrentTrieMap<T, V> instance) {
        int currsz = getSize();
        if (currsz != -1) {
            return currsz;
        }
        else {
            int sz = computeSize(instance);
            while (getSize() == -1) {
                swapSize(-1, sz);
            }
            return getSize();
        }
    }

    // lends itself towards being parallelizable by choosing a random starting offset in the array => if there are
    // concurrent size computations, they start at different positions, so they are more likely to be independent
    private int computeSize(final SpacialConcurrentTrieMap<T, V> ct) {
        int i = 0;
        int sz = 0;
        final int offset = (array.length > 0) ? ThreadLocalRandom.current().nextInt(0, array.length) : 0;
        while (i < array.length) {
            int pos = (i + offset) % array.length;
            Node<T, V> elem = array[pos];
            if (elem instanceof LeafNode<T, V>) {
                sz += 1;
            }
            else
            if (elem instanceof NodeWrapper<T, V> wrapper) {
                sz += wrapper.cachedSize(ct);
            }
            i += 1;
        }
        return sz;
    }

    protected BranchNode<T, V> updatedAt(int pos, final BaseNode<T, V> nn, final Gen gen) {
        BaseNode[] narr = array.clone();
        narr[pos] = nn;
        return new BranchNode<T, V>(bitmapLow, bitmapHigh, narr, gen, res);
    }

    protected BranchNode<T, V> removedAt(int idx, int pos, long flag, final Gen gen) {
        BaseNode[] arr = array;
        int len = arr.length;
        BaseNode[] narr = new BaseNode[len - 1];
        System.arraycopy (arr, 0, narr, 0, pos);
        System.arraycopy (arr, pos + 1, narr, pos, len - pos - 1);
        if (idx >= 64) {
            return new BranchNode<T, V>(bitmapLow, bitmapHigh ^ flag, narr, gen, res);
        }
        else {
            return new BranchNode<T, V>(bitmapLow ^ flag, bitmapHigh, narr, gen, res);
        }
    }

    protected BranchNode<T, V> insertedAt(int idx, int pos, long flag, final BaseNode<T, V> nn, final Gen gen) {
        int len = array.length;
        BaseNode[] narr = new BaseNode[len + 1];
        System.arraycopy(array, 0, narr, 0, pos);
        narr[pos] = nn;
        System.arraycopy(array, pos, narr, pos + 1, len - pos);
        if (idx >= 64) {
            return new BranchNode<T, V>(bitmapLow, bitmapHigh | flag, narr, gen, res);
        }
        else {
            return new BranchNode<T, V>(bitmapLow | flag, bitmapHigh, narr, gen, res);
        }
    }

    /**
     * Returns a copy of this BranchNode such that all the NodeWrappers below it are
     * copied to the specified generation `ngen`.
     */
    protected BranchNode<T, V> renewed(final Gen ngen, final SpacialConcurrentTrieMap<T, V> ct) {
        int i = 0;
        BaseNode[] arr = array;
        int len = arr.length;
        BaseNode[] narr = new BaseNode[len];
        while (i < len) {
            BaseNode<T, V> elem = arr[i];
            if (elem instanceof NodeWrapper<T, V> in) {
                narr[i] = in.copyToGen(ngen, ct);
            }
            else {
                narr[i] = elem;
            }
            i += 1;
        }
        return new BranchNode<T, V>(bitmapLow, bitmapHigh, narr, ngen, res);
    }

    protected BranchNode<T, V> toCompressedInternal(final SpacialConcurrentTrieMap<T, V> ct, int lev, Gen gen) {
        int i = 0;
        BaseNode[] arr = array;
        BaseNode[] tmparray = new BaseNode[arr.length];
        while (i < arr.length) { // construct new bitmap
            BaseNode<T, V> sub = arr[i];
            if (sub instanceof NodeWrapper<T, V> in) {
                BaseNode<T, V> inodemain = in.getGCAS(ct);
                assert (inodemain != null);
                tmparray[i] = resurrect(in, inodemain);
            }
            else
            if (sub instanceof LeafNode<T, V>) {
                tmparray[i] = sub;
            }
            i += 1;
        }

        return new BranchNode<T, V>(bitmapLow, bitmapHigh, tmparray, gen, res);
    }

    // - if the branching factor is 1 for this CNode, and the child is a tombed LeafNode, returns its tombed version
    // - otherwise, if there is at least one non-null node below, returns the version of this node with at least some null-inodes
    // removed (those existing when the op began)
    // - if there are only null-i-nodes below, returns null
    protected BaseNode<T, V> toCompressed(final SpacialConcurrentTrieMap<T, V> ct, int lev, Gen gen) {
        return toCompressedInternal(ct, lev, gen).toContracted();
    }

    private BaseNode<T, V> resurrect(final NodeWrapper<T, V> inode, final BaseNode<T, V> inodemain) {
        if (inodemain instanceof TombstoneNode<T, V> tn) {
            return tn.copyUntombed();
        }
        else {
            return inode;
        }
    }

    final BaseNode<T, V> toContracted() {
        if (array.length == 1 && res > 0) {
            BaseNode<T, V> first = array[0];
            if (first instanceof LeafNode<T, V> sn) {
                return sn.copyTombed();
            }
            else {
                return this;
            }
        }
        else {
            return this;
        }
    }

    public static <T, V> BaseNode<T, V> dual(final LeafNode<T, V> x, final LeafNode<T, V> y, int offset, Gen gen, int resolution) {
        final var maxResolution = Math.min(x.key.getResolution(), y.key.getResolution());
        if (resolution <= maxResolution) {
            long xidx = (x.key.getAddress() >>> (64 - offset - 3)) & 0x7;
            long yidx = (y.key.getAddress() >>> (64 - offset - 3)) & 0x7;
            int bmp = (1 << xidx) | (1 << yidx);

            if (xidx == yidx) {
                NodeWrapper<T, V> subinode = new NodeWrapper<>(gen);// (TrieMap.inodeupdater)
                subinode.wrapped = dual(x, y, offset + 3, gen, resolution + 1);
                return new BranchNode<T, V>(bmp, 0, new BaseNode[] { subinode }, gen, resolution);
            }
            else {
                if (xidx < yidx) {
                    return new BranchNode<T, V>(bmp, 0, new BaseNode[]{new LeafNode<>(x.key, x.value, x.hash, resolution), new LeafNode<>(y.key, y.value, y.hash, resolution)}, gen, resolution);
                }
                else {
                    return new BranchNode<T, V>(bmp, 0, new BaseNode[]{new LeafNode<>(y.key, y.value, y.hash, resolution), new LeafNode<>(x.key, x.value, x.hash, resolution)}, gen, resolution);
                }
            }
        }
        else {
            return new CollisionAwareNode<>(x.key, x.value, y.key, y.value, resolution);
        }
    }

    @Override
    public int resolution() {
        return res;
    }
}
