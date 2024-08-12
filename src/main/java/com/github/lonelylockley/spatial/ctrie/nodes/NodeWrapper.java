package com.github.lonelylockley.spatial.ctrie.nodes;

import com.github.lonelylockley.spatial.ctrie.SpatialConcurrentTrieMap;
import com.github.lonelylockley.spatial.ctrie.Gen;
import com.github.lonelylockley.spatial.ctrie.H3CellId;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

public class NodeWrapper<T, V> extends BaseNode<T, V> {

    private static final AtomicReferenceFieldUpdater<NodeWrapper, BaseNode> updater = AtomicReferenceFieldUpdater.newUpdater(NodeWrapper.class, BaseNode.class, "wrapped");

    public static final Condition KEY_PRESENT = new Condition();
    public static final Condition KEY_ABSENT = new Condition();
    public static final Condition RESTART = new Condition();

    protected volatile BaseNode<T, V> wrapped = null;
    public final Gen gen;

    public NodeWrapper(BaseNode<T, V> wrapped, Gen gen) {
        this.wrapped = wrapped;
        this.gen = gen;
    }

    public NodeWrapper(Gen gen) {
        this(null, gen);
    }

    public static <T, V> NodeWrapper<T, V> newRootNode() {
        Gen gen = new Gen();
        BranchNode<T, V> cn = new BranchNode<T, V>(0, 0, new BaseNode[0], gen, 0);
        return new NodeWrapper<>(cn, gen);
    }

    protected boolean swapWrappedNode(BaseNode<T, V> oldValue, BaseNode<T, V> newValue) {
        return updater.compareAndSet(this, oldValue, newValue);
    }

    protected void setWrappedNode(BaseNode<T, V> newValue) {
        updater.set(this, newValue);
    }

    public BaseNode<T, V> getWrapped() {
        return updater.get(this);
    }

    public final BaseNode<T, V> getGCAS(SpatialConcurrentTrieMap<T, V> instance) {
        BaseNode<T, V> copy = wrapped; // create a copy of a pointer
        if (copy.parent == null) {
            return copy;
        }
        else {
            return getGCASInternal(copy, instance);
        }
    }

    private BaseNode<T, V> getGCASInternal(BaseNode<T, V> copy, final SpatialConcurrentTrieMap<T, V> instance) {
        while (true) {
            if (copy == null) {
                return null;
            }
            else {
                // complete the GCAS
                BaseNode<T, V> parent = copy.parent;
                NodeWrapper<T, V> ctr = instance.readRoot(true);

                if (parent == null) {
                    return copy;
                }

                if (parent instanceof FailedNode<T, V> failed) {
                    // try to commit to previous value
                    if (swapWrappedNode(copy, failed.parent)) {
                        return failed.parent;
                    }
                    else {
                        // Tailrec
                        copy = /* READ */wrapped;
                    }
                }
                else {
                    // Assume that you've read the root from the generation G.
                    // Assume that the snapshot algorithm is correct.
                    // ==> you can only reach nodes in generations <= G.
                    // ==> `gen` is <= G.
                    // We know that `ctr.gen` is >= G.
                    // ==> if `ctr.gen` = `gen` then they are both equal to G.
                    // ==> otherwise, we know that either `ctr.gen` > G, `gen` < G, or both
                    if ((ctr.gen == gen) && !instance.isReadOnly()) {
                        // try to commit
                        if (copy.swapParent (parent, null))
                            return copy;
                        else {
                            // return GCAS_Complete (m, ct);
                            // tailrec
                            continue;
                        }
                    } else {
                        // try to abort
                        copy.swapParent(parent, new FailedNode<>(parent));
                        return getGCASInternal(/* READ */wrapped, instance);
                    }
                }
            }
        }
    }

    protected final boolean setGCAS(final BaseNode<T, V> oldValue, final BaseNode<T, V> newValue, final SpatialConcurrentTrieMap<T, V> ct) {
        newValue.setParent(oldValue);
        if (swapWrappedNode(oldValue, newValue)) {
            getGCASInternal(newValue, ct);
            return /* READ */newValue.parent == null;
        }
        else {
            return false;
        }
    }

    @Override
    public final int cachedSize(final SpatialConcurrentTrieMap<T, V> instance) {
        BaseNode<T, V> m = getGCAS(instance);
        return m.cachedSize(instance);
    }

    private NodeWrapper<T, V> wrap(final BaseNode<T, V> cn) {
        NodeWrapper<T, V> nin = new NodeWrapper<>(gen);
        nin.setWrappedNode(cn);
        return nin;
    }

    public final NodeWrapper<T, V> copyToGen(final Gen newGeneration, final SpatialConcurrentTrieMap<T, V> instance) {
        NodeWrapper<T, V> wrapper = new NodeWrapper<>(newGeneration);
        BaseNode<T, V> main = getGCAS(instance);
        wrapper.setWrappedNode(main);
        return wrapper;
    }

    private void clean(final NodeWrapper<T, V> nd, final SpatialConcurrentTrieMap<T, V> ct, int offset) {
        BaseNode<T, V> m = nd.getGCAS(ct);
        if (m instanceof BranchNode<T, V> cn) {
            nd.setGCAS(cn, cn.toCompressed(ct, offset, gen), ct);
        }
    }

    public final boolean insertInternal(final H3CellId<T> key, final V value, final long hash, final int offset, final NodeWrapper<T, V> parent, final Gen startgen, final SpatialConcurrentTrieMap<T, V> instance) {
        while(true) {
            BaseNode<T, V> m = getGCAS(instance); // use -Yinline!
            if (m instanceof BranchNode<T, V> cn) { // and RootNode too
                // 1) a multiway node
                final int idx;
                final int pos;
                final long flag;
                final long bmp;
                if (cn.res == 0) {
                    idx = key.getBaseCell();
                    flag = 1L << idx;
                    final long mask = flag - 1;
                    if (idx >= 64) {
                        pos = Long.bitCount(cn.bitmapLow) + Long.bitCount(cn.bitmapHigh & mask);
                        bmp = cn.bitmapHigh;
                    }
                    else {
                        pos = Long.bitCount(cn.bitmapLow & mask);
                        bmp = cn.bitmapLow;
                    }
                }
                else {
                    idx = (int) (hash >>> (64 - offset - 3)) & 0x7;
                    bmp = cn.bitmapLow;
                    flag = 1L << idx;
                    final long mask = flag - 1;
                    pos = Long.bitCount(bmp & mask);
                }

                if ((bmp & flag) != 0) {
                    // 1a) insert below
                    final BaseNode<T, V> cnAtPos = cn.array[pos];
                    if (cnAtPos instanceof NodeWrapper<T, V> in) {
                        if (startgen == in.gen) {
                            return in.insertInternal(key, value, hash, cn.res == 0 ? offset : offset + 3, this, startgen, instance);
                        }
                        else {
                            if (setGCAS(cn, cn.renewed(startgen, instance), instance)) {
                                continue;
                            }
                            else {
                                return false;
                            }
                        }
                    }
                    else
                    if (cnAtPos instanceof LeafNode<T, V> sn) {
                        if (sn.hash == hash && Objects.equals(sn.key, key)) {
                            return setGCAS(cn, cn.updatedAt(pos, new LeafNode<>(key, value, hash, sn.res), gen), instance);
                        }
                        else {
                            BranchNode<T, V> rn = (cn.gen == gen) ? cn : cn.renewed(gen, instance);
                            BaseNode<T, V> nn = rn.updatedAt(pos, wrap(BranchNode.dual(sn, new LeafNode<>(key, value, hash, sn.res), cn.res == 0 ? offset : offset + 3, gen, rn.res + 1)), gen);
                            return setGCAS(cn, nn, instance);
                        }
                    }
                }
                else {
                    BranchNode<T, V> rn = (cn.gen == gen) ? cn : cn.renewed(gen, instance);
                    BaseNode<T, V> ncnode = rn.insertedAt(idx, pos, flag, new LeafNode<>(key, value, hash, rn.res + 1), gen);
                    return setGCAS(cn, ncnode, instance);
                }
            }
            else
            if (m instanceof TombstoneNode<T, V>) {
                clean(parent, instance, offset - 3);
                return false;
            }
            else
            if (m instanceof CollisionAwareNode<T, V> ln) {
                BaseNode<T, V> nn = ln.inserted(key, value);
                return setGCAS(ln, nn, instance);
            }

            throw new RuntimeException ("Should not happen");
        }
    }

    /**
     * Inserts a new key value pair, given that a specific condition is met.
     *
     * @param cond
     *            null - don't care if the key was there
     *            KEY_ABSENT - key wasn't there
     *            KEY_PRESENT - key was there
     *            other value `v` - key must be bound to `v`
     * @return null if unsuccessful, Option[V] otherwise (indicating
     *         previous value bound to the key)
     */
    public final Optional<V> insertWithConditionInternal(final H3CellId<T> key, final V value, final V cond, final int offset, final NodeWrapper<T, V> parent, final Gen startgen, final SpatialConcurrentTrieMap<T, V> instance) {
        while (true) {
            BaseNode<T, V> m = getGCAS(instance); // use -Yinline!
            if (m instanceof BranchNode<T, V> cn) {
                // 1) a multiway node
                final int idx;
                final int pos;
                final long flag;
                final long bmp;
                if (cn.res == 0) {
                    idx = key.getBaseCell();
                    flag = 1L << idx;
                    final long mask = flag - 1;
                    if (idx >= 64) {
                        pos = Long.bitCount(cn.bitmapLow) + Long.bitCount(cn.bitmapHigh & mask);
                        bmp = cn.bitmapHigh;
                    }
                    else {
                        pos = Long.bitCount(cn.bitmapLow & mask);
                        bmp = cn.bitmapLow;
                    }
                }
                else {
                    idx = (int) (key.getAddress() >>> (64 - offset - 3)) & 0x7;
                    bmp = cn.bitmapLow;
                    flag = 1L << idx;
                    final long mask = flag - 1;
                    pos = Long.bitCount(bmp & mask);
                }

                if ((bmp & flag) != 0) {
                    // 1a) insert below
                    BaseNode<T, V> cnAtPos = cn.array[pos];
                    if (cnAtPos instanceof NodeWrapper<T, V> in) {
                        if (startgen == in.gen) {
                            return in.insertWithConditionInternal(key, value, cond, cn.res == 0 ? offset : offset + 3, this, startgen, instance);
                        }
                        else {
                            if (!setGCAS(cn, cn.renewed(startgen, instance), instance)) {
                                return null;
                            }
                        }
                    }
                    else
                    if (cnAtPos instanceof LeafNode<T, V> sn) {
                        if (cond == null) {
                            if (sn.hash == key.getAddress() && Objects.equals(sn.key, key)) {
                                if (setGCAS(cn, cn.updatedAt(pos, new LeafNode<>(key, value, key.getAddress(), sn.res), gen), instance)) {
                                    return Optional.ofNullable(sn.value);
                                }
                                else {
                                    return null;
                                }
                            }
                            else {
                                BranchNode<T, V> rn = (cn.gen == gen) ? cn : cn.renewed(gen, instance);
                                BaseNode<T, V> nn = rn.updatedAt (pos, wrap(BranchNode.dual(sn, new LeafNode<>(key, value, key.getAddress(), sn.res), cn.res == 0 ? offset : offset + 3, gen, sn.res + 1)), gen);
                                if (setGCAS(cn, nn, instance)) {
                                    return Optional.empty(); // None;
                                }
                                else {
                                    return null;
                                }
                            }

                        }
                        else
                        if (cond == NodeWrapper.KEY_ABSENT) {
                            if (sn.hash == key.getAddress() && Objects.equals(sn.key, key)) {
                                return Optional.ofNullable(sn.value);
                            }
                            else {
                                BranchNode<T, V> rn = (cn.gen == gen) ? cn : cn.renewed(gen, instance);
                                BaseNode<T, V> nn = rn.updatedAt(pos, wrap(BranchNode.dual(sn, new LeafNode<>(key, value, key.getAddress(), sn.res), cn.res == 0 ? offset : offset + 3, gen, sn.res + 1)), gen);
                                if (setGCAS(cn, nn, instance)) {
                                    return Optional.empty(); // None
                                }
                                else {
                                    return null;
                                }
                            }
                        }
                        else
                        if (cond == NodeWrapper.KEY_PRESENT) {
                            if (sn.hash == key.getAddress() && Objects.equals(sn.key, key)) {
                                if (setGCAS(cn, cn.updatedAt(pos, new LeafNode<>(key, value, key.getAddress(), sn.res), gen), instance)) {
                                    return Optional.ofNullable(sn.value);
                                }
                                else {
                                    return null;
                                }

                            }
                            else {
                                return Optional.empty();// None;
                            }
                        }
                        else {
                            if (sn.hash == key.getAddress() && Objects.equals(sn.key, key) && Objects.equals(sn.value, cond)) {
                                if (setGCAS(cn, cn.updatedAt(pos, new LeafNode<>(key, value, key.getAddress(), sn.res), gen), instance)) {
                                    return Optional.of(sn.value);
                                }
                                else {
                                    return null;
                                }
                            }
                            else {
                                return Optional.empty(); // None
                            }
                        }

                    }
                }
                else
                if (cond == null || cond == NodeWrapper.KEY_ABSENT) {
                    BranchNode<T, V> rn = (cn.gen == gen) ? cn : cn.renewed(gen, instance);
                    BranchNode<T, V> ncnode = rn.insertedAt(idx, pos, flag, new LeafNode<>(key, value, key.getAddress(), cn.res + 1), gen);
                    if (setGCAS(cn, ncnode, instance)) {
                        return Optional.empty();// None
                    }
                    else {
                        return null;
                    }
                }
                else
                if (cond == NodeWrapper.KEY_PRESENT) {
                    return Optional.empty();// None;
                }
                else {
                    return Optional.empty(); // None
                }
            }
            else
            if (m instanceof TombstoneNode<T, V>) {
                clean(parent, instance, offset - 3);
                return null;
            }
            else
            if (m instanceof CollisionAwareNode<T, V> ln) {
                // 3) an l-node
                if (cond == null) {
                    Optional<V> optv = ln.get(key);
                    if (insertIntoCollisionAwareNode(ln, key, value, instance)) {
                        return optv;
                    }
                    else {
                        return null;
                    }
                }
                else
                if (cond == NodeWrapper.KEY_ABSENT) {
                    Optional<V> t = ln.get(key);
                    if (t.isEmpty()) {
                        if (insertIntoCollisionAwareNode(ln, key, value, instance)) {
                            return Optional.empty();// None
                        }
                        else {
                            return null;
                        }
                    }
                    else {
                        return t;
                    }
                }
                else
                if (cond == NodeWrapper.KEY_PRESENT) {
                    Optional<V> t = ln.get(key);
                    if (t.isPresent()) {
                        if (insertIntoCollisionAwareNode(ln, key, value, instance)) {
                            return t;
                        }
                        else {
                            return null;
                        }
                    }
                    else {
                        return Optional.empty(); // None
                    }
                }
                else {
                    Optional<V> t = ln.get(key);
                    if (t.isPresent() && t.get() == cond) {
                        if (insertIntoCollisionAwareNode(ln, key, value, instance)) {
                            return Optional.of(cond); // typecast will probably fail and should be fixed in the future. original code uses Object for cond
                        }
                        else {
                            return null;
                        }

                    }
                    else {
                        return Optional.empty();
                    }
                }
            }
        }
    }

    private boolean insertIntoCollisionAwareNode(final CollisionAwareNode<T, V> ln, final H3CellId key, final V value, final SpatialConcurrentTrieMap<T, V> ct) {
        CollisionAwareNode<T, V> nn = ln.inserted(key, value);
        return setGCAS(ln, nn, ct);
    }

    private final void cleanParent(final BaseNode<T, V> nonlive, final NodeWrapper<T, V> parent, final SpatialConcurrentTrieMap<T, V> instance, final long hash, final int offset, final Gen startgen) {
        while (true) {
            BaseNode<T, V> pm = parent.getGCAS(instance);
            if (pm instanceof BranchNode<T, V> cn) {
                final int idx;
                final int pos;
                final long flag;
                final long bmp;
                if (cn.res == 0) {
                    // !!!
                    // we do not have a base cell here, but can calculate it knowing that
                    // base cell is stored using 7 bits and we're at offset 19 here
                    idx = (int) (hash >>> (64 - offset - 7));
                    flag = 1L << idx;
                    final long mask = flag - 1;
                    if (idx >= 64) {
                        pos = Long.bitCount(cn.bitmapLow) + Long.bitCount(cn.bitmapHigh & mask);
                        bmp = cn.bitmapHigh;
                    }
                    else {
                        pos = Long.bitCount(cn.bitmapLow & mask);
                        bmp = cn.bitmapLow;
                    }
                }
                else {
                    idx = (int) (hash >>> (64 - offset - 3)) & 0x7;
                    bmp = cn.bitmapLow;
                    flag = 1L << idx;
                    final long mask = flag - 1;
                    pos = Long.bitCount(bmp & mask);
                }

                if ((bmp & flag) != 0) {
                    BaseNode<T, V> sub = cn.array[pos];
                    if (sub == this) {
                        if (nonlive instanceof TombstoneNode<T, V> tn) {
                            BaseNode<T, V> ncn = cn.updatedAt(pos, tn.copyUntombed(), gen).toContracted();
                            if (!parent.setGCAS(cn, ncn, instance)) {
                                if (instance.readRoot().gen == startgen) {
                                    // tailrec
                                    continue;
                                }
                            }
                        }
                    }
                }
            }
            // parent is no longer a cnode, we're done
            break;
        }
    }

    /**
     * Removes the key associated with the given value.
     *
     * @param value
     *            if null, will remove the key irregardless of the value;
     *            otherwise removes only if binding contains that exact key
     *            and value
     * @return null if not successful, an Option[V] indicating the previous
     *         value otherwise
     */
    public final Optional<V> removeInternal(H3CellId<T> key, V value, int offset, final NodeWrapper<T, V> parent, final Gen startgen, final SpatialConcurrentTrieMap<T, V> instance) {
        BaseNode<T, V> m = getGCAS(instance); // use -Yinline!
        if (m instanceof BranchNode<T, V> cn) {
            final int idx;
            final int pos;
            final long flag;
            final long bmp;
            if (cn.res == 0) {
                idx = key.getBaseCell();
                flag = 1L << idx;
                final long mask = flag - 1;
                if (idx >= 64) {
                    pos = Long.bitCount(cn.bitmapLow) + Long.bitCount(cn.bitmapHigh & mask);
                    bmp = cn.bitmapHigh;
                }
                else {
                    pos = Long.bitCount(cn.bitmapLow & mask);
                    bmp = cn.bitmapLow;
                }
            }
            else {
                idx = (int) (key.getAddress() >>> (64 - offset - 3)) & 0x7;
                bmp = cn.bitmapLow;
                flag = 1L << idx;
                final long mask = flag - 1;
                pos = Long.bitCount(bmp & mask);
            }

            if ((bmp & flag) == 0) {
                return Optional.empty();
            }
            else {
                BaseNode<T, V> sub = cn.array[pos];
                Optional<V> result = null;
                if (sub instanceof NodeWrapper<T, V> in) {
                    if (startgen == in.gen) {
                        result = in.removeInternal(key, value, (cn.res == 0 ) ? offset : offset + 3, this, startgen, instance);
                    }
                    else
                    if (setGCAS(cn, cn.renewed(startgen, instance), instance)) {
                        result = removeInternal(key, value, offset, parent, startgen, instance);
                    }

                }
                else
                if (sub instanceof LeafNode<T, V> sn) {
                    if (sn.hash == key.getAddress() && Objects.equals(sn.key, key) && (value == null || value.equals(sn.value))) {
                        BaseNode<T, V> ncn = cn.removedAt(idx, pos, flag, gen).toContracted();
                        if (setGCAS(cn, ncn, instance)) {
                            result = Optional.ofNullable(sn.value);
                        }
                    }
                    else {
                        result = Optional.empty();
                    }
                }

                if (result == null || result.isEmpty()) {
                    return result;
                }
                else {
                    if (parent != null) { // never tomb at root
                        BaseNode<T, V> n = getGCAS(instance);
                        if (n instanceof TombstoneNode<T, V>) {
                            cleanParent(n, parent, instance, key.getAddress(), offset, startgen);
                        }
                    }
                    return result;
                }
            }
        }
        else
        if (m instanceof TombstoneNode<T, V>) {
            clean (parent, instance, offset - 3);
            return null;
        }
        else
        if (m instanceof CollisionAwareNode<T, V> ln) {
            if (value == null) {
                Optional<V> optv = ln.get(key);
                BaseNode<T, V> nn = ln.removed(key, instance);
                if (setGCAS(ln, nn, instance)) {
                    return optv;
                }
                return null;
            }
            else {
                Optional<V> tmp = ln.get(key);
                if (tmp.isPresent() && Objects.equals(tmp.get(), value)) {
                    BaseNode<T, V> nn = ln.removed(key, instance);
                    if (setGCAS(ln, nn, instance)) {
                        return tmp;
                    }
                    return null;
                }
                else {
                    return Optional.empty();
                }
            }
        }
        throw new RuntimeException ("Should not happen");
    }

    /**
     * Looks up the value associated with the key.
     *
     * @return null if no value has been found, RESTART if the operation
     *         wasn't successful, or any other value otherwise
     */
    public final Object lookupInternal(final H3CellId<T> key, int offset, NodeWrapper<T, V> parent, final Gen startgen, final SpatialConcurrentTrieMap<T, V> instance) {
        while (true) {
            BaseNode<T, V> m = getGCAS(instance); // use -Yinline!
            if (m instanceof BranchNode<T, V> cn) {
                // 1) a multinode
                int pos;
                long flag;
                final long bmp;
                if (cn.res == 0) {
                    final int idx = key.getBaseCell();
                    flag = 1L << idx;
                    final long mask = flag - 1;
                    if (idx >= 64) {
                        pos = Long.bitCount(cn.bitmapLow) + Long.bitCount(cn.bitmapHigh & mask);
                        bmp = cn.bitmapHigh;
                    }
                    else {
                        pos = Long.bitCount(cn.bitmapLow & mask);
                        bmp = cn.bitmapLow;
                    }
                }
                else {
                    final long idx = (key.getAddress() >>> (64 - offset - 3)) & 0x7;
                    bmp = cn.bitmapLow;
                    flag = 1L << idx;
                    final long mask = flag - 1;
                    pos = Long.bitCount(bmp & mask);
                }

                if ((bmp & flag) == 0) {
                    return null; // 1a) bitmap shows no binding
                }
                else { // 1b) bitmap contains a value - descend
                    final BaseNode<T, V> sub = cn.array[pos];
                    if (sub instanceof NodeWrapper<T, V> in) {
                        if (instance.isReadOnly() || (startgen == in.gen)) {
                            return in.lookupInternal(key, (cn.res == 0 ) ? offset : offset + 3, this, startgen, instance);
                        }
                        else {
                            if (setGCAS(cn, cn.renewed(startgen, instance), instance)) {
                                // Tailrec
                                continue;
                            }
                            else {
                                return RESTART;
                            }
                        }
                    }
                    else
                    if (sub instanceof LeafNode<T, V> sn) {
                        // 2) singleton node
                        if (sn.hash == key.getAddress() && Objects.equals(sn.key, key)) {
                            return sn.value;
                        }
                        else {
                            return null;
                        }
                    }
                }
            }
            else
            if (m instanceof TombstoneNode<T, V> tn) {
                // 3) non-live node
                return cleanReadOnly(tn, offset, parent, instance, key, key.getAddress());
            }
            else
            if (m instanceof CollisionAwareNode<T, V> can) {
                // 5) an l-node
                return can.get(key).orElse(null);
            }

            throw new RuntimeException ("Should not happen");
        }
    }

    private NodeWrapper<T, V> checkAndWrap(H3CellId<T> searchKey, H3CellId<T> nodeKey, int resolution, BaseNode<T, V> tn) {
        if (searchKey.getResolution() > nodeKey.getResolution()) {
            return null;
        }
        // compare base cell id and other cell ids till resolution of a search key
        var offset = 64 - 7 - (3 * searchKey.getResolution());
        var nk = (nodeKey.getAddress() << (H3CellId.BASE_OFFSET - 7)) >>> offset;
        var sk = (searchKey.getAddress() << (H3CellId.BASE_OFFSET - 7)) >>> offset;
        if (nk != sk) {
            return null;
        }
        // wrap
        var br = new BranchNode<T, V>(0, 0, new BaseNode[0], new Gen(), 0);
        final int idx;
        final int pos;
        final long flag;
        if (resolution == 0) {
            idx = nodeKey.getBaseCell();
            flag = 1L << idx;
            final long mask = flag - 1;
            if (idx >= 64) {
                pos = Long.bitCount(br.bitmapLow) + Long.bitCount(br.bitmapHigh & mask);
            }
            else {
                pos = Long.bitCount(br.bitmapLow & mask);
            }
        }
        else {
            idx = (int) (nodeKey.getAddress() >>> (64 - H3CellId.BASE_OFFSET - 3)) & 0x7;
            final long bmp = br.bitmapLow;
            flag = 1L << idx;
            final long mask = flag - 1;
            pos = Long.bitCount(bmp & mask);
        }
        return wrap(br.insertedAt(idx, pos, flag, tn, gen));
    }

    /**
     * Looks up the node at specified resolution
     *
     * @return null if no value has been found, RESTART if the operation
     *         wasn't successful, or any other value otherwise
     */
    public Object subTreeInternal(final H3CellId<T> key, int offset, final int resolution, NodeWrapper<T, V> parent, final Gen startgen, final SpatialConcurrentTrieMap<T, V> instance) {
        while (true) {
            BaseNode<T, V> m = getGCAS(instance); // use -Yinline!
            if (m instanceof BranchNode<T, V> cn) {
                // 1) a multinode
                int pos;
                long flag;
                final long bmp;
                if (cn.res == 0) {
                    final int idx = key.getBaseCell();
                    flag = 1L << idx;
                    final long mask = flag - 1;
                    if (idx >= 64) {
                        pos = Long.bitCount(cn.bitmapLow) + Long.bitCount(cn.bitmapHigh & mask);
                        bmp = cn.bitmapHigh;
                    }
                    else {
                        pos = Long.bitCount(cn.bitmapLow & mask);
                        bmp = cn.bitmapLow;
                    }
                }
                else {
                    final long idx = (key.getAddress() >>> (64 - offset - 3)) & 0x7;
                    bmp = cn.bitmapLow;
                    flag = 1L << idx;
                    final long mask = flag - 1;
                    pos = Long.bitCount(bmp & mask);
                }

                if ((bmp & flag) == 0) {
                    return null; // 1a) bitmap shows no binding
                }
                else { // 1b) bitmap contains a value - descend
                    final BaseNode<T, V> sub = cn.array[pos];
                    if (sub instanceof NodeWrapper<T, V> in) {
                        if (resolution == key.getResolution()) {
                            return sub;
                        }
                        else
                        if (instance.isReadOnly() || (startgen == in.gen)) {
                            return in.subTreeInternal(key, (cn.res == 0) ? offset : offset + 3, resolution + 1, this, startgen, instance);
                        }
                        else
                        if (!setGCAS(cn, cn.renewed(startgen, instance), instance)) {
                            return RESTART;
                        }
                    }
                    else
                    if (sub instanceof LeafNode<T, V> sn) {
                        // 2) singleton node check if path prefix the same
                        return checkAndWrap(key, sn.key, resolution, sn);
                    }
                    else {
                        return null;
                    }
                }
            }
            else
            if (m instanceof TombstoneNode<T, V> tn) {
                // 3) non-live node
                var res = cleanReadOnly(tn, offset, parent, instance, key, key.getAddress());
                if (res == null || res instanceof Condition) {
                    return res;
                }
                else {
                    return checkAndWrap(key, tn.key, resolution, tn);
                }
            }
            else
            if (m instanceof CollisionAwareNode<T, V> can) {
                // 5) an l-node
                if (can.listmap.isEmpty()) {
                    return null;
                }
                else {
                    var kv = can.listmap.iterator().next();
                    return checkAndWrap(key, kv.getKey(), resolution, can);
                }
            }

            throw new RuntimeException ("Should not happen");
        }
    }

    private Object cleanReadOnly(final TombstoneNode<T, V> tn, final int offset, final NodeWrapper<T, V> parent, final SpatialConcurrentTrieMap<T, V> ct, H3CellId<T> k, long hc) {
        if (!ct.isReadOnly()) {
            clean(parent, ct, offset - 3);
            return RESTART;
        }
        else {
            if (tn.hash == hc && Objects.equals(tn.key, k)) {
                return tn.value;
            }
            else {
                return null;
            }
        }
    }

    @Override
    public int resolution() {
        return -1;
    }

    public static final class Condition {}

}
