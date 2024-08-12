package com.github.lonelylockley.spatial.ctrie;

import com.github.lonelylockley.spatial.ctrie.nodes.BaseNode;
import com.github.lonelylockley.spatial.ctrie.nodes.NodeWrapper;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

public class SpatialConcurrentTrieMap<T, V> extends AbstractMap<H3CellId<T>, V> implements ConcurrentMap<H3CellId<T>, V>, Serializable {

    private static final AtomicReferenceFieldUpdater<SpatialConcurrentTrieMap, BaseNode> ROOT_UPDATER = AtomicReferenceFieldUpdater.newUpdater(SpatialConcurrentTrieMap.class, BaseNode.class, "root");
    private static final long serialVersionUID = 1L;
    private static final Field READONLY_FIELD;

    static {
        final Field f;
        try {
            f = SpatialConcurrentTrieMap.class.getDeclaredField("readOnly");
        }
        catch (NoSuchFieldException | SecurityException e) {
            throw new ExceptionInInitializerError(e);
        }
        f.setAccessible(true);
        READONLY_FIELD = f;
    }

    private final transient boolean readOnly;
    private transient volatile BaseNode<T, V> root;
    private transient EntrySet entrySet = new EntrySet();

    protected SpatialConcurrentTrieMap(final NodeWrapper<T, V> r, boolean readOnly) {
        this.readOnly = readOnly;
        this.root = r;
    }

    public SpatialConcurrentTrieMap() {
        this(NodeWrapper.newRootNode(), false);
    }

    public final boolean isReadOnly() {
        return readOnly;
    }

    protected final void assertWritable() {
        if (isReadOnly()) {
            throw new IllegalStateException("Attempted to modify a read-only snapshot");
        }
    }

    public final NodeWrapper<T, V> readRoot(boolean abort) {
        return getRootRDCSS(abort);
    }

    public final NodeWrapper<T, V> readRoot() {
        return getRootRDCSS(false);
    }

    public boolean swapRoot(BaseNode<T, V> oldValue, BaseNode<T, V> newValue) {
        assertWritable();
        return ROOT_UPDATER.compareAndSet(this, oldValue, newValue);
    }

    protected final NodeWrapper<T, V> completeRDCSS(final boolean abort) {
        while (true) {
            BaseNode<T, V> v = /* READ */root;
            if (v instanceof NodeWrapper<T, V> nw) {
                return nw;
            }
            else
            if (v instanceof RDCSSDescriptor<T, V> desc) {
                NodeWrapper<T, V> ov = desc.old;
                BaseNode<T, V> exp = desc.expectedmain;
                NodeWrapper<T, V> nv = desc.nv;

                if (abort) {
                    if (swapRoot(desc, ov)) {
                        return ov;
                    }
                    else {
                        // tailrec
                        continue;
                    }
                }
                else {
                    BaseNode<T, V> oldmain = ov.getGCAS(this);
                    if (oldmain == exp) {
                        if (swapRoot(desc, nv)) {
                            desc.setCommitted(true);
                            return nv;
                        }
                        else {
                            // tailrec
                            continue;
                        }
                    }
                    else {
                        if (swapRoot(desc, ov)) {
                            return ov;
                        }
                        else {
                            // tailrec
                            continue;
                        }
                    }
                }
            }

            throw new RuntimeException ("Should not happen");
        }
    }

    protected final boolean swapRootRDCSS(final NodeWrapper<T, V> ov, final BaseNode<T, V> expectedmain, final NodeWrapper<T, V> nv) {
        RDCSSDescriptor<T, V> desc = new RDCSSDescriptor<>(ov, expectedmain, nv);
        if (swapRoot(ov, desc)) {
            completeRDCSS(false);
            return /* READ */desc.isCommitted();
        }
        else {
            return false;
        }
    }

    protected final NodeWrapper<T, V> getRootRDCSS(boolean abort) {
        BaseNode<T, V> r = /* READ */root;
        if (r instanceof NodeWrapper<T, V> nw) {
            return nw;
        }
        else
        if (r instanceof RDCSSDescriptor<T, V>) {
            return completeRDCSS(abort);
        }
        throw new RuntimeException("This should not happen");
    }

    protected final NodeWrapper<T, V> getRootRDCSS() {
        return getRootRDCSS(false);
    }

    private void insertByHash(final H3CellId<T> key, final long hash, final V value) {
        while (true) {
            NodeWrapper<T, V> r = getRootRDCSS();
            if (!r.insertInternal(key, value, hash, H3CellId.BASE_OFFSET, null, r.gen, this)) {
                // tailrec
                continue;
            }
            break;
        }
    }

    protected Optional<V> insertWithConditionByHash(final H3CellId<T> key, final V value, final V cond) {
        while (true) {
            NodeWrapper<T, V> r = getRootRDCSS();
            Optional<V> ret = r.insertWithConditionInternal(key, value, cond, H3CellId.BASE_OFFSET, null, r.gen, this);
            if (ret != null) {
                return ret;
            }
        }
    }

    private V lookupByHash(final H3CellId<T> k) {
        while (true) {
            NodeWrapper<T, V> r = getRootRDCSS();
            Object res = r.lookupInternal(k, H3CellId.BASE_OFFSET, null, r.gen, this);
            if (res != NodeWrapper.RESTART) {
                return (V) res;
            }
        }
    }

    protected final V lookup(H3CellId<T> k) {
        return lookupByHash(k);
    }

    protected Optional<V> removeByHash(final H3CellId<T> key, final V value) {
        while (true) {
            NodeWrapper<T, V> r = getRootRDCSS();
            Optional<V> res = r.removeInternal(key, value, H3CellId.BASE_OFFSET, null, r.gen, this);
            if (res != null) {
                return res;
            }
        }
    }

    public final SpatialConcurrentTrieMap<T, V> subTree(final H3CellId<T> key) {
        while (true) {
            NodeWrapper<T, V> r = getRootRDCSS();
            Object res = r.subTreeInternal(key, H3CellId.BASE_OFFSET, 0, null, r.gen, this);
            if (res != NodeWrapper.RESTART) {
                res = res == null ? NodeWrapper.newRootNode() : res;
                return new SpatialConcurrentTrieMap<>((NodeWrapper<T, V>) res, true);
            }
        }
    }

    // =================================================================================================================
    // PUBLIC API
    // =================================================================================================================

    /**
     * Returns a snapshot of this TrieMap. This operation is lock-free and
     * linearizable.
     *
     * The snapshot is lazily updated - the first time some branch in the
     * snapshot or this TrieMap are accessed, they are rewritten. This means
     * that the work of rebuilding both the snapshot and this TrieMap is
     * distributed across all the threads doing updates or accesses subsequent
     * to the snapshot creation.
     */

    public final SpatialConcurrentTrieMap<T, V> snapshot() {
        while (true) {
            NodeWrapper<T, V> r = getRootRDCSS();
            final BaseNode<T, V> expmain = r.getGCAS(this);
            if (swapRootRDCSS(r, expmain, r.copyToGen(new Gen(), this))) {
                return new SpatialConcurrentTrieMap<>(r.copyToGen(new Gen(), this), readOnly);
            }
        }
    }

    /**
     * Returns a read-only snapshot of this TrieMap. This operation is lock-free
     * and linearizable.
     *
     * The snapshot is lazily updated - the first time some branch of this
     * TrieMap are accessed, it is rewritten. The work of creating the snapshot
     * is thus distributed across subsequent updates and accesses on this
     * TrieMap by all threads. Note that the snapshot itself is never rewritten
     * unlike when calling the `snapshot` method, but the obtained snapshot
     * cannot be modified.
     *
     * This method is used by other methods such as `size` and `iterator`.
     */
    public final SpatialConcurrentTrieMap<T, V> readOnlySnapshot() {
        // Is it a snapshot of a read-only snapshot?
        if(isReadOnly()) {
            return this;
        }
        while (true) {
            NodeWrapper<T, V> r = getRootRDCSS();
            BaseNode<T, V> expmain = r.getGCAS(this);
            if (swapRootRDCSS(r, expmain, r.copyToGen(new Gen(), this))) {
                return new SpatialConcurrentTrieMap<>(r, true);
            }
        }
    }

    /**
     * Return an iterator over a TrieMap.
     *
     * If this is a read-only snapshot, it would return a read-only iterator.
     *
     * If it is the original TrieMap or a non-readonly snapshot, it would return
     * an iterator that would allow for updates.
     *
     * @return
     */
    public Iterator<Entry<H3CellId<T>, V>> iterator() {
        if (isReadOnly()) {
            return readOnlySnapshot().readOnlyIterator();
        }
        else {
            return new TrieMapIterator<>(0, this);
        }
    }

    /***
     * Return an iterator over a TrieMap.
     * This is a read-only iterator.
     *
     * @return
     */
    public Iterator<Entry<H3CellId<T>, V>> readOnlyIterator() {
        if (!isReadOnly()) {
            return readOnlySnapshot().readOnlyIterator();
        }
        else {
            return new TrieMapReadOnlyIterator<>(0, this);
        }
    }

    protected int cachedSize() {
        NodeWrapper<T, V> r = getRootRDCSS();
        return r.cachedSize(this);
    }

    protected final void update(H3CellId<T> k, V v) {
        insertByHash(k, k.getAddress(), v);
    }

    protected Set<Entry<H3CellId<T>, V>> getEntrySet() {
        return entrySet;
    }

    protected SpatialConcurrentTrieMap<T, V> add(H3CellId<T> key, V value) {
        update(key, value);
        return this;
    }

    @Override
    public V putIfAbsent(H3CellId<T> key, V value) {
        return putIfAbsentOpt(key, value).orElse(null);
    }

    public Optional<V> replaceOpt(H3CellId<T> key, V value) {
        assertWritable();
        return insertWithConditionByHash(key, value, (V) NodeWrapper.KEY_PRESENT);
    }

    @Override
    public boolean replace(H3CellId<T> key, V oldValue, V newValue) {
        assertWritable();
        return insertWithConditionByHash(key, newValue, oldValue).isPresent();
    }

    @Override
    public V replace(H3CellId<T> key, V value) {
        return replaceOpt(key, value).orElse(null);
    }

    final public Optional<V> putIfAbsentOpt(H3CellId<T> key, V value) {
        assertWritable();
        return insertWithConditionByHash(key, value, (V) NodeWrapper.KEY_ABSENT);
    }

    @Override
    public int size() {
        if (!isReadOnly()) {
            return readOnlySnapshot().size();
        }
        else {
            return cachedSize();
        }
    }

    protected Optional<V> putOpt(H3CellId<T> key, V value) {
        assertWritable();
        return insertWithConditionByHash(key, value, null);
    }

    @Override
    public V put(H3CellId<T> key, V value) {
        return putOpt(key, value).orElse(null);
    }

    @Override
    public V get(Object key) {
        return lookup((H3CellId<T>) key);
    }

    protected Optional<V> removeOpt(H3CellId<T> key) {
        return removeIfValueExists(key, null);
    }

    protected Optional<V> removeIfValueExists(H3CellId<T> key, V value) {
        assertWritable();
        return removeByHash(key, value);
    }

    @Override
    public V remove(Object key) {
        var cellId = (H3CellId<T>) key;
        return removeOpt(cellId).orElse(null);
    }

    @Override
    public boolean remove(Object key, Object value) {
        return removeIfValueExists((H3CellId<T>) key, (V) value).isPresent();
    }

    @Override
    public boolean containsKey(Object key) {
        return lookup((H3CellId<T>) key) != null;
    }

    @Override
    public Set<Entry<H3CellId<T>, V>> entrySet() {
        return getEntrySet();
    }

    @Override
    final public void clear() {
        while (true) {
            NodeWrapper<T, V> r = getRootRDCSS();
            if (swapRootRDCSS(r, r.getGCAS(this), NodeWrapper.newRootNode())) {
                return;
            }
        }
    }

    // =================================================================================================================
    // =================================================================================================================

    private void readObject(ObjectInputStream inputStream) throws IOException, ClassNotFoundException {
        inputStream.defaultReadObject();
        this.root = NodeWrapper.newRootNode();
        this.entrySet = new EntrySet();

        final boolean ro = inputStream.readBoolean();
        final int size = inputStream.readInt();
        for (int i = 0; i < size; ++i) {
            final var key = (H3CellId<T>) inputStream.readObject();
            final var value = (V) inputStream.readObject();
            add(key, value);
        }

        // Propagate the read-only bit
        try {
            READONLY_FIELD.setBoolean(this, ro);
        }
        catch (IllegalAccessException e) {
            throw new IOException("Failed to set read-only flag", e);
        }
    }

    private void writeObject(ObjectOutputStream outputStream) throws IOException {
        outputStream.defaultWriteObject();

        final Map<H3CellId<T>, V> ro = readOnlySnapshot();
        outputStream.writeBoolean(isReadOnly());
        outputStream.writeInt(ro.size());

        for (Entry<H3CellId<T>, V> e : ro.entrySet()) {
            outputStream.writeObject(e.getKey());
            outputStream.writeObject(e.getValue());
        }
    }

    /**
     * Support for EntrySet operations required by the Map interface
     *
     */
    final class EntrySet extends AbstractSet<Entry<H3CellId<T>, V>> implements Serializable {

        @Override
        public Iterator<Entry<H3CellId<T>, V>> iterator() {
            return SpatialConcurrentTrieMap.this.iterator();
        }

        @Override
        public boolean contains(final Object o) {
            if (!(o instanceof Map.Entry)) {
                return false;
            }
            final Entry<H3CellId<T>, V> e = (Entry<H3CellId<T>, V>) o;
            final H3CellId<T> k = e.getKey();
            final V v = lookup(k);
            return v != null;
        }

        @Override
        public boolean remove(final Object o) {
            if (!(o instanceof Map.Entry)) {
                return false;
            }
            final Entry<H3CellId<T>, V> e = (Entry<H3CellId<T>, V>) o;
            final H3CellId<T> k = e.getKey();
            return null != SpatialConcurrentTrieMap.this.remove(k);
        }

        @Override
        public int size() {
            int size = 0;
            for (final Iterator<?> i = iterator(); i.hasNext(); i.next()) {
                size++;
            }
            return size;
        }

        @Override
        public void clear() {
            SpatialConcurrentTrieMap.this.clear();
        }
    }

}
