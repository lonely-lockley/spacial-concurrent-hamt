package com.github.lonelylockley.spatial.ctrie;

import com.github.lonelylockley.spatial.ctrie.nodes.BaseNode;
import com.github.lonelylockley.spatial.ctrie.nodes.BranchNode;
import com.github.lonelylockley.spatial.ctrie.nodes.CollisionAwareNode;
import com.github.lonelylockley.spatial.ctrie.nodes.LeafNode;
import com.github.lonelylockley.spatial.ctrie.nodes.NodeWrapper;
import com.github.lonelylockley.spatial.ctrie.nodes.TerminalNode;
import com.github.lonelylockley.spatial.ctrie.nodes.TombstoneNode;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class TrieMapIterator <T, V> implements Iterator<Map.Entry<H3CellId<T>, V>> {
    private int level;
    protected SpatialConcurrentTrieMap<T, V> ct;
    private final boolean mustInit;
    private BaseNode[][] stack = new BaseNode[H3CellId.MAX_DEPTH][];
    private int[] stackpos = new int[H3CellId.MAX_DEPTH];
    private int depth = -1;
    private Iterator<Map.Entry<H3CellId<T>, V>> subiter = null;
    private BaseNode<T, V> current = null;
    private Map.Entry<H3CellId<T>, V> lastReturned = null;

    TrieMapIterator(int level, final SpatialConcurrentTrieMap<T, V> ct, boolean mustInit) {
        this.level = level;
        this.ct = ct;
        this.mustInit = mustInit;
        if (this.mustInit) {
            initialize();
        }
    }

    TrieMapIterator(int level, SpatialConcurrentTrieMap<T, V> ct) {
        this(level, ct, true);
    }

    public boolean hasNext() {
        return (current != null) || (subiter != null);
    }

    public Map.Entry<H3CellId<T>, V> next() {
        if (hasNext()) {
            Map.Entry<H3CellId<T>, V> r = null;
            if (subiter != null) {
                r = subiter.next();
                checkSubiter();
            }
            else {
                if (current instanceof TerminalNode<T, V> kv) {
                    r = kv.kvPair();
                }
                advance();
            }

            lastReturned = r;
            return r != null ? nextEntry(r) : null;
        }
        else {
            return null;
        }
    }

    Map.Entry<H3CellId<T>, V> nextEntry(final Map.Entry<H3CellId<T>, V> rr) {
        return new Map.Entry<>() {
            private V updated = null;

            @Override
            public H3CellId<T> getKey() {
                return rr.getKey();
            }

            @Override
            public V getValue() {
                return (updated == null) ? rr.getValue() : updated;
            }

            @Override
            public V setValue(V value) {
                updated = value;
                return ct.replace(getKey(), value);
            }
        };
    }

    private void readin(NodeWrapper<T, V> in) {
        BaseNode<T, V> m = in.getGCAS(ct);
        if (m instanceof BranchNode<T, V> cn) {
            depth += 1;
            stack[depth] = cn.array;
            stackpos[depth] = -1;
            advance();
        }
        else
        if (m instanceof TombstoneNode<T, V> tm) {
            current = tm;
        }
        else
        if (m instanceof CollisionAwareNode<T, V> can) {
            subiter = can.listmap.iterator();
            checkSubiter();
        }
        else
        if (m == null) {
            current = null;
        }
    }

    // @inline
    private void checkSubiter() {
        if (!subiter.hasNext()) {
            subiter = null;
            advance();
        }
    }

    // @inline
    void initialize() {
        NodeWrapper<T, V> r = ct.getRootRDCSS();
        readin(r);
    }

    void advance () {
        if (depth >= 0) {
            int npos = stackpos[depth] + 1;
            if (npos < stack[depth].length) {
                stackpos[depth] = npos;
                BaseNode<T, V> elem = stack[depth][npos];
                if (elem instanceof LeafNode<T, V> ln) {
                    current = ln;
                }
                else
                if (elem instanceof NodeWrapper<T, V> nw) {
                    readin(nw);
                }
            }
            else {
                depth -= 1;
                advance();
            }
        }
        else {
            current = null;
        }
    }

    protected TrieMapIterator<T, V> newIterator(int lev, SpatialConcurrentTrieMap<T, V> ct, boolean mustInit) {
        return new TrieMapIterator<>(lev, ct, mustInit);
    }

    protected void dupTo(TrieMapIterator<T, V> it) {
        it.level = this.level;
        it.ct = this.ct;
        it.depth = this.depth;
        it.current = this.current;

        // these need a deep copy
        System.arraycopy(this.stack, 0, it.stack, 0, 7);
        System.arraycopy(this.stackpos, 0, it.stackpos, 0, 7);

        // this one needs to be evaluated
        if (this.subiter == null) {
            it.subiter = null;
        }
        else {
            List<Map.Entry<H3CellId<T>, V>> lst = toList(this.subiter);
            this.subiter = lst.iterator();
            it.subiter = lst.iterator();
        }
    }

    private List<Map.Entry<H3CellId<T>, V>> toList(Iterator<Map.Entry<H3CellId<T>, V>> it) {
        ArrayList<Map.Entry<H3CellId<T>, V>> list = new ArrayList<> ();
        while (it.hasNext()) {
            list.add(it.next());
        }
        return list;
    }

    @Override
    public void remove() {
        if (lastReturned != null) {
            ct.remove(lastReturned.getKey());
            lastReturned = null;
        }
        else {
            throw new IllegalStateException();
        }
    }

}
