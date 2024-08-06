package com.github.lonelylockley.spacial.ctrie.nodes;

import com.github.lonelylockley.spacial.ctrie.H3CellId;

import java.util.Map;

public abstract class TerminalNode<T, V> extends BaseNode<T, V> {

    public abstract Map.Entry<H3CellId<T>, V> kvPair();

}
