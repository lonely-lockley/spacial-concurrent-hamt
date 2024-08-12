package com.github.lonelylockley.spatial.ctrie.nodes;

import java.io.Serializable;

public interface Node<T, V> extends Serializable {

    int resolution();

}
