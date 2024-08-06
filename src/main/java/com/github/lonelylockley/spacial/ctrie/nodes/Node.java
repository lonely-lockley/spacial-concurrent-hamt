package com.github.lonelylockley.spacial.ctrie.nodes;

import java.io.Serializable;

public interface Node<T, V> extends Serializable {

    int resolution();

}
