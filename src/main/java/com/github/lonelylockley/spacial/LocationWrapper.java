package com.github.lonelylockley.spacial;

import com.github.lonelylockley.spacial.ctrie.H3CellId;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

class LocationWrapper<T, V> {

    private final AtomicReference<H3CellId<T>> cellId = new AtomicReference<>();
    private final AtomicReference<V> value = new AtomicReference<>();

    public LocationWrapper(H3CellId<T> cellId, V value) {
        this.cellId.set(cellId);
        this.value.set(value);
    }

    public void swapCellId(H3CellId<T> fromCellId, H3CellId<T> toCellId) {
        cellId.compareAndSet(fromCellId, toCellId);
    }

    public H3CellId<T> cellId() {
        return cellId.get();
    }

    public V unwrap(H3CellId<T> expectedCellId) {
        if (Objects.equals(expectedCellId, cellId.get())) {
            return value.get();
        }
        else {
            return null;
        }
    }

    public V unwrap() {
        return value.get();
    }

    public void update(V value) {
        this.value.set(value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof LocationWrapper lw) {
            return Objects.equals(lw.value.get(), this.value.get());
        }
        return false;
    }

    @Override
    public int hashCode() {
        return this.value.get().hashCode();
    }
}
