package com.github.lonelylockley.spatial;

import com.github.lonelylockley.spatial.ctrie.H3CellId;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiFunction;

public interface Tracker<T, V> {

     /**
      * Register a new business entity
      * @param cellId H3 cell address in string format
      * @param businessEntityId a unique identifier of a business entity stored in value to tell one from another in case of
      *                         location collision
      * @param value a business entity to store
      */
     V startTracking(String cellId, T businessEntityId, V value);

     boolean isTracking(T businessEntityId);

     H3CellId<T> getLocation(T businessEntityId);

     /**
      * Change a cell of a business object
      * @param fromCellId original cell id where object is stored now
      * @param toCellId a new cell id
      */
     boolean updateLocation(T businessEntityId, H3CellId<T> toCellId);

     /**
      * Remove a registered object from a current location
      * @param cellId cell id locating business object
      */
     V finishTracking(T businessEntityId);

     void updateValue(T businessEntityId, V value);

     V getValue(T businessEntityId);

     Collection<Entry<H3CellId<T>, V>> getAllWithinRing(String cellId, int resolution, int range);
     Collection<Entry<H3CellId<T>, V>> getAllWithinCircle(String cellId, int resolution, int range);
     Collection<Entry<H3CellId<T>, V>> findAround(String cellId, BiFunction<H3CellId<T>, V, Boolean> predicate, int resolution, int range, int limit);
     Map<T, H3CellId<T>> getAllBusinessEntitiesLocations();

}
