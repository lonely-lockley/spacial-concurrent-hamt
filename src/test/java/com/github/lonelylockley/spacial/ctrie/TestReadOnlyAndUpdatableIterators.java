package com.github.lonelylockley.spacial.ctrie;

import com.github.lonelylockley.spacial.TestBase;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/***
 * 
 * Test that read-only iterators do not allow for any updates.
 * Test that non read-only iterators allow for updates. 
 *
 */
public class TestReadOnlyAndUpdatableIterators extends TestBase<String> {
    private static SpacialConcurrentTrieMap<String, Integer> sctm;
    private static final int MAP_SIZE = 200;
    
    @BeforeClass
    public static void setUp() {
        var generator = new TestReadOnlyAndUpdatableIterators();
        sctm = new SpacialConcurrentTrieMap<>();
        for (int j = 0; j < MAP_SIZE; j++) {
            Assert.assertNull(sctm.put(generator.generateRandomCell(String.valueOf(j)), j));
        }                
    }
    
    @Test
    public void testReadOnlyIterator () {
        var it = sctm.readOnlyIterator();
        try {
            it.next().setValue(0);
            // It should have generated an exception, because it is a read-only iterator
            Assert.assertFalse(true);
        }
        catch (Exception e) {}
        try {
            it.remove();
            // It should have generated an exception, because it is a read-only iterator
            Assert.assertFalse(true);
        }
        catch (Exception e) {}
    }

    @Test
    public void testReadOnlySnapshotReadOnlyIterator () {
        var roSnapshot = sctm.readOnlySnapshot();
        var it = roSnapshot.readOnlyIterator ();
        try {
            it.next().setValue (0);
            // It should have generated an exception, because it is a read-only iterator
            Assert.assertFalse(true);
        }
        catch (Exception e) {}
        try {
            it.remove ();
            // It should have generated an exception, because it is a read-only iterator
            Assert.assertFalse(true);
        }
        catch (Exception e) {}
    }

    @Test
    public void testReadOnlySnapshotIterator () {
        var roSnapshot = sctm.readOnlySnapshot();
        var it = roSnapshot.iterator();
        try {
            it.next().setValue(0);
            // It should have generated an exception, because it is a read-only iterator
            Assert.assertFalse(true);
        }
        catch (Exception e) {}
        try {
            it.remove ();
            // It should have generated an exception, because it is a read-only iterator
            Assert.assertFalse(true);
        }
        catch (Exception e) {}
    }

    @Test
    public void testIterator() {
        var it = sctm.iterator();
        try {
            it.next().setValue(0);
        }
        catch (Exception e) {
            // It should not have generated an exception, because it is a non read-only iterator
            Assert.assertFalse(true);
        }
        
        try {
            it.remove();
        }
        catch (Exception e) {
            // It should not have generated an exception, because it is a non read-only iterator
            Assert.assertFalse(true);
        }
        
        // All changes are done on the original map
        Assert.assertEquals(MAP_SIZE - 1, sctm.size());
    }

    @Test
    public void testSnapshotIterator() {
        var snapshot = sctm.snapshot();
        var it = snapshot.iterator();
        try {
            it.next().setValue(0);
        }
        catch (Exception e) {
            // It should not have generated an exception, because it is a non read-only iterator
            Assert.assertFalse(true);
        }
        try {
            it.remove ();
        }
        catch (Exception e) {
            // It should not have generated an exception, because it is a non read-only iterator
            Assert.assertFalse(true);
        }

        // All changes are done on the snapshot, not on the original map
        // Map size should remain unchanged
        Assert.assertEquals(MAP_SIZE - 1, sctm.size());
        // snapshot size was changed
        Assert.assertEquals(MAP_SIZE - 2, snapshot.size());
    }
}
