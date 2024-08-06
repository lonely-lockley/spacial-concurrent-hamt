package com.github.lonelylockley.spacial.ctrie;

import com.github.lonelylockley.spacial.TestBase;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Map;

public class TestBranchNodeFlagCollision extends TestBase<String> {

    @Test
    public void testBranchNodeFlagCollisionRandom() {
        final Map<H3CellId<String>, String> map = new SpacialConcurrentTrieMap<>();
        final var z15169 = generateNonRandomCell(77, "z15169");
        final var z28336 = generateNonRandomCell(109, "z28336");

        Assert.assertNull(map.get(z15169));
        Assert.assertNull(map.get(z28336));

        map.put(z15169, "15169");
        Assert.assertEquals("15169", map.get(z15169));
        Assert.assertNull(map.get(z28336));

        map.put(z28336, "28336");
        Assert.assertEquals("15169", map.get(z15169));
        Assert.assertEquals("28336", map.get(z28336));

        map.remove(z15169);

        Assert.assertNull(map.get(z15169));
        Assert.assertEquals("28336", map.get(z28336));

        map.remove(z28336);

        Assert.assertNull(map.get(z15169));
        Assert.assertNull(map.get(z28336));
    }
}
