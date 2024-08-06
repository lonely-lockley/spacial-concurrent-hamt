package com.github.lonelylockley.spacial.ctrie;

import com.github.lonelylockley.spacial.TestBase;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Map;

public class TestBranchNodeInsertionIncorrectOrder extends TestBase<String> {

    @Test
    public void testCNodeInsertionIncorrectOrder () {
        final Map<H3CellId<String>, String> map = new SpacialConcurrentTrieMap<>();
        final var z3884 = generateNonRandomCell(646748598416010371L, "z3884");
        final var z4266 = generateNonRandomCell(647857218979916116L, "z4266");
        map.put(z3884, "3884");
        Assert.assertEquals("3884", map.get(z3884));
        
        map.put(z4266, "4266");
        Assert.assertEquals("3884", map.get(z3884));
        Assert.assertEquals("4266", map.get(z4266));
    }
}
