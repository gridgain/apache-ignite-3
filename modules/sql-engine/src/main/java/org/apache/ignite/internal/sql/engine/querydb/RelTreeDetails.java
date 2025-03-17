package org.apache.ignite.internal.sql.engine.querydb;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;

final class RelTreeDetails implements Iterable<RelNodeDetails> {

    private final Int2ObjectMap<List<RelNodeDetails>> levels = new Int2ObjectOpenHashMap<>();

    void addNode(IgniteRel rel, int level, int childIndex, Integer parentId) {
        List<RelNodeDetails> items = levels.computeIfAbsent(level, k -> new ArrayList<>());
        items.add(new RelNodeDetails(rel, level, childIndex, items.size(), parentId));
    }

    public int depth() {
        return levels.size();
    }

    public List<RelNodeDetails> level(int num) {
        return levels.get(num);
    }

    @Override
    public Iterator<RelNodeDetails> iterator() {
        return new Iterator<>() {
            int currentLevel = 0;
            int idx = 0;

            @Override
            public boolean hasNext() {
                if (currentLevel < depth() - 1 || currentLevel == depth() - 1 && idx < levels.get(currentLevel).size()) {
                    return true;
                } else {
                    return false;
                }
            }

            @Override
            public RelNodeDetails next() {
                if (!hasNext()) {
                    return null;
                }

                List<RelNodeDetails> level = levels.get(currentLevel);
                if (idx == level.size()) {
                    idx = 0;
                    currentLevel += 1;
                    level = levels.get(currentLevel);
                }

                RelNodeDetails node = level.get(idx);
                idx += 1;
                return node;
            }
        };
    }
}
