package cn.com.bronzebeard.paimon.flink;

import cn.com.bronzebeard.paimon.flink.mysql.format.PatternMatchedToTargetMapper;
import cn.com.bronzebeard.paimon.flink.mysql.format.entity.DataPair;
import cn.com.bronzebeard.paimon.flink.mysql.format.entity.DataPairBuilder;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.paimon.catalog.Identifier;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author sherhomhuang
 * @date 2024/09/13 11:50
 * @description
 */
public class PatternMatchedToTargetMapperTest {
    @Test
    public void test01() {
        String srcDb = "testDb";
        List<String> tableInDb = Lists.newArrayList("t_1", "t_2", "ta", "t_10");
        List<String> newTableList = Lists.newArrayList("t_3", "t_4", "t_11", "tb");
        List<DataPair> dataPairList = new ArrayList<>();
        dataPairList.add(DataPairBuilder.aDataPair()
                .srcDatabasePattern(srcDb)
                .srcTablePattern("t_1")
                .tgtDatabase("paimonDb")
                .tgtTable("pt1")
                .build()
        );
        dataPairList.add(DataPairBuilder.aDataPair()
                .srcDatabasePattern(srcDb)
                .srcTablePattern("t_(\\d+)$")
                .tgtDatabase("paimonDb")
                .tgtTable("pt_all")
                .build()
        );
        dataPairList.add(DataPairBuilder.aDataPair()
                .srcDatabasePattern(srcDb)
                .srcTablePattern("ta")
                .tgtDatabase("paimonDb")
                .tgtTable("pt_a")
                .build()
        );
        Map<String, List<Identifier>> srcPatternToTargetTable = dataPairList.stream()
                .collect(Collectors.groupingBy(DataPair::getFullSrcTableNamePattern)).entrySet().stream()
                .map(e -> Pair.of(e.getKey()
                        , e.getValue().stream()
                                .map(dp -> new Identifier(dp.getTgtDatabase(), dp.getTgtTable()))
                                .collect(Collectors.toList())))
                .collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
        PatternMatchedToTargetMapper<Identifier> referenceTableNameConverter = new PatternMatchedToTargetMapper<>(srcPatternToTargetTable);
        tableInDb.forEach(tableName -> {
            referenceTableNameConverter.addNewMatchedToMapper(srcDb + "." + tableName);
        });
        newTableList.forEach(tableName -> {
            referenceTableNameConverter.addNewMatchedToMapper(srcDb + "." + tableName);
        });

        CollectionUtils.union(tableInDb, newTableList).forEach(tableName -> {
            System.out.println(String.format("%s -> %s", tableName, referenceTableNameConverter.getTargetByMatched(srcDb + "." + tableName)));
        });
    }
}
