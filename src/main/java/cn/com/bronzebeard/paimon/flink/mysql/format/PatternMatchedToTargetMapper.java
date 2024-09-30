package cn.com.bronzebeard.paimon.flink.mysql.format;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author sherhomhuang
 * @date 2024/05/11 21:24
 * @description
 */
public class PatternMatchedToTargetMapper<T> implements Serializable {
    private final Map<String, List<T>> expectPatternToTarget;

    private final Map<String, List<String>> matchedToPatternList;

    public PatternMatchedToTargetMapper(Map<String, List<T>> expectPatternToTarget) {
        this.expectPatternToTarget = expectPatternToTarget;
        this.matchedToPatternList = new HashMap<>();
    }

    public List<T> getTargetByMatched(String matched) {
        List<String> patternList = matchedToPatternList.get(matched);
        if (CollectionUtils.isEmpty(patternList)) {
            return Collections.emptyList();
        }
        return patternList.stream().map(pattern -> expectPatternToTarget.get(pattern))
                .filter(CollectionUtils::isNotEmpty)
                .flatMap(List::stream)
                .collect(Collectors.toList())
                ;
    }

    public List<Pair<String, List<T>>> getPatternAndTargetByMatched(String matched) {
        return expectPatternToTarget.entrySet().stream()
                .filter(e -> matched.matches(e.getKey()))
                .map(e -> Pair.of(e.getKey(), e.getValue()))
                .collect(Collectors.toList());
    }

    public boolean addNewMatchedToMapper(String matched) {
        List<Pair<String, List<T>>> matchedPatternAndTgtTablePair = getPatternAndTargetByMatched(matched);
        if (CollectionUtils.isEmpty(matchedPatternAndTgtTablePair)) {
            return false;
        }
        matchedToPatternList.put(matched, matchedPatternAndTgtTablePair.stream().map(Pair::getKey).collect(Collectors.toList()));
        return true;
    }

}
