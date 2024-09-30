package cn.com.bronzebeard.paimon.flink.common.util;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;

/**
 * @author Sherhom
 * @date 2020/9/4 11:34
 */
public class ExceptionUtil {
    public static String toStackTraceString(Throwable e) {
        if (e == null)
            return "null";
        StringWriter stringWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stringWriter));
        return stringWriter.toString();
    }
    public static Throwable getBasicThrowable(Throwable caused,Throwable defaultCause) {
        return visitThrowable(caused,(e, last) -> (e.orElse(last.orElse(defaultCause))));
    }
    public static Throwable visitThrowable(Throwable cause,
                                           BiFunction<Optional<Throwable>, Optional<Throwable>,Throwable> actionToEnd) {
        return visitThrowable(cause, (e,last)->false, (e, last) -> e.get(), actionToEnd);
    }
    public static Throwable visitThrowable(Throwable cause,
                                                     BiPredicate<Optional<Throwable>, Optional<Throwable>> stopCondition,
                                                     BiFunction<Optional<Throwable>, Optional<Throwable>,Throwable> actionToEnd) {
        return visitThrowable(cause, stopCondition, (e, last) -> e.get(), actionToEnd);
    }

    public static <T> T visitThrowable(Throwable cause,
                                       BiPredicate<Optional<Throwable>, Optional<Throwable>> stopCondition,
                                       BiFunction<Optional<Throwable>, Optional<Throwable>, T> dealThrowable,
                                       BiFunction<Optional<Throwable>, Optional<Throwable>, T> actionToEnd) {
        Set<Throwable> visitedThrowable = new HashSet<>();
        Optional<Throwable> lastCause = null;
        do {
            if (stopCondition.test(Optional.ofNullable(cause), lastCause)) {
                return dealThrowable.apply(Optional.ofNullable(cause), lastCause);
            }
            visitedThrowable.add(cause);
            lastCause = Optional.ofNullable(cause);
        } while ((cause = cause.getCause()) != null && !visitedThrowable.contains(cause));
        return actionToEnd.apply(Optional.ofNullable(cause), lastCause);
    }
}
