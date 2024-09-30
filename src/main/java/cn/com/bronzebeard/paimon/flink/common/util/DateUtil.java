package cn.com.bronzebeard.paimon.flink.common.util;

import cn.com.bronzebeard.paimon.flink.common.exception.GlobalException;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;


/**
 * @author Sherhom
 * @date 2021/6/30 16:22
 */
public class DateUtil {
    public final static Logger log = LoggerFactory.getLogger(DateUtil.class);

    private final static ThreadLocal<Map<String, SimpleDateFormat>> dateFormatCache = new ThreadLocal<>();
    public final static String DEFAULT_FORMAT_STRING = "yyyy-MM-dd HH:mm:ss";
    public final static String DEFAULT_DAY_FORMAT_STRING = "yyyy-MM-dd";
    public final static String default_file_format_string = "yyyy-MM-dd__HH-mm-ss";

    public static String calendar2Str(Calendar calendar) {
        SimpleDateFormat df = getDateFormat(DEFAULT_FORMAT_STRING);// 设置你想要的格式
        String dateStr = df.format(calendar.getTime());
        return dateStr;
    }

    public static String calendar2Str(Calendar calendar, String formatStr) {
        SimpleDateFormat df = getDateFormat(formatStr);// 设置你想要的格式
        String dateStr = df.format(calendar.getTime());
        return dateStr;
    }

    public static Calendar str2Calendar(String dateStr) {
        return str2Calendar(dateStr, DEFAULT_FORMAT_STRING);
    }

    public static Calendar yesterdayCalendar() {
        return getOffsetDay(Calendar.getInstance(), -1);
    }

    public static String localDateTime2Str(LocalDateTime localDateTime) {
        return localDateTime2Str(localDateTime, DEFAULT_FORMAT_STRING);
    }

    public static String localDateTime2Str(LocalDateTime localDateTime, String parttern) {
        return localDateTime.format(DateTimeFormatter.ofPattern(parttern));
    }

    public static Long localDateTime2Timestamp(LocalDateTime localDateTime) {
        return localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
    }

    public static Long localDateTime2Timestamp(LocalDateTime localDateTime, long offsetTime, TimeUnit offsetUnit) {
        return localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli() + offsetUnit.toMillis(offsetTime);
    }

    public static LocalDateTime string2LocalDateTime(String dateStr, String pattern) {
        Date date = str2Date(dateStr, pattern);
        return timestamp2LocalDateTime(date.getTime());
    }

    public static String timestamp2String(Long timestamp) {
        return date2String(new Date(timestamp), DEFAULT_FORMAT_STRING);
    }

    public static String timestamp2String(Long timestamp, String pattern) {
        return date2String(new Date(timestamp), pattern);
    }

    public static LocalDateTime timestamp2LocalDateTime(Long timestamp) {
        Instant instant = Instant.ofEpochMilli(timestamp);
        return LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
    }

    public static Calendar str2Calendar(String dateStr, String format) {
        Date date = str2Date(dateStr, format);

        Calendar calendar = Calendar.getInstance();

        calendar.setTime(date);
        return calendar;
    }

    public static Date str2Date(String dateStr, String formatStr) {
        SimpleDateFormat sdf = getDateFormat(formatStr);
        try {
            return sdf.parse(dateStr);
        } catch (Exception e) {
            throw new GlobalException(e);
        }
    }

    public static String str2str(String srcDateStr, String srcFormat, String tgtFormat) {
        return date2String(str2Date(srcDateStr, srcFormat), tgtFormat);
    }

    public static String date2String(Date d, String formatStr) {
        return getDateFormat(formatStr).format(d);
    }

    public static String date2String(Date d) {
        return date2String(d, DEFAULT_FORMAT_STRING);
    }

    public static SimpleDateFormat getDateFormat(String formatStr) {
//        if(dateFormatCache.get()==null){
//            dateFormatCache.set(new HashMap<>());
//        }
//        if(!dateFormatCache.get().containsKey(formatStr)){
//            SimpleDateFormat sdf=
//                    new SimpleDateFormat(formatStr);
//            dateFormatCache.get().put(formatStr,sdf);
//        }
//        return dateFormatCache.get().get(formatStr);
        return new SimpleDateFormat(formatStr);
    }

    public static Calendar asCalendar(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        return calendar;
    }

    public static Calendar asCalendar(LocalDate localDate) {
        return asCalendar(asDate(localDate));
    }


    public static Calendar asCalendar(LocalDateTime localDateTime) {
        return asCalendar(asDate(localDateTime));
    }

    public static Date asDate(LocalDate localDate) {
        return Date.from(localDate.atStartOfDay().atZone(ZoneId.systemDefault()).toInstant());
    }

    public static Date asDate(LocalDateTime localDateTime) {
        return Date.from(localDateTime.atZone(ZoneId.systemDefault()).toInstant());
    }

    public static LocalDate asLocalDate(Date date) {
        return Instant.ofEpochMilli(date.getTime()).atZone(ZoneId.systemDefault()).toLocalDate();
    }

    public static LocalDateTime asLocalDateTime(Date date) {
        return Instant.ofEpochMilli(date.getTime()).atZone(ZoneId.systemDefault()).toLocalDateTime();
    }

    public static Long now() {
        return Calendar.getInstance().getTimeInMillis();
    }

    public static String now(String pattern) {
        return date2String(new Date(), pattern);
    }

    public static Long lastToNowMs() {
        Calendar positionTime = Calendar.getInstance();
        positionTime.set(Calendar.HOUR_OF_DAY, 0);
        positionTime.set(Calendar.MINUTE, 0);
        positionTime.set(Calendar.SECOND, 0);
        return now() - positionTime.getTimeInMillis();
    }

    public static List<String> checkInRange(List<String> dates, String startDate, String endDate) throws Throwable {
        Set<String> set = dates.stream().collect(Collectors.toSet());
        Calendar sDate = DateUtil.str2Calendar(startDate, DEFAULT_DAY_FORMAT_STRING);
        Calendar eDate = DateUtil.str2Calendar(endDate, DEFAULT_DAY_FORMAT_STRING);
        int d = 0;
        Calendar curDate = sDate, lastDate = (Calendar) curDate.clone();
        lastDate.add(Calendar.DAY_OF_MONTH, -1);
        List<String> miss = new ArrayList<>();
        String startMiss = null;
        while (curDate.before(eDate) || curDate.equals(eDate)) {
            String dateString = DateUtil.calendar2Str(curDate, DEFAULT_DAY_FORMAT_STRING);
            if (!set.contains(dateString)) {
                if (startMiss == null)
                    startMiss = dateString;
            } else {
                if (startMiss != null) {
                    String lastDateString = DateUtil.calendar2Str(lastDate, DEFAULT_DAY_FORMAT_STRING);
                    if (startMiss.equals(lastDateString)) {
                        miss.add(startMiss);
                    } else {
                        miss.add(startMiss + "到" + lastDateString);
                    }
                    startMiss = null;
                }
            }
            d++;
            curDate.add(Calendar.DAY_OF_MONTH, 1);
            lastDate.add(Calendar.DAY_OF_MONTH, 1);
        }
        if (startMiss != null) {
            String lastDateString = DateUtil.calendar2Str(lastDate, DEFAULT_DAY_FORMAT_STRING);
            if (startMiss.equals(lastDateString)) {
                miss.add(startMiss);
            } else {
                miss.add(startMiss + "到" + lastDateString);
            }
        }
        return miss;
    }

    public static <T> List<T> doInDateRange(String startDate, String endDate, Function<String, T> function) {
        return doInDateRange(startDate, endDate, DEFAULT_DAY_FORMAT_STRING, function);
    }

    public static <T> List<T> doInDateRange(String startDate, String endDate, String dateFormat, Function<String, T> function) {
        Calendar sDate = DateUtil.str2Calendar(startDate, DEFAULT_DAY_FORMAT_STRING);
        Calendar eDate = DateUtil.str2Calendar(endDate, DEFAULT_DAY_FORMAT_STRING);
        Calendar curDate = sDate, lastDate = (Calendar) curDate.clone();
        lastDate.add(Calendar.DAY_OF_MONTH, -1);
        List<T> result = new ArrayList<>();
        while (curDate.before(eDate) || curDate.equals(eDate)) {
            String dateString = DateUtil.calendar2Str(curDate, dateFormat);
            result.add(function.apply(dateString));
            curDate.add(Calendar.DAY_OF_MONTH, 1);
            lastDate.add(Calendar.DAY_OF_MONTH, 1);
        }
        return result;
    }

    public static <T> List<T> doInDateIntervalRange(
            String startDate, String endDate,
            int intervalDays, BiFunction<String, String, T> function) throws ParseException {
        return doInDateIntervalRange(startDate, endDate, intervalDays, DEFAULT_DAY_FORMAT_STRING, function);
    }

    public static <T> List<T> doInDateIntervalRange(
            String startDate, String endDate,
            int intervalDays, String dateFormat, BiFunction<String, String, T> function) throws ParseException {
        Calendar sDate = null;
        sDate = DateUtil.str2Calendar(startDate, DateUtil.DEFAULT_DAY_FORMAT_STRING);
        Calendar eDate = DateUtil.str2Calendar(endDate, DateUtil.DEFAULT_DAY_FORMAT_STRING);
        Calendar curDate = sDate, lastDate = (Calendar) curDate.clone();
        List<T> result = new ArrayList<>();
        while (curDate.before(eDate) || curDate.equals(eDate)) {
            String curStartDt = DateUtil.calendar2Str(curDate, dateFormat);
            curDate.add(Calendar.DAY_OF_MONTH, intervalDays);
            Calendar curEndDate = curDate.before(eDate) || curDate.equals(eDate) ? curDate : eDate;
            String curEndDt = DateUtil.calendar2Str(curEndDate, dateFormat);
            result.add(function.apply(curStartDt, curEndDt));
            curDate.add(Calendar.DAY_OF_MONTH, 1);
        }
        return result;
    }

    public static String getOffsetDt(String dt, int offsetDay) {
        return getOffsetDt(dt, offsetDay, DEFAULT_DAY_FORMAT_STRING);
    }

    public static String getOffsetDt(String dt, int offsetDay, String pattern) {
        Calendar today = str2Calendar(dt, pattern);
        Calendar offsetCalendar = getOffsetDay(today, offsetDay);
        return calendar2Str(offsetCalendar, pattern);
    }

    public static Calendar getOffsetDay(Calendar today, int offsetDay) {
        return getOffsetDay(today, offsetDay, 0, 0, 0);
    }

    public static Calendar getOffsetDay(Calendar today, int offsetDay, int hour, int minute, int second) {

        Calendar lastDay = (Calendar) today.clone();
        lastDay.add(Calendar.DAY_OF_MONTH, offsetDay);
        lastDay.set(Calendar.HOUR_OF_DAY, hour);
        lastDay.set(Calendar.MINUTE, minute);
        lastDay.set(Calendar.SECOND, second);
        return lastDay;
    }

    public static String getOffsetDayString(Calendar calendar, int offsetDay) {
        Calendar oneDayBefore = DateUtil.getOffsetDay(calendar, offsetDay);
        String dtValue = DateUtil.calendar2Str(oneDayBefore, DEFAULT_DAY_FORMAT_STRING);
        return dtValue;
    }

    public static String getOffsetDayString(Calendar today, int offsetDay, int hour, int minute, int second, String formatStr) {

        Calendar lastDay = getOffsetDay(today, offsetDay, hour, minute, second);
        return calendar2Str(lastDay, formatStr);
    }

    public static long diffMs(String dt01, String dt02) {
        Date date01 = str2Date(dt01, DEFAULT_DAY_FORMAT_STRING);
        Date date02 = str2Date(dt02, DEFAULT_DAY_FORMAT_STRING);
        long interMillis = date01.getTime() - date02.getTime();
        return interMillis;
    }

    public static int diffDay(String dt01, String dt02) {
        long interMillis = diffMs(dt01, dt02);
        return (int) (TimeUnit.MILLISECONDS.toDays(interMillis));
    }

    public static Pair<String, String> msToStartDtAndEndDt(long ttl) {
        return msToStartDtAndEndDt(ttl, 0);
    }

    public static Pair<String, String> msToStartDtAndEndDt(long ttl, int offsetEndDay) {
        Calendar today = Calendar.getInstance();
        return msToStartDtAndEndDt(today, ttl, offsetEndDay);
    }


    public static Pair<String, String> msToStartDtAndEndDt(Calendar today, long ttl, int offsetEndDay) {
        int ttlDay = (int) TimeUnit.DAYS.convert(ttl, TimeUnit.MILLISECONDS);
        Calendar afterOneDay = DateUtil.getOffsetDay(today, offsetEndDay);
        Calendar lastNDays = DateUtil.getOffsetDay(today, -ttlDay);
        try {
            String startDt = DateUtil.calendar2Str(lastNDays, DEFAULT_DAY_FORMAT_STRING);
            String endDt = DateUtil.calendar2Str(afterOneDay, DEFAULT_DAY_FORMAT_STRING);
            return Pair.of(startDt, endDt);
        } catch (Exception e) {
            log.error(e.getMessage());
            return null;
        }
    }

    public static boolean testDateFormat(String dateString, String pattern) {
        try {
            DateUtil.str2Date(dateString, pattern);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /*
     * 0: Sunday
     * 1: Friday
     * */
    public static int getWeek(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        int weekIndex = calendar.get(Calendar.DAY_OF_WEEK) - 1;

        return weekIndex >= 0 ? weekIndex : 0;
    }

    public static Timestamp autoConvertStringToTimestamp(String value) throws ParseException {
        if (value == null) {
            return null;
        }
        Long timeInMillis;
        if (NumberUtils.isNumber(value) && value.length() == 10) {
            timeInMillis = Long.parseLong(value + "000");
        } else if (NumberUtils.isNumber(value) && value.length() == 13) {
            timeInMillis = Long.parseLong(value);
        } else if (value.matches("\\d{4}-\\d{2}-\\d{2}\\s{1}\\d{2}:\\d{2}:\\d{2}")) {
            timeInMillis = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(value).getTime();
        } else if (value.matches("\\d{4}-\\d{2}-\\d{2}\\s{1}\\d{2}:\\d{2}")) {
            timeInMillis = new SimpleDateFormat("yyyy-MM-dd HH:mm").parse(value).getTime();
        } else if (value.matches("\\d{4}-\\d{2}-\\d{2}\\s{1}\\d{2}")) {
            timeInMillis = new SimpleDateFormat("yyyy-MM-dd HH").parse(value).getTime();
        } else if (value.matches("\\d{4}-\\d{2}-\\d{2}")) {
            timeInMillis = new SimpleDateFormat("yyyy-MM-dd").parse(value).getTime();
        } else if (value.matches("\\d{8}")) {
            timeInMillis = new SimpleDateFormat("yyyyMMdd").parse(value).getTime();
        } else if (value.matches("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.\\d{3}Z")) {
            timeInMillis = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS").parse(value).getTime();
        } else if (value.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3}")) {
            timeInMillis = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS").parse(value).getTime();
        } else {
            throw new GlobalException("can not format value [" + value + "] to timestamp");
        }
        return new Timestamp(timeInMillis);
    }

    public static boolean canAutoConvertStringToTimestamp(String value) {
        try {
            autoConvertStringToTimestamp(value);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public static String autoFormatDateTime(String value, String format) throws ParseException {

        if (format.contains("`T`")) {
            format = format.replaceAll("`T`", "'T'");
        }
        Timestamp timestamp = DateUtil.autoConvertStringToTimestamp(value);
        if (timestamp == null) {
            return null;
        }
        SimpleDateFormat sf = new SimpleDateFormat(format);
        Date date = new Date(timestamp.getTime());
        return sf.format(date);
    }
}
