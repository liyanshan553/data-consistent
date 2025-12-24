package com.smee.binlog.replayer;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.var;

import java.util.ArrayList;
import java.util.regex.Pattern;

@Data
@NoArgsConstructor
public class BinlogWatermark {
    private static final Pattern PATTERN = Pattern.compile("\\d+");
    private int fileId;
    private int position;
    private String raw;

    @Override
    public String toString() {
        return raw;
    }

    public static BinlogWatermark parse(String str) {
        var matcher = PATTERN.matcher(str);
        var arr = new ArrayList<Integer>(2);
        while (matcher.find()) {
            arr.add(Integer.parseInt(matcher.group()));
        }
        var bw = new BinlogWatermark();
        bw.raw = str;
        bw.fileId = arr.get(0);
        bw.position = arr.get(1);
        return bw;
    }
}