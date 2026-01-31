package com.example.flink;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

public class HttpStudentPerfProcessor implements RecordProcessor {

    private static final String[] REQUIRED = {
            "student_id","week","study_hours","sleep_hours","stress_level","attendance_rate",
            "screen_time_hours","caffeine_intake","learning_efficiency","fatigue_index",
            "quiz_score","assignment_score"
    };

    @Override
    public String name() {
        return "http_student_perf";
    }

    @Override
    public boolean supports(ObjectNode n) {
        return n.hasNonNull("quiz_score") && n.hasNonNull("assignment_score");
    }

    @Override
    public ObjectNode process(ObjectNode n) {
        for (String k : REQUIRED) {
            if (!n.hasNonNull(k)) {
                throw new IllegalArgumentException("Missing: " + k);
            }
        }

        long studentId = RecordUtil.toLong(n, "student_id");
        int week = (int) RecordUtil.toLong(n, "week");

        double studyHours = RecordUtil.toDouble(n, "study_hours");
        double sleepHours = RecordUtil.toDouble(n, "sleep_hours");
        double stress = RecordUtil.toDouble(n, "stress_level");
        double attendance = RecordUtil.toDouble(n, "attendance_rate");
        double screen = RecordUtil.toDouble(n, "screen_time_hours");
        int caffeine = (int) RecordUtil.toLong(n, "caffeine_intake");
        double learnEff = RecordUtil.toDouble(n, "learning_efficiency");
        double fatigue = RecordUtil.toDouble(n, "fatigue_index");
        double quiz = RecordUtil.toDouble(n, "quiz_score");
        double assign = RecordUtil.toDouble(n, "assignment_score");

        n.put("student_id", studentId);
        n.put("week", week);
        n.put("study_hours", studyHours);
        n.put("sleep_hours", sleepHours);
        n.put("stress_level", stress);
        n.put("attendance_rate", attendance);
        n.put("screen_time_hours", screen);
        n.put("caffeine_intake", caffeine);
        n.put("learning_efficiency", learnEff);
        n.put("fatigue_index", fatigue);
        n.put("quiz_score", quiz);
        n.put("assignment_score", assign);

        double perf = 0.5 * quiz + 0.5 * assign + attendance * 10.0 + learnEff * 5.0 - stress * 1.0 - fatigue * 4.0;
        perf = RecordUtil.clamp(perf, 0, 100);

        n.put("performance_index", perf);

        long ts = System.currentTimeMillis();
        RecordUtil.putMeta(n, ts);

        return n;
    }
}
