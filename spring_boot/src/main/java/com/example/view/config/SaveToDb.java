package com.example.view.config;

import com.example.common.Json;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.stereotype.Service;

@Service
public class SaveToDb {

    private final Repository repo;

    public SaveToDb(Repository repo) {
        this.repo = repo;
    }

    public void save(String message) {
        ObjectNode n = Json.parseObj(message);

        DBSchema e = new DBSchema();

        e.setStudentId(toLong(n, "student_id"));
        e.setWeek(toInt(n, "week"));

        e.setStudyHours(toDouble(n, "study_hours"));
        e.setSleepHours(toDouble(n, "sleep_hours"));
        e.setStressLevel(toDouble(n, "stress_level"));

        e.setAttendanceRate(toDouble(n, "attendance_rate"));
        e.setScreenTimeHours(toDouble(n, "screen_time_hours"));
        e.setCaffeineIntake(toInt(n, "caffeine_intake"));

        e.setLearningEfficiency(toDouble(n, "learning_efficiency"));
        e.setFatigueIndex(toDouble(n, "fatigue_index"));

        e.setQuizScore(toDouble(n, "quiz_score"));
        e.setAssignmentScore(toDouble(n, "assignment_score"));
        e.setPerformanceIndex(toDouble(n, "performance_index"));

        e.setSource(text(n, "_source"));
        e.setIngestedAt(text(n, "_ingested_at"));
        e.setProcessedAt(text(n, "_processed_at"));

        Long ts = n.hasNonNull("processed_ts_ms") ? n.get("processed_ts_ms").asLong()
                : (n.hasNonNull("ts_ms") ? n.get("ts_ms").asLong() : null);
        e.setProcessedTsMs(ts);

        repo.save(e);
    }

    private static String text(ObjectNode n, String k) {
        return n.hasNonNull(k) ? n.get(k).asText() : null;
    }

    private static Long toLong(ObjectNode n, String k) {
        if (!n.hasNonNull(k)) return null;
        return n.get(k).isNumber() ? n.get(k).asLong() : Long.parseLong(n.get(k).asText().trim());
    }

    private static Integer toInt(ObjectNode n, String k) {
        Long v = toLong(n, k);
        return v == null ? null : v.intValue();
    }

    private static Double toDouble(ObjectNode n, String k) {
        if (!n.hasNonNull(k)) return null;
        return n.get(k).isNumber() ? n.get(k).asDouble() : Double.parseDouble(n.get(k).asText().trim());
    }
}