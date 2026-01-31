package com.example.view.config;

import com.example.common.Json;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class SaveToDb {

    private final Repository repo;

    public void save(String message) {
        final ObjectNode n;
        try {
            n = Json.parseObj(message);
        } catch (Exception e) {
            log.warn("[VIEW] invalid json, skip. err={}", e.getMessage());
            return;
        }

        DBSchema e = DBSchema.builder()
                .studentId(toLong(n, "student_id"))
                .week(toInt(n, "week"))

                .studyHours(toDouble(n, "study_hours"))
                .sleepHours(toDouble(n, "sleep_hours"))
                .stressLevel(toDouble(n, "stress_level"))

                .attendanceRate(toDouble(n, "attendance_rate"))
                .screenTimeHours(toDouble(n, "screen_time_hours"))
                .caffeineIntake(toInt(n, "caffeine_intake"))

                .learningEfficiency(toDouble(n, "learning_efficiency"))
                .fatigueIndex(toDouble(n, "fatigue_index"))

                .quizScore(toDouble(n, "quiz_score"))
                .assignmentScore(toDouble(n, "assignment_score"))
                .performanceIndex(toDouble(n, "performance_index"))

                .source(text(n, "_source"))
                .ingestedAt(text(n, "_ingested_at"))
                .processedAt(text(n, "_processed_at"))
                .processedTsMs(extractTs(n))
                .build();

        repo.save(e);
    }

    private static Long extractTs(ObjectNode n) {
        if (n.hasNonNull("processed_ts_ms")) return n.get("processed_ts_ms").asLong();
        if (n.hasNonNull("ts_ms")) return n.get("ts_ms").asLong();
        return null;
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
