package com.example.view.config;

import jakarta.persistence.*;
import lombok.*;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Entity
@Table(name = "student_performance")
public class DBSchema {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private Long studentId;
    private Integer week;

    private Double studyHours;
    private Double sleepHours;
    private Double stressLevel;

    private Double attendanceRate;
    private Double screenTimeHours;
    private Integer caffeineIntake;

    private Double learningEfficiency;
    private Double fatigueIndex;

    private Double quizScore;
    private Double assignmentScore;
    private Double performanceIndex;

    private String source;
    private String ingestedAt;
    private String processedAt;
    private Long processedTsMs;
}
