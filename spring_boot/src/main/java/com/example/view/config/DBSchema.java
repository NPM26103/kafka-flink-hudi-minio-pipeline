package com.example.view.config;

import jakarta.persistence.*;

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

    public Long getId() { return id; }

    public Long getStudentId() { return studentId; }
    public void setStudentId(Long studentId) { this.studentId = studentId; }

    public Integer getWeek() { return week; }
    public void setWeek(Integer week) { this.week = week; }

    public Double getStudyHours() { return studyHours; }
    public void setStudyHours(Double studyHours) { this.studyHours = studyHours; }

    public Double getSleepHours() { return sleepHours; }
    public void setSleepHours(Double sleepHours) { this.sleepHours = sleepHours; }

    public Double getStressLevel() { return stressLevel; }
    public void setStressLevel(Double stressLevel) { this.stressLevel = stressLevel; }

    public Double getAttendanceRate() { return attendanceRate; }
    public void setAttendanceRate(Double attendanceRate) { this.attendanceRate = attendanceRate; }

    public Double getScreenTimeHours() { return screenTimeHours; }
    public void setScreenTimeHours(Double screenTimeHours) { this.screenTimeHours = screenTimeHours; }

    public Integer getCaffeineIntake() { return caffeineIntake; }
    public void setCaffeineIntake(Integer caffeineIntake) { this.caffeineIntake = caffeineIntake; }

    public Double getLearningEfficiency() { return learningEfficiency; }
    public void setLearningEfficiency(Double learningEfficiency) { this.learningEfficiency = learningEfficiency; }

    public Double getFatigueIndex() { return fatigueIndex; }
    public void setFatigueIndex(Double fatigueIndex) { this.fatigueIndex = fatigueIndex; }

    public Double getQuizScore() { return quizScore; }
    public void setQuizScore(Double quizScore) { this.quizScore = quizScore; }

    public Double getAssignmentScore() { return assignmentScore; }
    public void setAssignmentScore(Double assignmentScore) { this.assignmentScore = assignmentScore; }

    public Double getPerformanceIndex() { return performanceIndex; }
    public void setPerformanceIndex(Double performanceIndex) { this.performanceIndex = performanceIndex; }

    public String getSource() { return source; }
    public void setSource(String source) { this.source = source; }

    public String getIngestedAt() { return ingestedAt; }
    public void setIngestedAt(String ingestedAt) { this.ingestedAt = ingestedAt; }

    public String getProcessedAt() { return processedAt; }
    public void setProcessedAt(String processedAt) { this.processedAt = processedAt; }

    public Long getProcessedTsMs() { return processedTsMs; }
    public void setProcessedTsMs(Long processedTsMs) { this.processedTsMs = processedTsMs; }
}
