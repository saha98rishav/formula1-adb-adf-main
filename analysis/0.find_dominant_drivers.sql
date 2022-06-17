-- Databricks notebook source
SELECT driver_name,
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points
    FROM f1_presentation.calculated_race_results
GROUP BY driver_name
ORDER BY SUM(calculated_points) DESC;