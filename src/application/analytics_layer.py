"""
CareChain Analytics Layer - Federated Learning & Healthcare Insights
Single file implementation for cloud-based healthcare analytics
"""

import asyncio
import json
import time
from typing import Dict, Any, List, Optional
from dataclasses import dataclass
import statistics
import random

@dataclass
class AnalyticsInsight:
    """Healthcare analytics insight"""
    insight_id: str
    insight_type: str
    data_sources: List[str]
    results: Dict[str, Any]
    confidence_score: float
    timestamp: float
    affected_patients: List[str]

class FederatedLearningCoordinator:
    """Coordinates federated learning across healthcare institutions"""
    
    def __init__(self):
        self.models: Dict[str, Dict[str, Any]] = {}
        self.participating_hospitals: List[str] = []
        self.global_model_weights: Dict[str, Any] = {}
        self.training_rounds = 0
        
    async def register_hospital(self, hospital_id: str) -> Dict[str, Any]:
        """Register hospital for federated learning"""
        if hospital_id not in self.participating_hospitals:
            self.participating_hospitals.append(hospital_id)
        
        return {
            "status": "registered",
            "hospital_id": hospital_id,
            "total_participants": len(self.participating_hospitals)
        }
    
    async def create_global_model(self, model_id: str, model_type: str) -> Dict[str, Any]:
        """Create new federated learning model"""
        self.models[model_id] = {
            "model_id": model_id,
            "model_type": model_type,  # diagnosis, prediction, risk_assessment
            "created_at": time.time(),
            "training_rounds": 0,
            "participating_hospitals": [],
            "accuracy_metrics": {},
            "privacy_budget": 1.0,
            "status": "initialized"
        }
        
        return {
            "status": "created",
            "model_id": model_id,
            "model_type": model_type
        }
    
    async def coordinate_training_round(self, model_id: str) -> Dict[str, Any]:
        """Coordinate federated learning training round"""
        if model_id not in self.models:
            return {"status": "model_not_found"}
        
        model = self.models[model_id]
        
        # Simulate federated learning round
        training_results = []
        
        for hospital_id in self.participating_hospitals:
            # Simulate local training at hospital
            local_result = await self._simulate_local_training(hospital_id, model_id)
            training_results.append(local_result)
        
        # Aggregate results (simplified)
        aggregated_weights = self._aggregate_model_weights(training_results)
        
        # Update global model
        model["training_rounds"] += 1
        model["accuracy_metrics"] = self._calculate_accuracy_metrics(training_results)
        model["last_updated"] = time.time()
        
        self.global_model_weights[model_id] = aggregated_weights
        
        return {
            "status": "round_completed",
            "model_id": model_id,
            "round": model["training_rounds"],
            "participants": len(training_results),
            "accuracy": model["accuracy_metrics"].get("global_accuracy", 0)
        }
    
    async def _simulate_local_training(self, hospital_id: str, model_id: str) -> Dict[str, Any]:
        """Simulate local model training at hospital"""
        # Simulate training with differential privacy
        return {
            "hospital_id": hospital_id,
            "model_id": model_id,
            "local_accuracy": random.uniform(0.75, 0.95),
            "data_samples": random.randint(100, 1000),
            "training_time": random.uniform(30, 120),  # seconds
            "privacy_epsilon": 0.1,  # Differential privacy parameter
            "model_weights": {
                f"layer_{i}": random.uniform(-1, 1) for i in range(5)
            }
        }
    
    def _aggregate_model_weights(self, training_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Aggregate model weights from all hospitals"""
        # Simplified federated averaging
        aggregated = {}
        
        if training_results:
            # Weight by number of samples
            total_samples = sum(result["data_samples"] for result in training_results)
            
            for layer in ["layer_0", "layer_1", "layer_2", "layer_3", "layer_4"]:
                weighted_sum = sum(
                    result["model_weights"][layer] * result["data_samples"]
                    for result in training_results
                )
                aggregated[layer] = weighted_sum / total_samples
        
        return aggregated
    
    def _calculate_accuracy_metrics(self, training_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calculate global accuracy metrics"""
        if not training_results:
            return {}
        
        accuracies = [result["local_accuracy"] for result in training_results]
        
        return {
            "global_accuracy": statistics.mean(accuracies),
            "accuracy_std": statistics.stdev(accuracies) if len(accuracies) > 1 else 0,
            "min_accuracy": min(accuracies),
            "max_accuracy": max(accuracies),
            "participating_hospitals": len(training_results)
        }

class HealthcareAnalytics:
    """Healthcare data analytics and insights"""
    
    def __init__(self):
        self.analytics_cache: Dict[str, Any] = {}
        self.insight_history: List[AnalyticsInsight] = []
        
    async def analyze_population_health(self, patient_data: List[Dict[str, Any]]) -> AnalyticsInsight:
        """Analyze population health trends"""
        insight_id = f"pop_health_{int(time.time())}"
        
        # Extract key metrics
        metrics = self._extract_health_metrics(patient_data)
        
        # Analyze trends
        trends = self._analyze_health_trends(metrics)
        
        # Generate insights
        insights = self._generate_population_insights(trends)
        
        result = AnalyticsInsight(
            insight_id=insight_id,
            insight_type="population_health",
            data_sources=[f"patient_{i}" for i in range(len(patient_data))],
            results={
                "metrics": metrics,
                "trends": trends,
                "insights": insights,
                "risk_factors": self._identify_risk_factors(patient_data)
            },
            confidence_score=random.uniform(0.8, 0.95),
            timestamp=time.time(),
            affected_patients=[data.get("patient_id", "") for data in patient_data]
        )
        
        self.insight_history.append(result)
        return result
    
    def _extract_health_metrics(self, patient_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Extract key health metrics from patient data"""
        metrics = {
            "total_patients": len(patient_data),
            "avg_heart_rate": 0,
            "avg_blood_pressure": 0,
            "avg_glucose": 0,
            "temperature_readings": []
        }
        
        heart_rates = []
        blood_pressures = []
        glucose_levels = []
        
        for patient in patient_data:
            sensor_data = patient.get("sensor_data", {})
            
            if "heart_rate" in sensor_data:
                heart_rates.append(sensor_data["heart_rate"])
            if "blood_pressure" in sensor_data:
                blood_pressures.append(sensor_data["blood_pressure"])
            if "blood_glucose" in sensor_data:
                glucose_levels.append(sensor_data["blood_glucose"])
        
        if heart_rates:
            metrics["avg_heart_rate"] = statistics.mean(heart_rates)
        if blood_pressures:
            metrics["avg_blood_pressure"] = statistics.mean(blood_pressures)
        if glucose_levels:
            metrics["avg_glucose"] = statistics.mean(glucose_levels)
        
        return metrics
    
    def _analyze_health_trends(self, metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze health trends from metrics"""
        trends = {
            "heart_rate_trend": "stable",
            "blood_pressure_trend": "stable",
            "glucose_trend": "stable",
            "overall_trend": "stable"
        }
        
        # Simplified trend analysis
        if metrics.get("avg_heart_rate", 0) > 90:
            trends["heart_rate_trend"] = "elevated"
        elif metrics.get("avg_heart_rate", 0) < 60:
            trends["heart_rate_trend"] = "low"
        
        if metrics.get("avg_blood_pressure", 0) > 130:
            trends["blood_pressure_trend"] = "elevated"
        
        if metrics.get("avg_glucose", 0) > 126:
            trends["glucose_trend"] = "elevated"
        
        # Overall trend
        concerning_trends = sum(1 for trend in trends.values() if trend == "elevated")
        if concerning_trends >= 2:
            trends["overall_trend"] = "concerning"
        
        return trends
    
    def _generate_population_insights(self, trends: Dict[str, Any]) -> List[str]:
        """Generate actionable insights from trends"""
        insights = []
        
        if trends["heart_rate_trend"] == "elevated":
            insights.append("Population shows elevated heart rate patterns - consider cardiovascular screening")
        
        if trends["blood_pressure_trend"] == "elevated":
            insights.append("Hypertension prevalence detected - implement blood pressure monitoring program")
        
        if trends["glucose_trend"] == "elevated":
            insights.append("Elevated glucose levels observed - diabetes prevention program recommended")
        
        if trends["overall_trend"] == "concerning":
            insights.append("Multiple health indicators show concerning trends - comprehensive health intervention needed")
        
        if not insights:
            insights.append("Population health metrics within normal ranges - continue monitoring")
        
        return insights
    
    def _identify_risk_factors(self, patient_data: List[Dict[str, Any]]) -> List[str]:
        """Identify common risk factors"""
        risk_factors = []
        
        # Analyze patterns in patient data
        high_risk_patients = 0
        for patient in patient_data:
            sensor_data = patient.get("sensor_data", {})
            
            risk_score = 0
            if sensor_data.get("heart_rate", 0) > 100:
                risk_score += 1
            if sensor_data.get("blood_pressure", 0) > 140:
                risk_score += 1
            if sensor_data.get("blood_glucose", 0) > 140:
                risk_score += 1
            
            if risk_score >= 2:
                high_risk_patients += 1
        
        risk_percentage = (high_risk_patients / len(patient_data)) * 100 if patient_data else 0
        
        if risk_percentage > 30:
            risk_factors.append("High cardiovascular risk prevalence")
        if risk_percentage > 20:
            risk_factors.append("Metabolic syndrome indicators")
        
        return risk_factors
    
    async def detect_outbreak_patterns(self, sensor_data: List[Dict[str, Any]]) -> AnalyticsInsight:
        """Detect potential disease outbreak patterns"""
        insight_id = f"outbreak_detection_{int(time.time())}"
        
        # Analyze temperature patterns for fever detection
        fever_cases = 0
        total_readings = len(sensor_data)
        
        for reading in sensor_data:
            if reading.get("sensor_type") == "body_temperature":
                temp = reading.get("value", 98.6)
                if temp > 100.4:  # Fever threshold
                    fever_cases += 1
        
        fever_rate = (fever_cases / total_readings) * 100 if total_readings > 0 else 0
        
        # Detect outbreak
        outbreak_detected = fever_rate > 15  # More than 15% fever rate
        
        result = AnalyticsInsight(
            insight_id=insight_id,
            insight_type="outbreak_detection",
            data_sources=[reading.get("device_id", "") for reading in sensor_data],
            results={
                "fever_cases": fever_cases,
                "total_readings": total_readings,
                "fever_rate": fever_rate,
                "outbreak_detected": outbreak_detected,
                "alert_level": "high" if outbreak_detected else "low",
                "recommendations": self._get_outbreak_recommendations(outbreak_detected, fever_rate)
            },
            confidence_score=random.uniform(0.85, 0.98),
            timestamp=time.time(),
            affected_patients=list(set(reading.get("patient_id", "") for reading in sensor_data))
        )
        
        self.insight_history.append(result)
        return result
    
    def _get_outbreak_recommendations(self, outbreak_detected: bool, fever_rate: float) -> List[str]:
        """Get recommendations based on outbreak analysis"""
        if outbreak_detected:
            return [
                "Immediate isolation protocols for affected patients",
                "Contact tracing for high-risk exposures",
                "Enhanced monitoring of asymptomatic individuals",
                "Alert public health authorities",
                "Implement additional hygiene measures"
            ]
        elif fever_rate > 5:
            return [
                "Continue monitoring fever patterns",
                "Prepare outbreak response protocols",
                "Increase testing capacity"
            ]
        else:
            return ["Continue routine monitoring"]

class AnalyticsOrchestrator:
    """Orchestrates analytics workflows and federated learning"""
    
    def __init__(self):
        self.fl_coordinator = FederatedLearningCoordinator()
        self.healthcare_analytics = HealthcareAnalytics()
        self.active_analyses: Dict[str, Any] = {}
        
    async def process_blockchain_data(self, blockchain_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process data from blockchain layer"""
        processed_data = blockchain_data.get("processed_data", {})
        
        # Store for analytics
        analysis_id = f"analysis_{int(time.time())}"
        self.active_analyses[analysis_id] = {
            "data": processed_data,
            "timestamp": time.time(),
            "status": "processing"
        }
        
        # Perform analytics
        if processed_data.get("confidence_score", 0) > 0.8:
            # High confidence data - use for population health analysis
            patient_data = [{"patient_id": processed_data.get("patient_id", ""), 
                           "sensor_data": processed_data}]
            
            insight = await self.healthcare_analytics.analyze_population_health(patient_data)
            
            # Send insights to presentation layer
            await self._send_insights_to_presentation(insight)
        
        return {
            "status": "processed",
            "analysis_id": analysis_id,
            "timestamp": time.time()
        }
    
    async def _send_insights_to_presentation(self, insight: AnalyticsInsight):
        """Send analytics insights to presentation layer"""
        from src.infrastructure.messaging.event_bus import layer_communicator
        
        await layer_communicator.publish_analytics_insight(
            analysis_id=insight.insight_id,
            insights={
                "insight_type": insight.insight_type,
                "results": insight.results,
                "confidence_score": insight.confidence_score,
                "affected_patients": insight.affected_patients
            }
        )
    
    def get_analytics_status(self) -> Dict[str, Any]:
        """Get analytics layer status"""
        return {
            "active_analyses": len(self.active_analyses),
            "total_insights": len(self.healthcare_analytics.insight_history),
            "federated_models": len(self.fl_coordinator.models),
            "participating_hospitals": len(self.fl_coordinator.participating_hospitals),
            "status": "operational"
        }

# Global analytics orchestrator
analytics_orchestrator = AnalyticsOrchestrator()

# Initialize analytics layer
async def initialize_analytics_layer():
    """Initialize analytics layer with default models"""
    # Register sample hospitals
    await analytics_orchestrator.fl_coordinator.register_hospital("hospital_001")
    await analytics_orchestrator.fl_coordinator.register_hospital("hospital_002")
    
    # Create sample federated learning models
    await analytics_orchestrator.fl_coordinator.create_global_model("heart_disease_prediction", "diagnosis")
    await analytics_orchestrator.fl_coordinator.create_global_model("diabetes_risk_assessment", "risk_assessment")
    
    print("Analytics layer initialized with federated learning models")
