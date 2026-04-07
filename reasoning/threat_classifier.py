from reasoning.llm_reasoner import ThreatAssessment


# Minimum confidence required to take each action automatically
# Below these thresholds the action is downgraded to the next safer level
CONFIDENCE_THRESHOLDS = {
    "block":   0.80,
    "alert":   0.65,
    "review":  0.50,
    "monitor": 0.0,
}


def classify_response(assessment: ThreatAssessment) -> dict:
    """
    Maps a ThreatAssessment to a final action decision.

    Applies confidence thresholds — a block with 0.4 confidence
    is downgraded to review. This prevents irreversible actions
    when the system is uncertain.

    Returns a routing decision with the final action and reason.
    """
    recommended = assessment.recommended_action
    confidence  = assessment.confidence
    threshold   = CONFIDENCE_THRESHOLDS.get(recommended, 0.0)

    if confidence >= threshold:
        final_action = recommended
        downgraded   = False
    else:
        # Downgrade to the next safer action
        downgrade_map = {
            "block":  "review",
            "alert":  "review",
            "review": "monitor",
        }
        final_action = downgrade_map.get(recommended, "monitor")
        downgraded   = True
        print(f"[classifier] Downgraded {recommended} → {final_action} "
              f"(confidence {confidence} below threshold {threshold})")

    return {
        "final_action":     final_action,
        "original_action":  recommended,
        "threat_level":     assessment.threat_level,
        "confidence":       confidence,
        "downgraded":       downgraded,
        "requires_human":   assessment.requires_human_review,
        "assessment_text":  assessment.assessment,
        "key_risk_factors": assessment.key_risk_factors,
    }
