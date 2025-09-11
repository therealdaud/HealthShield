# HealthShield
Turns raw forecasts into individualized heat-risk guidance—compute your personal heat index, see clear risk levels, and get alerts when thresholds are crossed.

HeatShield is a lightweight, serverless pilot that computes a personalized heat index and delivers actionable heat-risk guidance. It blends local weather data with simple user factors (e.g., activity level, clothing, acclimatization) to provide more precise safety signals than raw temperature. The stack is intentionally minimal (API + small datastore + static web UI + optional alerting) to keep ops overhead and cost low while staying easy for a 3-person team to build, run, and iterate.

Core features:
- Personalized heat index & risk levels (beyond plain temp/feels-like)
- Simple web UI + clean API for integrations
- Optional alerts/notifications when thresholds are exceeded
- Minimal, low-cost serverless architecture; easy to deploy and maintain

Why it exists:
Standard heat metrics often miss personal context. HeatShield adds lightweight user inputs to refine risk—helping students, workers, and athletes make safer choices under hot conditions.
