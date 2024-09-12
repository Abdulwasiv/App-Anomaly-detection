from datetime import timedelta
from celery.schedules import schedule

CELERY_IMPORTS = "chaos_genius.jobs"
CELERY_TASK_RESULT_EXPIRES = 30
CELERY_TIMEZONE = "UTC"

CELERY_ACCEPT_CONTENT = ["json", "msgpack", "yaml"]
CELERY_TASK_SERIALIZER = "json"
CELERY_RESULT_SERIALIZER = "json"

CELERYBEAT_SCHEDULE = {
    "anomaly-scheduler": {
        "task": "chaos_genius.jobs.analytics_scheduler.scheduler_wrapper",
        "schedule": schedule(timedelta(minutes=1)),  # Runs every 1 minute
        "args": (),
    },
    "alert-digest-daily-scheduler": {
        "task": "chaos_genius.jobs.alert_tasks.alert_digest_daily_scheduler",
        "schedule": schedule(timedelta(minutes=1)),  # Runs every 1 minute
        "args": (),
    },
    "alerts-daily": {
        "task": "chaos_genius.jobs.alert_tasks.check_event_alerts",
        "schedule": schedule(timedelta(minutes=1)),  # Runs every 1 minute
        "args": ("daily",),
    },
    "alerts-hourly": {
        "task": "chaos_genius.jobs.alert_tasks.check_event_alerts",
        "schedule": schedule(timedelta(minutes=1)),  # Runs every 1 minute
        "args": ("hourly",),
    },
    "metadata-prefetch-daily": {
        "task": "chaos_genius.jobs.metadata_prefetch.metadata_prefetch_daily_scheduler",
        "schedule": schedule(timedelta(minutes=1)),  # Runs every 1 minute
        "args": (),
    },
}

CELERY_ROUTES = {
    "chaos_genius.jobs.anomaly_tasks.*": {"queue": "anomaly-rca"},
    "chaos_genius.jobs.analytics_scheduler.*": {"queue": "anomaly-rca"},
    "chaos_genius.jobs.alert_tasks.*": {"queue": "alerts"},
    "chaos_genius.jobs.metadata_prefetch.*": {"queue": "alerts"},
}

# Configuration
class Config:
    enable_utc = True
