"""APScheduler entry — registers 4 cron jobs with DST-safe timezone."""
from __future__ import annotations

import logging
import signal

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from . import state
from .jobs import daily_close_digest, earnings_sweep, price_tick, weekly_sweep

log = logging.getLogger(__name__)


def _safe(fn):
    def wrapper(*a, **kw):
        try:
            fn(*a, **kw)
        except Exception as e:
            log.exception("job %s crashed: %s", fn.__name__, e)
    wrapper.__name__ = fn.__name__
    return wrapper


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    state.init_schema()
    log.info("DCF-50 scheduler starting")

    sch = BlockingScheduler(timezone="America/New_York")

    # price_tick every 10 min, 9:30–16:00 ET, mon-fri
    sch.add_job(
        _safe(price_tick),
        CronTrigger(day_of_week="mon-fri", hour="9-15", minute="*/10",
                    timezone="America/New_York"),
        id="price_tick",
        name="price_tick (10-min)",
    )
    # catch 16:00 exactly as part of the loop
    sch.add_job(
        _safe(price_tick),
        CronTrigger(day_of_week="mon-fri", hour="16", minute="0",
                    timezone="America/New_York"),
        id="price_tick_close",
        name="price_tick (close)",
    )

    # daily_close_digest at 16:15 ET, mon-fri
    sch.add_job(
        _safe(daily_close_digest),
        CronTrigger(day_of_week="mon-fri", hour="16", minute="15",
                    timezone="America/New_York"),
        id="daily_close_digest",
    )

    # weekly_sweep — sun 04:00 UTC (= midnight ET, idle)
    sch.add_job(
        _safe(weekly_sweep),
        CronTrigger(day_of_week="sun", hour="4", minute="0", timezone="UTC"),
        id="weekly_sweep",
    )

    # earnings_sweep — tue/thu 22:00 UTC
    sch.add_job(
        _safe(earnings_sweep),
        CronTrigger(day_of_week="tue,thu", hour="22", minute="0", timezone="UTC"),
        id="earnings_sweep",
    )

    for job in sch.get_jobs():
        log.info("registered: %s  trigger=%s", job.id, job.trigger)

    def _shutdown(*_):
        log.info("shutdown requested")
        sch.shutdown(wait=False)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    sch.start()


if __name__ == "__main__":
    main()
