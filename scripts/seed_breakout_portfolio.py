#!/usr/bin/env python3
"""Import a historical occupancy observation into a separate, order-incapable book."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

from lnmarkets_bot.portfolio.store import PortfolioStore, read_overview


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--database", type=Path, default=Path("runs/portfolio-shadow.sqlite"))
    args = parser.parse_args()
    if args.database.resolve().is_relative_to(Path("/var/lib")):
        parser.error("this development seed command cannot write production state")
    snapshot = json.loads(args.snapshot.read_text())
    store = PortfolioStore(args.database)
    instance_id = "btc_close_range_v1_shadow"
    store.register(instance_id, "paper", snapshot["rules_sha256"])
    store.import_shadow_observation(instance_id, snapshot, now=datetime.now(UTC))
    overview = read_overview(args.database)
    print(
        json.dumps(
            {
                "database": str(args.database),
                "order_capability": False,
                "strategies": overview["strategies"],
                "occupancy": [
                    {
                        "instance_id": seed["instance_id"],
                        "observed_through": seed["close_ts"],
                        "parent_occupied": seed["parent_occupied"],
                        "campaign": seed["observation"].get("active_hypothetical_stack"),
                    }
                    for seed in overview["seeds"]
                ],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
