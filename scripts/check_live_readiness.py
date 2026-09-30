"""Monitoring probe for trader readiness; suitable for a systemd oneshot."""

import json
import sys
import urllib.error
import urllib.request


def main():
    try:
        with urllib.request.urlopen("http://127.0.0.1:8082/readyz", timeout=45) as response:
            report = json.load(response)
    except urllib.error.HTTPError as exc:
        report = json.loads(exc.read())
    except (OSError, ValueError):
        print("Trader readiness unavailable")
        return 1
    print(json.dumps(report, sort_keys=True))
    return 0 if report.get("ready") else 1


if __name__ == "__main__":
    sys.exit(main())
