#!/usr/bin/env python3
import json
import os
import re
import sys
from pathlib import Path

HOSTS_FILE = Path("/etc/hosts")
CONTROLLER_ENV = os.environ.get("SLURM_CONTROL_HOST", "slurm-ctrl")
SKIP_FQDN = os.environ.get("INCLUDE_FQDN_HOSTS", "0") not in {"1", "true", "yes", "on"}
NAME_REGEX = os.environ.get("HOSTS_NAME_REGEX", "").strip()
CTRL_HINT_RE = re.compile(r"(^|[-_])(ctrl|controller|head|master)([-_]|$)", re.IGNORECASE)


def parse_hosts_file(path: Path):
    hosts = []
    seen = set()

    if not path.exists():
        return hosts

    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.split("#", 1)[0].strip()
        if not line:
            continue

        parts = re.split(r"\s+", line)
        if len(parts) < 2:
            continue

        ip = parts[0].strip()
        names = [x.strip() for x in parts[1:] if x.strip()]
        if not names:
            continue

        # IPv4 only; skip loopback / wildcard entries.
        if ":" in ip:
            continue
        if ip.startswith("127.") or ip == "0.0.0.0":
            continue

        hostname = names[0]
        if hostname in seen:
            continue
        if hostname.endswith("localhost") or hostname.startswith("ip6-"):
            continue
        if SKIP_FQDN and "." in hostname:
            continue
        if NAME_REGEX and not re.search(NAME_REGEX, hostname):
            continue
        seen.add(hostname)

        hosts.append(
            {
                "name": hostname,
                "ip": ip,
                "aliases": names[1:],
            }
        )

    return hosts


def pick_controller(hosts):
    if not hosts:
        return None

    for host in hosts:
        if host["name"] == CONTROLLER_ENV or CONTROLLER_ENV in host["aliases"]:
            return host["name"]

    for host in hosts:
        if CTRL_HINT_RE.search(host["name"]):
            return host["name"]

    return hosts[0]["name"]


def build_inventory():
    hosts = parse_hosts_file(HOSTS_FILE)
    controller = pick_controller(hosts)

    hostvars = {}
    all_hosts = []
    controller_hosts = []
    compute_hosts = []

    for host in hosts:
        name = host["name"]
        all_hosts.append(name)

        hostvars[name] = {
            "ansible_host": host["ip"],
            "host_ip": host["ip"],
            "host_aliases": host["aliases"],
            "slurm_node_name": name,
        }

        if name == controller:
            controller_hosts.append(name)
        else:
            compute_hosts.append(name)

    return {
        "_meta": {"hostvars": hostvars},
        "all": {"hosts": all_hosts},
        "slurm_nodes": {"hosts": all_hosts},
        "slurm_controller": {"hosts": controller_hosts},
        "slurm_build": {"hosts": controller_hosts},
        "slurm_compute": {"hosts": compute_hosts},
    }


def main():
    if len(sys.argv) == 2 and sys.argv[1] == "--list":
        print(json.dumps(build_inventory(), indent=2))
        return

    if len(sys.argv) == 3 and sys.argv[1] == "--host":
        print(json.dumps({}))
        return

    print(json.dumps(build_inventory(), indent=2))


if __name__ == "__main__":
    main()
