"""Watch changes in DNX IXFR log"""

import argparse
import json
import logging
from collections import defaultdict


def change2rr(change: dict):
    rdata = change["rdata"]
    if isinstance(rdata, list) and len(rdata) == 1:
        rdata = rdata[0]
    return (f'{change["name"]} {change["rdclass"]} {change["rdtype"]} {rdata}')


def main():
    """Main function"""

    parser = argparse.ArgumentParser(description="Report changes in IXFR log")

    parser.add_argument(
        "--log", dest="log", metavar="filename", help="IXFR log file", required=True,
    )
    parser.add_argument(
        "--watch",
        dest="watchlist",
        metavar="filename",
        help="Watch list file",
        default="watchlist.json",
        required=False,
    )
    parser.add_argument(
        "--report",
        dest="report",
        metavar="filename",
        help="Report result file",
        default="report.json",
        required=False,
    )
    parser.add_argument(
        "--debug", dest="debug", action="store_true", help="Enable debugging"
    )

    args = parser.parse_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    with open(args.watchlist, "rt") as input_file:
        watchlist = json.loads(input_file.read())

    domains_per_recipient = defaultdict(list)
    for domain, recipients in watchlist.items():
        for r in recipients:
            domains_per_recipient[r].append(domain)

    data_del = defaultdict(set)
    data_add = defaultdict(set)
    modified_names = set()

    with open(args.log, "rt") as input_file:
        for r in input_file.readlines():
            change = json.loads(r)
            name = change["name"]

            if name not in watchlist:
                continue

            modified_names.add(name)
            v = change2rr(change)

            if change["deleted"]:
                data_del[name].add(v)
                data_add[name].discard(v)
            else:
                data_add[name].add(v)
                data_del[name].discard(v)

    for recipient, domains in domains_per_recipient.items():
        print(f"Report for {recipient}:")
        for d in domains:
            for rr in sorted(data_del[d]):
                print(f"  Deleted: {rr}")
            for rr in sorted(data_add[d]):
                print(f"  Added:   {rr}")
,ma

if __name__ == "__main__":
    main()
