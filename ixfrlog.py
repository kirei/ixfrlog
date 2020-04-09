"""Log and watch DNS IXFR (RFC 1995)"""

import argparse
import json
import logging
import os
import sys
import tempfile
from typing import Optional
from typing.io import IO

import dns.query
import dns.rdataclass
import dns.rdatatype
import dns.zone

TEMPFILE = "ixfrlog."

IGNORE_RDATATYPES = [
    dns.rdatatype.SOA,
    dns.rdatatype.NSEC,
    dns.rdatatype.NSEC3,
    dns.rdatatype.RRSIG,
]


class FailedIXFR(Exception):
    def __init__(self, serial):
        self.serial = serial


def name2str(name: dns.name, origin: str) -> str:
    n = str(name)
    if n == "@":
        return str(origin)
    return ".".join([n, str(origin)])


def ixfrlog(nameserver: str, zone: str, serial: int, file: IO) -> Optional[int]:
    messages = dns.query.xfr(
        where=nameserver, zone=zone, rdtype=dns.rdatatype.IXFR, serial=serial
    )
    origin = dns.name.from_text(zone)
    ixfr_found = False
    first_soa = None
    second_soa = None
    for m in messages:
        for rrset in m.answer:
            if rrset.rdtype == dns.rdatatype.SOA:
                logging.debug(f"SOA %s", rrset)
                if first_soa is None:
                    first_soa = rrset[0]
                else:
                    if second_soa is None:
                        second_soa = rrset[0]
                    last_serial = serial
                    serial = rrset[0].serial
                    if serial == last_serial:
                        logging.debug(f"state=DEL")
                        ixfr_found = True
                        action_add = False
                    else:
                        logging.debug(f"state=ADD")
                        ixfr_found = True
                        action_add = True
            if rrset.rdtype in IGNORE_RDATATYPES:
                continue
            if not ixfr_found:
                raise FailedIXFR(serial=first_soa.serial)

            log_owner = name2str(rrset.name, origin)
            log_serial = serial
            log_action = "add" if action_add else "del"

            log_entry = {
                "serial": log_serial,
                "deleted": not action_add,
                "name": log_owner,
                "ttl": int(rrset.ttl),
                "rdclass": dns.rdataclass.to_text(rrset.rdclass),
                "rdtype": dns.rdatatype.to_text(rrset.rdtype),
                "rdata": [rr.to_text(origin=origin, relativize=False) for rr in rrset],
                "text": rrset.to_text(origin=origin, relativize=False),
            }

            file.write(json.dumps(log_entry) + "\n")

            for text in rrset.to_text(origin=origin, relativize=False).split("\n"):
                logging.debug(f"{log_serial} {log_action.upper()} {text}")

    return serial


def main():
    """Main function"""

    parser = argparse.ArgumentParser(description="IXFR Log")

    parser.add_argument(
        "--state",
        dest="state",
        metavar="filename",
        help="State file",
        default="ixfrlog.state",
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

    logger = logging.getLogger(__name__)
    exit_status = 0

    with open(args.state, "rt") as file:
        state = json.loads(file.read())

    for zone, config in state.items():
        last_serial = config.get("serial", 0)
        output_file = tempfile.NamedTemporaryFile(
            mode="wt", dir=".", prefix=TEMPFILE, suffix=".tmp", delete=False
        )

        try:
            new_serial = ixfrlog(
                nameserver=config["nameserver"],
                zone=zone,
                serial=last_serial,
                file=output_file,
            )
            if last_serial == new_serial:
                logger.info("No changes for zone=%s serial=%d", zone, new_serial)
            else:
                logger.info("Logged changes for zone=%s serial=%d", zone, new_serial)
        except FailedIXFR as exc:
            new_serial = None
            state[zone]["serial"] = exc.serial
            logger.warning("IXFR not available, fast forward to serial %d", exc.serial)

        output_size = os.fstat(output_file.fileno()).st_size
        output_file.close()

        if new_serial is not None and last_serial != new_serial and output_size > 0:
            state[zone]["serial"] = new_serial
            zone = zone.rstrip(".")
            os.rename(output_file.name, f"{zone}-{new_serial}.log")
        else:
            os.unlink(output_file.name)
            exit_status = -1

    with open(args.state, "wt") as file:
        file.write(json.dumps(state, indent=4))

    sys.exit(exit_status)


if __name__ == "__main__":
    main()
