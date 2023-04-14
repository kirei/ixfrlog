"""Log and watch DNS IXFR (RFC 1995)"""

import argparse
import json
import logging
import os
import socket
import sys
import tempfile
from dataclasses import dataclass
from typing import IO, Optional

import dns.query
import dns.rdataclass
import dns.rdatatype
import dns.zone
import paho.mqtt.client as mqtt

TEMPFILE = "ixfrlog."

MQTT_TOPIC = "ixfrlog"

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


@dataclass(frozen=True)
class IXFRresult(object):
    changes: int = 0
    serial: Optional[int] = None


def ixfrlog(
    nameserver: str,
    zone: str,
    serial: int,
    fp: Optional[IO] = None,
    mqttc: Optional[mqtt.Client] = None,
) -> IXFRresult:
    nameserver = socket.gethostbyname(nameserver)
    messages = dns.query.xfr(
        where=nameserver, zone=zone, rdtype=dns.rdatatype.IXFR, serial=serial
    )
    origin = dns.name.from_text(zone)
    ixfr_found = False
    first_soa = None
    second_soa = None
    changes = 0
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

            changes += 1

            log_owner = name2str(rrset.name, origin)
            log_serial = serial
            log_action = "add" if action_add else "del"

            if fp:
                log_entry = {
                    "serial": log_serial,
                    "deleted": not action_add,
                    "name": log_owner,
                    "ttl": int(rrset.ttl),
                    "rdclass": dns.rdataclass.to_text(rrset.rdclass),
                    "rdtype": dns.rdatatype.to_text(rrset.rdtype),
                    "rdata": [
                        rr.to_text(origin=origin, relativize=False) for rr in rrset
                    ],
                    "text": rrset.to_text(origin=origin, relativize=False),
                }
                fp.write(json.dumps(log_entry) + "\n")

            if mqttc:
                message = {
                    "serial": log_serial,
                    "deleted": not action_add,
                    "name": log_owner,
                    "ttl": int(rrset.ttl),
                    "rdclass": dns.rdataclass.to_text(rrset.rdclass),
                    "rdtype": dns.rdatatype.to_text(rrset.rdtype),
                    "rdata": [
                        rr.to_text(origin=origin, relativize=False) for rr in rrset
                    ],
                }
                mqttc.publish(f"{MQTT_TOPIC}/{zone}", json.dumps(message))

            for text in rrset.to_text(origin=origin, relativize=False).split("\n"):
                logging.debug(f"{log_serial} {log_action.upper()} {text}")

    return IXFRresult(serial=serial, changes=changes)


def main():
    """Main function"""

    parser = argparse.ArgumentParser(description="IXFR Log")

    parser.add_argument(
        "--state",
        metavar="filename",
        help="State file",
        default="ixfrlog.state",
        required=False,
    )
    parser.add_argument(
        "--mqtt",
        metavar="URL",
        help="MQTT server URL",
        required=False,
    )
    parser.add_argument(
        "--nameserver",
        metavar="address",
        help="Name server",
        required=False,
    )
    parser.add_argument("--log", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--debug", action="store_true", help="Enable debugging")
    parser.add_argument("zone", nargs="*", help="Zone to track")

    args = parser.parse_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    logger = logging.getLogger(__name__)
    exit_status = 0

    try:
        with open(args.state, "rt") as fp:
            state = json.loads(fp.read())
    except FileNotFoundError:
        state = {}
        for zone in args.zone:
            state[zone] = {"nameserver": args.nameserver}

    if args.mqtt:
        mqttc = mqtt.Client()
        mqttc.connect(args.mqtt)
    else:
        mqttc = None

    for zone, config in state.items():
        last_serial = config.get("serial", 0)

        if args.log:
            output_fp = tempfile.NamedTemporaryFile(
                mode="wt", dir=".", prefix=TEMPFILE, suffix=".tmp", delete=False
            )
        else:
            output_fp = None

        try:
            res = ixfrlog(
                nameserver=config["nameserver"],
                zone=zone,
                serial=last_serial,
                fp=output_fp,
                mqttc=mqttc,
            )
            new_serial = res.serial
            if last_serial == new_serial:
                logger.info("No changes for zone %s serial %d", zone, new_serial)
            else:
                logger.info(
                    "Logged %d changes for zone %s serial %d",
                    res.changes,
                    zone,
                    new_serial,
                )
        except FailedIXFR as exc:
            new_serial = None
            state[zone]["serial"] = exc.serial
            logger.warning("IXFR not available, fast forward to serial %d", exc.serial)

        if output_fp:
            output_size = os.fstat(output_fp.fileno()).st_size
            output_fp.close()

        updated = new_serial is not None and last_serial != new_serial

        if updated and res:
            state[zone]["serial"] = new_serial

        if output_fp:
            if updated and output_size > 0:
                zone = zone.rstrip(".")
                filename = f"{zone}-{new_serial}.log"
                state[zone]["filename"] = filename
                os.rename(output_fp.name, filename)
            else:
                os.unlink(output_fp.name)
                exit_status = -1

    if mqttc:
        mqttc.disconnect()

    with open(args.state, "wt") as fp:
        fp.write(json.dumps(state, indent=4))

    sys.exit(exit_status)


if __name__ == "__main__":
    main()
