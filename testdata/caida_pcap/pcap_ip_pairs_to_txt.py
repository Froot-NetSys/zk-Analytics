#!/usr/bin/env python3
"""
Read one or more PCAP files and write per-packet (src_ip, dst_ip) and packet_length
to per-key text files (one file per (src_ip, dst_ip) pair).

Output format (TSV, no header by default):
  <src_ip_u32>\t<dst_ip_u32>\t<packet_length>\n

Output files are named: <idx>_<src_ip_u32>_<dst_ip_u32>.txt where idx is a stable
u64 derived from (src_ip_u32, dst_ip_u32).

This script is dependency-free (standard library only) and supports classic PCAP
with common link types (IPv6 packets are skipped):
  - DLT_EN10MB (Ethernet)
  - DLT_RAW (Raw IP)
  - DLT_LINUX_SLL (Linux cooked capture)
"""

from __future__ import annotations

import argparse
import gzip
import os
import struct
import sys
import tempfile
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO, Iterator, Optional, Tuple


PCAP_MAGIC_LE = 0xA1B2C3D4
PCAP_MAGIC_BE = 0xD4C3B2A1
PCAP_MAGIC_NS_LE = 0xA1B23C4D
PCAP_MAGIC_NS_BE = 0x4D3CB2A1

DLT_NULL = 0
DLT_EN10MB = 1
DLT_RAW = 101
DLT_LINUX_SLL = 113

ETHERTYPE_IPV4 = 0x0800
ETHERTYPE_IPV6 = 0x86DD
ETHERTYPE_VLAN_8021Q = 0x8100
ETHERTYPE_VLAN_8021AD = 0x88A8


@dataclass(frozen=True)
class PcapGlobalHeader:
    endian: str  # "<" or ">"
    linktype: int


def _open_maybe_gzip(path: Path) -> BinaryIO:
    if path.suffix == ".gz":
        return gzip.open(path, "rb")
    return path.open("rb")


def _read_exact(f: BinaryIO, n: int) -> bytes:
    b = f.read(n)
    if len(b) != n:
        raise EOFError
    return b


def read_pcap_global_header(f: BinaryIO) -> PcapGlobalHeader:
    magic_bytes = _read_exact(f, 4)
    magic_le = struct.unpack("<I", magic_bytes)[0]
    magic_be = struct.unpack(">I", magic_bytes)[0]

    if magic_le in (PCAP_MAGIC_LE, PCAP_MAGIC_NS_LE):
        endian = "<"
    elif magic_be in (PCAP_MAGIC_BE, PCAP_MAGIC_NS_BE):
        endian = ">"
    else:
        raise ValueError(f"Unsupported PCAP magic: 0x{magic_le:08x}/0x{magic_be:08x}")

    # We don't need most fields; still read them to advance file cursor correctly.
    # struct pcap_hdr_s {
    #   u16 v_major, u16 v_minor, i32 thiszone, u32 sigfigs, u32 snaplen, u32 network
    # }
    gh = _read_exact(f, 20)
    (v_major, v_minor, _thiszone, _sigfigs, _snaplen, network) = struct.unpack(
        f"{endian}HHiiii", gh
    )
    if v_major < 2:
        raise ValueError(f"Unexpected PCAP version: {v_major}.{v_minor}")
    return PcapGlobalHeader(endian=endian, linktype=network)


def iter_pcap_packets(
    f: BinaryIO, gh: PcapGlobalHeader
) -> Iterator[Tuple[int, int, int, bytes]]:
    # struct pcaprec_hdr_s { u32 ts_sec, u32 ts_usec, u32 incl_len, u32 orig_len }
    hdr_struct = struct.Struct(f"{gh.endian}IIII")
    while True:
        hdr = f.read(hdr_struct.size)
        if not hdr:
            return
        if len(hdr) != hdr_struct.size:
            raise EOFError("Truncated per-packet header")
        ts_sec, ts_subsec, incl_len, orig_len = hdr_struct.unpack(hdr)
        data = _read_exact(f, incl_len)
        yield ts_sec, incl_len, orig_len, data


def parse_ip_pair_from_frame(linktype: int, frame: bytes) -> Optional[Tuple[int, int]]:
    """
    Returns (src_ip_u32, dst_ip_u32) if frame contains an IPv4 packet; otherwise None.
    """
    if linktype == DLT_RAW:
        ip_payload = frame
    elif linktype == DLT_EN10MB:
        if len(frame) < 14:
            return None
        ethertype = struct.unpack("!H", frame[12:14])[0]
        offset = 14
        # VLAN tags: 802.1Q (4 bytes) and 802.1ad (QinQ)
        while ethertype in (ETHERTYPE_VLAN_8021Q, ETHERTYPE_VLAN_8021AD):
            if len(frame) < offset + 4:
                return None
            ethertype = struct.unpack("!H", frame[offset + 2 : offset + 4])[0]
            offset += 4
        ip_payload = frame[offset:]
    elif linktype == DLT_LINUX_SLL:
        # Linux cooked capture v1 header is 16 bytes; protocol at offset 14.
        if len(frame) < 16:
            return None
        ethertype = struct.unpack("!H", frame[14:16])[0]
        ip_payload = frame[16:]
        if ethertype not in (ETHERTYPE_IPV4, ETHERTYPE_IPV6):
            return None
    elif linktype == DLT_NULL:
        # BSD loopback header (4 bytes) + IP; family is host endian and OS-dependent.
        if len(frame) < 4:
            return None
        ip_payload = frame[4:]
    else:
        return None

    if len(ip_payload) < 1:
        return None
    version = ip_payload[0] >> 4
    if version == 4:
        if len(ip_payload) < 20:
            return None
        ihl = (ip_payload[0] & 0x0F) * 4
        if ihl < 20 or len(ip_payload) < ihl:
            return None
        src = ip_payload[12:16]
        dst = ip_payload[16:20]
        return struct.unpack("!I", src)[0], struct.unpack("!I", dst)[0]
    if version == 6:
        return None
    return None


def iter_pcap_files(root: Path, recursive: bool) -> Iterator[Path]:
    exts = {".pcap", ".cap"}

    def is_pcap_file(p: Path) -> bool:
        if p.is_dir():
            return False
        if p.suffix in exts:
            return True
        if p.suffix == ".gz" and p.with_suffix("").suffix in exts:
            return True
        return False

    if root.is_file():
        if is_pcap_file(root):
            yield root
        return

    if recursive:
        for dirpath, _dirnames, filenames in os.walk(root):
            for name in filenames:
                p = Path(dirpath) / name
                if is_pcap_file(p):
                    yield p
    else:
        for p in sorted(root.iterdir()):
            if is_pcap_file(p):
                yield p


def _process_one_file(
    pcap_path: Path,
    out_dir: Path,
    use_incl_len: bool,
    max_packets: int,
) -> Tuple[int, int, int, int]:
    total_packets = 0
    total_written = 0
    total_skipped = 0
    total_errors = 0

    file_handles: dict[Path, "TextIO"] = {}
    try:
        with _open_maybe_gzip(pcap_path) as f:
            gh = read_pcap_global_header(f)
            for _ts, incl_len, orig_len, frame in iter_pcap_packets(f, gh):
                total_packets += 1
                pair = parse_ip_pair_from_frame(gh.linktype, frame)
                if pair is None:
                    total_skipped += 1
                else:
                    src, dst = pair
                    pkt_len = incl_len if use_incl_len else orig_len
                    if pkt_len <= 0:
                        pkt_len = incl_len
                    key_id = (src << 32) | dst
                    out_path = out_dir / f"{key_id}_{src}_{dst}.txt"
                    out = file_handles.get(out_path)
                    if out is None:
                        out = out_path.open("a", encoding="utf-8", newline="\n")
                        file_handles[out_path] = out
                    out.write(f"{src}\t{dst}\t{pkt_len}\n")
                    total_written += 1
                if max_packets and total_packets >= max_packets:
                    break
    except Exception:
        total_errors += 1
    finally:
        for out in file_handles.values():
            out.close()

    return total_packets, total_written, total_skipped, total_errors


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(
        description="Extract per-packet (src_ip, dst_ip, packet_length) from PCAP(s) into one .txt file."
    )
    ap.add_argument(
        "input",
        nargs="?",
        default=".",
        help="PCAP file or directory (default: current directory).",
    )
    ap.add_argument(
        "-o",
        "--output",
        default="",
        help="Output directory (deprecated; use --out-dir).",
    )
    ap.add_argument(
        "--out-dir",
        default="/mydata/zkTelemetry/testdata/caida_pcap/caida_txt",
        help="Output directory for per-key .txt files.",
    )
    ap.add_argument(
        "-r",
        "--recursive",
        action="store_true",
        help="Recurse into subdirectories when input is a directory.",
    )
    ap.add_argument(
        "--use-incl-len",
        action="store_true",
        help="Use captured length (incl_len) instead of original length (orig_len) as packet_length.",
    )
    ap.add_argument(
        "--max-packets",
        type=int,
        default=0,
        help="Stop after N packets total (0 = no limit).",
    )
    ap.add_argument(
        "-j",
        "--jobs",
        type=int,
        default=1,
        help="Parallel worker processes (default: 1).",
    )
    args = ap.parse_args(argv)

    in_path = Path(args.input)
    out_dir = Path(args.out_dir)
    if args.output:
        out_dir = Path(args.output)
    out_dir.mkdir(parents=True, exist_ok=True)

    paths = list(iter_pcap_files(in_path, args.recursive))
    if not paths:
        print(f"[warn] no PCAP files found under {in_path}", file=sys.stderr)
        return 1

    total_packets = 0
    total_written = 0
    total_skipped = 0
    total_errors = 0

    if args.jobs <= 1:
        for pcap_path in paths:
            p, w, s, e = _process_one_file(
                pcap_path, out_dir, args.use_incl_len, args.max_packets
            )
            total_packets += p
            total_written += w
            total_skipped += s
            total_errors += e
            if args.max_packets and total_packets >= args.max_packets:
                break
    else:
        if args.max_packets:
            print(
                "[warn] --max-packets is applied per file when --jobs>1",
                file=sys.stderr,
            )
        tmp_files = []
        for pcap_path in paths:
            tmp_dir_path = Path(
                tempfile.mkdtemp(dir=out_dir, prefix="pcap_ip_pairs_tmp_")
            )
            tmp_files.append((pcap_path, tmp_dir_path))

        with ProcessPoolExecutor(max_workers=args.jobs) as ex:
            futs = []
            for pcap_path, tmp_path in tmp_files:
                futs.append(
                    ex.submit(
                        _process_one_file,
                        pcap_path,
                        tmp_path,
                        args.use_incl_len,
                        args.max_packets,
                    )
                )
            for fut in as_completed(futs):
                p, w, s, e = fut.result()
                total_packets += p
                total_written += w
                total_skipped += s
                total_errors += e
        for _pcap_path, tmp_path in tmp_files:
            for tmp_file in tmp_path.iterdir():
                final_path = out_dir / tmp_file.name
                with tmp_file.open("r", encoding="utf-8") as src, final_path.open(
                    "a", encoding="utf-8", newline="\n"
                ) as dst:
                    dst.write(src.read())
                tmp_file.unlink(missing_ok=True)
            tmp_path.rmdir()

    print(
        f"Done. packets={total_packets} written={total_written} skipped={total_skipped} errors={total_errors} output_dir={out_dir}",
        file=sys.stderr,
    )
    return 0 if total_errors == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
