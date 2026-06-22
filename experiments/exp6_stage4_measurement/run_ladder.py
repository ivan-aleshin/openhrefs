"""Operator runner for the S4-0a slice ladder.

Two sub-commands:
  prepare  -- sample WAT paths for each ladder size from a manifest, write per-size
              lists + an STS object list (same seed -> same files reused across clouds).
  fit      -- given collected per-size metric values (size,value pairs), print the
              fitted slope/r2 and the full-crawl extrapolation for the S4-0a->S4-0b gate.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from experiments.exp6_stage4_measurement.analysis import fit_ladder
from experiments.exp6_stage4_measurement.io import read_wat_manifest
from experiments.exp6_stage4_measurement.stage_wat import build_sts_manifest
from experiments.exp6_stage4_measurement.transforms import nested_ladder_samples

_LADDER = (200, 1000, 3000)


def _prepare(args: argparse.Namespace) -> None:
    Path(args.out_dir).mkdir(parents=True, exist_ok=True)
    manifest = read_wat_manifest(args.manifest)
    samples = nested_ladder_samples(manifest, list(_LADDER), seed=args.seed)
    for size in _LADDER:
        slice_paths = samples[size]
        with open(f"{args.out_dir}/watlist_{size}.txt", "w") as fh:
            fh.write("\n".join(slice_paths) + "\n")
        with open(f"{args.out_dir}/manifest_{size}.csv", "w") as fh:
            fh.write(build_sts_manifest(slice_paths))
    print(
        f"prepared NESTED ladder (200 sub 1000 sub 3000, seed={args.seed}) in {args.out_dir}; "
        "stage manifest_3000.csv once — 200/1000 are subsets of it"
    )


def _fit(args: argparse.Namespace) -> None:
    points = [tuple(p) for p in json.loads(args.points)]  # e.g. '[[200,x],[1000,y],[3000,z]]'
    fit = fit_ladder(points)
    print(f"slope={fit['slope']:.4g} intercept={fit['intercept']:.4g} r2={fit['r2']:.5f}")
    print(
        f"full-crawl ({args.full_files} files) extrapolation: {fit['predict'](args.full_files):.4g}"
    )
    if len(points) < 3:
        print("NOTE: < 3 ladder points — two points fit a line trivially; this supports only an")
        print("      extrapolation-only ADR, NOT the S4-0b full-pass gate (spec).")
    if fit["r2"] < 0.95:
        print("WARNING: r2 < 0.95 — non-linearity; halt full pass and revisit (spec gate).")


def main() -> None:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("prepare")
    p.add_argument("--manifest", required=True, help="wat.paths.gz path")
    p.add_argument("--out-dir", required=True)
    p.add_argument("--seed", type=int, default=42)
    p.set_defaults(func=_prepare)
    f = sub.add_parser("fit")
    f.add_argument("--points", required=True, help="JSON list of [size,value] pairs")
    f.add_argument("--full-files", type=int, default=100_000)
    f.set_defaults(func=_fit)
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
