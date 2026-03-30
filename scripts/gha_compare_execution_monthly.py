import argparse
import os
import sys
from typing import Dict, List, Optional, Tuple

from gha_compare_execution import (
    create_session,
    build_period_chunks,
    durations_from_runs,
    fetch_runs,
    fmt_seconds,
    summarize,
)


def accumulate_period(
    owner: str,
    repo: str,
    session,
    start_date: str,
    end_date: str,
    label: str,
    workflow: Optional[str],
    max_seconds: Optional[float],
) -> Tuple[Dict, Dict[str, int]]:
    all_durations: List[float] = []
    total_sources = {"timing": 0, "jobs": 0, "unavailable": 0, "outlier": 0}

    print(f"=== {label} monthly breakdown ===")
    for chunk_start, chunk_end in build_period_chunks(start_date, end_date, True):
        runs = fetch_runs(session, owner, repo, chunk_start, chunk_end, label, workflow)
        durations, sources = durations_from_runs(session, owner, repo, runs, max_seconds)
        stats = summarize(durations)

        for key, value in sources.items():
            total_sources[key] += value
        all_durations.extend(durations)

        print(
            f"{chunk_start}..{chunk_end}: "
            f"runs={len(runs)}, "
            f"count={stats.get('count', 0)}, "
            f"mean={fmt_seconds(stats.get('mean_s')) if stats.get('count', 0) else '-'}, "
            f"median={fmt_seconds(stats.get('median_s')) if stats.get('count', 0) else '-'}, "
            f"timing={sources['timing']}, "
            f"jobs={sources['jobs']}, "
            f"unavailable={sources['unavailable']}, "
            f"outlier={sources['outlier']}"
        )

    total_stats = summarize(all_durations)
    print()
    print(f"=== {label} total ===")
    print(f"count: {total_stats.get('count', 0)}")
    if total_stats.get("count", 0):
        print(f"mean: {fmt_seconds(total_stats['mean_s'])} ({total_stats['mean_s']:.1f}s)")
        print(f"median: {fmt_seconds(total_stats['median_s'])} ({total_stats['median_s']:.1f}s)")
        print(f"stddev: {fmt_seconds(total_stats['stddev_s'])} ({total_stats['stddev_s']:.1f}s)")
        print(f"min: {fmt_seconds(total_stats['min_s'])} ({total_stats['min_s']:.1f}s)")
        print(f"max: {fmt_seconds(total_stats['max_s'])} ({total_stats['max_s']:.1f}s)")
    print(
        f"sources: timing={total_sources['timing']}, "
        f"jobs-fallback={total_sources['jobs']}, "
        f"unavailable={total_sources['unavailable']}, "
        f"outlier-skipped={total_sources['outlier']}"
    )
    print()
    return total_stats, total_sources


def main():
    parser = argparse.ArgumentParser(
        description="Compare GitHub Actions pure execution durations with monthly breakdown"
    )
    parser.add_argument("--owner", required=True)
    parser.add_argument("--repo", required=True)
    parser.add_argument(
        "--token-env",
        default="GITHUB_TOKEN",
        help="環境変数名からトークンを読み取る (既定: GITHUB_TOKEN)",
    )
    parser.add_argument("--start-1", required=True, help="期間1開始 (YYYY-MM-DD)")
    parser.add_argument("--end-1", required=True, help="期間1終了 (YYYY-MM-DD)")
    parser.add_argument("--start-2", required=True, help="期間2開始 (YYYY-MM-DD)")
    parser.add_argument("--end-2", required=True, help="期間2終了 (YYYY-MM-DD)")
    parser.add_argument(
        "--workflow",
        help="ワークフロー名に含まれる文字列でフィルタ (省略で全ワークフロー)",
    )
    parser.add_argument(
        "--max-duration-hours",
        type=float,
        default=None,
        help="この時間（時間単位）を超えるランを異常値として除外 (例: 24)",
    )
    args = parser.parse_args()

    token = os.getenv(args.token_env)
    if not token:
        print(
            f"環境変数 {args.token_env} に GITHUB トークンを設定してください",
            file=sys.stderr,
        )
        sys.exit(2)

    session = create_session(token)
    max_seconds = args.max_duration_hours * 3600 if args.max_duration_hours is not None else None

    stats1, _ = accumulate_period(
        args.owner,
        args.repo,
        session,
        args.start_1,
        args.end_1,
        "period1",
        args.workflow,
        max_seconds,
    )
    stats2, _ = accumulate_period(
        args.owner,
        args.repo,
        session,
        args.start_2,
        args.end_2,
        "period2",
        args.workflow,
        max_seconds,
    )

    if stats1.get("count", 0) and stats2.get("count", 0):
        diff_mean = stats2["mean_s"] - stats1["mean_s"]
        diff_median = stats2["median_s"] - stats1["median_s"]
        print("=== Difference (period2 - period1) ===")
        print(f"mean diff: {fmt_seconds(diff_mean)} ({diff_mean:.1f}s)")
        print(f"median diff: {fmt_seconds(diff_median)} ({diff_median:.1f}s)")


if __name__ == "__main__":
    main()
