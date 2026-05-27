# orchestration/assets/ingestion.py
"""
Health-check asset for the ingestion service.
The ingestor is a long-running process — this asset records its observability.
"""
import subprocess

from dagster import AssetCheckResult, MaterializeResult, MetadataValue, asset, asset_check


@asset(group_name="ingestion", description="Topic bsky.posts in Redpanda")
def redpanda_topic_bsky_posts() -> MaterializeResult:
    """Probe topic depth and latest offset."""
    result = subprocess.run(
        ["docker", "exec", "redpanda", "rpk", "topic", "describe", "bsky.posts"],
        capture_output=True, text=True, check=True,
    )
    return MaterializeResult(
        metadata={"describe": MetadataValue.md(f"```\n{result.stdout}\n```")}
    )


@asset_check(asset=redpanda_topic_bsky_posts)
def topic_has_recent_events() -> AssetCheckResult:
    """Verify the topic received at least one event in the last ~30 seconds."""
    result = subprocess.run(
        ["docker", "exec", "redpanda", "rpk", "topic", "consume",
         "bsky.posts", "-n", "1", "--offset", "end"],
        capture_output=True, text=True, timeout=35,
    )
    ok = result.returncode == 0 and len(result.stdout.strip()) > 0
    return AssetCheckResult(
        passed=ok,
        description="recent event observed" if ok else "no recent events",
    )


assets = [redpanda_topic_bsky_posts]
