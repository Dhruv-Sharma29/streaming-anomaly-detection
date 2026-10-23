"""The Java detector (used by the Flink job) must match the Python reference exactly. Runs the
compiled job jar's DetectorCli on reference final windows: no cluster needed, only the JDK."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from detector.ewma import run
from generator.generate import Config, generate
from reference.compute import compute
from semantics import CONSTANTS_FILE

ROOT = Path(__file__).resolve().parents[1]
from infra.stack import JAVA_HOME  # noqa: E402  (bundled JDK on Linux, $JAVA_HOME on macOS)

JAVA = JAVA_HOME / "bin/java"
JAR = ROOT / "job/target/stream-job-0.1.0.jar"

pytestmark = pytest.mark.skipif(not (JAVA.exists() and JAR.exists()), reason="build the job jar first")


@pytest.mark.parametrize("seed", range(1, 11))
def test_java_detector_matches_python_exactly(seed, tmp_path):
    _, records = generate(Config(seed=seed))
    windows_ = compute([r["value"] for r in records])["windows"]
    path = tmp_path / "windows.json"
    path.write_text(json.dumps(windows_))
    java = json.loads(subprocess.run([str(JAVA), "-cp", str(JAR), "pipeline.DetectorCli", str(CONSTANTS_FILE), str(path)],
                                     capture_output=True, text=True, check=True).stdout)
    python = run(windows_)
    assert java == python  # exact, including z_at_open floats
    assert python, "scenario should produce at least one alert"
