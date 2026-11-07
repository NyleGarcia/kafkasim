import json
import subprocess

from kafkasim_harness import CALIBRATED, ROOT, SCENARIOS
from kafkasim_harness.compose import plan
from kafkasim_harness.signature import compare, signature


def test_compose_plan_for_every_calibrated_scenario(tmp_path):
    for slug in CALIBRATED:
        m = json.loads((SCENARIOS / f"{slug}.json").read_text())
        p = plan(m, tmp_path / slug)
        doc = json.loads(p.path.read_text())
        brokers = m["clusters"][0]["brokers"]
        assert len([s for s in doc["services"].values() if s["environment"]["KAFKA_PROCESS_ROLES"] == "broker"]) == len(brokers)
        assert set(p.names.values()) >= {b["id"] for b in brokers}


def test_sim_signature_matches_expected_outcomes(tmp_path):
    out = tmp_path / "sim.json"
    subprocess.run(
        ["npx", "tsx", str(ROOT / "packages/engine/scripts/run.ts"), str(SCENARIOS / "single-cluster-broker-failure.json"), str(out)],
        check=True,
        cwd=ROOT,
    )
    sig = signature(json.loads(out.read_text()))
    assert sig["lost_acked_records"] is False
    assert sig["leadership_left_failed_node"] == {"b1": True}
    assert sig["writes_resumed"] is True
    ok, _ = compare(sig, sig, list(sig))
    assert ok
