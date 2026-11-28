import json

import pandas as pd
import pytest

from access.esmf_trace.coupling import (
    coupling_steps,
    coupling_steps_for_case,
    coupling_summary,
)

RUN = "[ESMF]/[ensemble] RunPhase1/[ESM0001] RunPhase1"
NS = 1_000_000_000

ICE_S = 2.0
SERIAL_S = 0.5
TRANSFER_S = 0.1

OCN_WORK = [3.0, 1.0, 3.0, 1.0, 3.0]


def _event(path, pet, start_s, end_s, depth=3):
    return {
        "model_component": path,
        "pet": pet,
        "start": round(start_s * NS),
        "end": round(end_s * NS),
        "duration_s": end_s - start_s,
        "depth": depth,
    }


def _timeseries(work, speed=None, n_med=1):
    """
    Simulate MED PETs 0..n_med-1 and, after them, the PETs of each concurrent
    component in `work` ({tag: per-step work}); speed[tag] gives one work
    multiplier per PET of that component (default one PET at 1.0).

    Per step the MED side sends to every component in turn, runs ICE for
    ICE_S, collects every component in turn, then spends SERIAL_S. Each
    component waits in MED-TO-X until its data is sent.
    """
    speed = speed or {}
    rows = [_event("[ESMF]/[ensemble] Init 1/[ESM0001] IPDv02p5/[OCN-TO-MED] RunPhase1", 0, -5.0, -4.0)]
    pets, next_pet = {}, n_med
    for comp in work:
        pets[comp] = list(range(next_pet, next_pet + len(speed.get(comp, (1.0,)))))
        next_pet += len(pets[comp])
    comp_t = {pet: 0.0 for comp in work for pet in pets[comp]}
    med_t = 0.0

    for i in range(len(next(iter(work.values())))):
        t, sent, finish = med_t, {}, {}
        for comp in work:
            for pet in range(n_med):
                rows.append(_event(f"{RUN}/[MED-TO-{comp}] RunPhase1", pet, t, t + TRANSFER_S))
            t += TRANSFER_S
            sent[comp] = t
        for comp, comp_work in work.items():
            for pet, multiplier in zip(pets[comp], speed.get(comp, (1.0,)), strict=True):
                start = max(comp_t[pet], sent[comp])
                rows.append(_event(f"{RUN}/[MED-TO-{comp}] RunPhase1", pet, comp_t[pet], start))
                comp_t[pet] = start + comp_work[i] * multiplier
                rows.append(_event(f"{RUN}/[{comp}] RunPhase1", pet, start, comp_t[pet]))
                rows.append(_event(f"{RUN}/[{comp}-TO-MED] RunPhase1", pet, comp_t[pet], comp_t[pet]))
            finish[comp] = max(comp_t[pet] for pet in pets[comp])
        for pet in range(n_med):
            rows.append(_event(f"{RUN}/[ICE] RunPhase1", pet, t, t + ICE_S))
            rows.append(_event(f"{RUN}/[ICE] RunPhase1/cice_run_total", pet, t, t + ICE_S, depth=4))
        t += ICE_S
        for comp in work:
            end = max(t, finish[comp])
            for pet in range(n_med):
                rows.append(_event(f"{RUN}/[{comp}-TO-MED] RunPhase1", pet, t, end))
            t = end
        for pet in range(n_med):
            rows.append(_event(f"{RUN}/[MED] med_phases_restart_write", pet, t, t + SERIAL_S))
        med_t = t + SERIAL_S
    return pd.DataFrame(rows)


def test_steps_attribute_waits_to_the_slower_side():
    steps = coupling_steps(_timeseries({"OCN": OCN_WORK}))

    assert len(steps) == 4
    assert set(steps["component"]) == {"OCN"}
    assert steps["med_concurrent_s"].tolist() == pytest.approx([ICE_S] * 4)
    assert steps["med_serial_s"].tolist() == pytest.approx([SERIAL_S] * 4)
    assert steps["med_wait_s"].tolist() == pytest.approx([1.0, 0.0, 1.0, 0.0])
    # the ocean always waits through the serial phase and the transfer, and on
    # 1 s steps also for the last 1 s of ICE
    assert steps["wait_s"].tolist() == pytest.approx([0.6, 1.6, 0.6, 1.6])
    assert (steps["walltime_s"] - steps["active_s"] - steps["wait_s"]).abs().max() < 1e-12
    # ICE shares the MED PET, so it is MED-side work; the Init-phase connector is ignored
    assert steps["ice_active_s"].tolist() == pytest.approx([ICE_S] * 4)
    assert steps["med_active_s"].tolist() == pytest.approx([SERIAL_S] * 4)


def test_every_concurrent_component_is_analysed():
    steps = coupling_steps(_timeseries({"OCN": OCN_WORK, "WAV": [0.5] * 5}))
    ocn = steps[steps["component"] == "OCN"]
    wav = steps[steps["component"] == "WAV"]

    assert len(ocn) == len(wav) == 4
    # OCN's window spans the MED-TO-WAV send and ICE
    assert ocn["med_concurrent_s"].tolist() == pytest.approx([TRANSFER_S + ICE_S] * 4)
    assert ocn["med_wait_s"].tolist() == pytest.approx([0.9, 0.0, 0.9, 0.0])
    # WAV is never the bottleneck; its MED-side window holds ICE and, since OCN is
    # collected first, the MED side's 0.9 s wait on the 3 s ocean steps
    assert (wav["active_s"] < wav["med_concurrent_s"]).all()
    assert wav["med_wait_s"].tolist() == pytest.approx([0.0] * 4)
    assert wav["med_concurrent_s"].tolist() == pytest.approx([ICE_S + 0.9, ICE_S, ICE_S + 0.9, ICE_S])
    assert wav["med_serial_s"].tolist() == pytest.approx([SERIAL_S + TRANSFER_S] * 4)

    only_wav = coupling_steps(_timeseries({"OCN": OCN_WORK, "WAV": [0.5] * 5}), components="WAV")
    assert set(only_wav["component"]) == {"WAV"}

    with pytest.raises(ValueError, match=r"\[ICE\] has no run-phase PETs apart from MED"):
        coupling_steps(_timeseries({"OCN": OCN_WORK}), components="ICE")


def test_slice_and_explicit_pets():
    steps = coupling_steps(_timeseries({"OCN": OCN_WORK}), pets={"OCN": 1, "MED": "0"}, start_index=1, end_index=-1)
    assert steps["coupling_step"].tolist() == [1, 2]

    with pytest.raises(ValueError, match="not MED PETs"):
        coupling_steps(_timeseries({"OCN": [1.0, 1.0]}), pets={"MED": 1})


def test_several_pets_report_the_slowest():
    single = coupling_steps(_timeseries({"OCN": OCN_WORK}))
    several = coupling_steps(_timeseries({"OCN": OCN_WORK}, speed={"OCN": (1.0, 0.8)}, n_med=2))

    shared = ["walltime_s", "active_s", "wait_s", "med_concurrent_s", "med_wait_s", "med_serial_s"]
    pd.testing.assert_frame_equal(several[shared], single[shared])

    slow_only = coupling_steps(
        _timeseries({"OCN": OCN_WORK}, speed={"OCN": (1.0, 0.8)}, n_med=2), pets={"OCN": "2-3:2", "MED": [1]}
    )
    pd.testing.assert_frame_equal(slow_only[shared], single[shared])


def test_walltime_adds_up_to_the_elapsed_time():
    # OCN PET 2 starts step 2 half a second late, so its step 1 is 0.5 s longer and its step 2 0.5 s shorter.
    # The mean over the PETs keeps the total at the elapsed time; the largest per step would add 0.5 s.
    df = _timeseries({"OCN": OCN_WORK}, speed={"OCN": (1.0, 1.0)})
    calls = df.index[(df["pet"] == 2) & df["model_component"].str.endswith("/[OCN] RunPhase1")]
    df.loc[calls[2], "start"] += NS // 2

    walltime = coupling_steps(df)["walltime_s"]
    single = coupling_steps(_timeseries({"OCN": OCN_WORK}))["walltime_s"]
    starts = df.loc[calls, "start"]
    assert walltime.sum() == pytest.approx((starts.iloc[-1] - starts.iloc[0]) / NS)
    assert walltime.tolist() == pytest.approx([single[0], single[1] + 0.25, single[2] - 0.25, single[3]])


def test_unequal_event_counts_raise():
    df = _timeseries({"OCN": [1.0, 1.0, 1.0]})
    df = df.drop(df[(df["pet"] == 1) & df["model_component"].str.endswith("[OCN] RunPhase1")].index[-1])
    with pytest.raises(ValueError, match="Unequal coupling event counts"):
        coupling_steps(df)


def test_summary():
    steps = coupling_steps(_timeseries({"OCN": OCN_WORK, "WAV": [0.5] * 5}))
    summary = coupling_summary(steps, coupling_timestep_seconds={"OCN": 3600, "WAV": 1800}, ncpus=10)

    assert summary.index.tolist() == ["OCN", "WAV"]
    ocn = summary.loc["OCN"]
    assert ocn["seconds_per_sim_day"] == pytest.approx(steps.loc[steps["component"] == "OCN", "walltime_s"].mean() * 24)
    assert ocn["core_hours_per_sim_year"] == pytest.approx(10 * ocn["seconds_per_sim_day"] * 365 / 3600)
    assert summary.loc["WAV", "seconds_per_sim_day"] == pytest.approx(2 * ocn["seconds_per_sim_day"])

    # 3 s ocean steps outrun the 2.1 s window by 0.9 s; the window outruns the 1 s steps by 1.1 s
    assert ocn["idle_on_component_s"] == pytest.approx(1.8 / 4)
    assert ocn["idle_on_med_s"] == pytest.approx(2.2 / 4)
    assert ocn["idle_on_component_steps"] == ocn["idle_on_med_steps"] == 2
    # WAV is never longer than its MED-side window
    assert summary.loc["WAV", "idle_on_med_steps"] == 4
    assert summary.loc["WAV", "idle_on_component_steps"] == 0


def test_summary_means_every_timing_column():
    steps = coupling_steps(_timeseries({"OCN": OCN_WORK}))
    summary = coupling_summary(steps)

    timing = ["walltime_s", "active_s", "wait_s", "med_concurrent_s", "med_wait_s", "med_serial_s", "ice_active_s"]
    for column in timing:
        assert summary.loc["OCN", f"{column[:-2]}_mean_s"] == pytest.approx(steps[column].mean())
    assert summary.loc["OCN", "ice_active_mean_s"] == pytest.approx(ICE_S)


def test_summary_of_no_steps():
    # two coupling calls make one step, which end_index=-1 drops
    steps = coupling_steps(_timeseries({"OCN": [1.0, 1.0]}), end_index=-1)
    assert steps.empty

    summary = coupling_summary(steps, coupling_timestep_seconds=3600, ncpus=10)
    assert summary.empty
    assert summary.index.name == "component"
    assert {"n_steps", "walltime_mean_s", "wait_mean_s", "idle_on_med_s", "core_hours_per_sim_year"} <= set(
        summary.columns
    )


def test_steps_for_case(tmp_path):
    for index in (0, 1):
        out = tmp_path / "postprocessing_case" / f"output{index:03d}"
        out.mkdir(parents=True)
        rows = _timeseries({"OCN": [1.0, 3.0, 1.0]}).to_dict("records")
        (out / "case_timeseries.json").write_text(json.dumps(rows))

    steps = coupling_steps_for_case(tmp_path, "postprocessing_case")
    assert steps["output_name"].tolist() == ["output000"] * 2 + ["output001"] * 2
    assert set(steps["case_name"]) == {"postprocessing_case"}

    with pytest.raises(FileNotFoundError):
        coupling_steps_for_case(tmp_path, "missing")


def test_steps_for_case_keeps_output_order(tmp_path):
    # payu goes on from output999 to output1000, which sorts before output999 as text
    for index in (999, 1000):
        out = tmp_path / "postprocessing_case" / f"output{index:03d}"
        out.mkdir(parents=True)
        rows = _timeseries({"OCN": [1.0, 3.0, 1.0]}).to_dict("records")
        (out / "case_timeseries.json").write_text(json.dumps(rows))

    steps = coupling_steps_for_case(tmp_path, "postprocessing_case")
    assert steps["output_name"].unique().tolist() == ["output999", "output1000"]
