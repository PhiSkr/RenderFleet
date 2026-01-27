import json
import os
import shutil
import time


def load_config():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(os.path.dirname(base_dir), "config.json")
    local_path = os.path.join(os.path.dirname(base_dir), "local_config.json")
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    if os.path.exists(local_path):
        with open(local_path, "r", encoding="utf-8") as f:
            local = json.load(f)
        cfg.update(local)
    return cfg


def clear_dir(path):
    if not os.path.exists(path):
        return
    for name in os.listdir(path):
        full = os.path.join(path, name)
        try:
            if os.path.isdir(full):
                shutil.rmtree(full)
            else:
                os.remove(full)
        except OSError:
            pass


def wait_for(condition_fn, timeout=30, interval=1):
    start = time.time()
    while time.time() - start < timeout:
        if condition_fn():
            return True
        time.sleep(interval)
    return False


def read_heartbeat(hb_dir, worker_id):
    hb_path = os.path.join(hb_dir, f"{worker_id}.json")
    try:
        with open(hb_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return {}


def make_vid_job(root, name, frame_count=6):
    os.makedirs(root, exist_ok=True)
    job_dir = os.path.join(root, name)
    os.makedirs(job_dir, exist_ok=True)
    for i in range(frame_count):
        frame = os.path.join(job_dir, f"frame_{i:03d}.png")
        with open(frame, "w", encoding="utf-8") as f:
            f.write("dummy")
        with open(os.path.splitext(frame)[0] + ".txt", "w", encoding="utf-8") as f:
            f.write(f"prompt {i}")


def main():
    cfg = load_config()
    root = os.path.abspath(os.path.expanduser(cfg.get("syncthing_root", "~/RenderFleet")))
    worker_id = cfg.get("worker_id", "worker001")
    vid_queue = os.path.join(root, "01_job_factory", "vid_queue")
    active_inbox = os.path.join(root, "02_active_floor", worker_id, "inbox")
    hb_dir = os.path.abspath(os.path.expanduser(cfg.get("heartbeat_path", os.path.join(root, "_system", "heartbeats"))))
    commands_dir = os.path.join(root, "_system", "commands")

    os.makedirs(vid_queue, exist_ok=True)
    os.makedirs(active_inbox, exist_ok=True)
    os.makedirs(commands_dir, exist_ok=True)

    clear_dir(vid_queue)
    clear_dir(active_inbox)

    # Step 1: setup weighted vid jobs
    make_vid_job(vid_queue, "background_vid_0", frame_count=8)
    make_vid_job(vid_queue, "default_vid_0", frame_count=4)
    make_vid_job(vid_queue, "high_prio_vid_0", frame_count=3)

    # Ensure high priority dispatch
    def inbox_has(prefix):
        return any(name.startswith(prefix) for name in os.listdir(active_inbox))

    got_high = wait_for(lambda: inbox_has("high_prio_vid_0"), timeout=40)
    assert got_high, "[FAIL] high_prio_vid job was not dispatched early"

    # Step 2: preemption with VIP directory
    def background_running():
        hb = read_heartbeat(hb_dir, worker_id)
        return "background_vid" in str(hb.get("current_job", ""))

    wait_for(background_running, timeout=40)

    make_vid_job(vid_queue, "VIP_video", frame_count=2)

    def yield_cmd_exists():
        return os.path.exists(os.path.join(commands_dir, f"{worker_id}.cmd"))

    yielded = wait_for(yield_cmd_exists, timeout=30)
    assert yielded, "[FAIL] Yield command was not issued within 30s"

    def background_returned():
        return any(name.startswith("background_vid") for name in os.listdir(vid_queue))

    returned = wait_for(background_returned, timeout=30)
    assert returned, "[FAIL] Background vid job did not return to queue"

    def vip_dispatched():
        return any("VIP_video" in name for name in os.listdir(active_inbox))

    vip_sent = wait_for(vip_dispatched, timeout=30)
    assert vip_sent, "[FAIL] VIP video job was not dispatched"

    # Step 3: Integrity - ensure job directory still intact after move back
    bg_dir = os.path.join(vid_queue, "background_vid_0")
    assert os.path.isdir(bg_dir), "[FAIL] Background vid directory missing after yield"
    frames = [f for f in os.listdir(bg_dir) if f.endswith(".png")]
    assert frames, "[FAIL] No frames present in background job after yield"

    print("[PASS] Video weights + VIP preemption + integrity checks passed")


if __name__ == "__main__":
    main()
