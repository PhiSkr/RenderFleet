import glob
import json
import os
import random
import shutil
import subprocess
import sys
import time


def load_config(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.json")
CONFIG = load_config(CONFIG_PATH)


def get_sys_path(subpath):
    root = os.path.expanduser(CONFIG.get("syncthing_root", ""))
    return os.path.join(root, subpath)


def send_heartbeat(config, status="IDLE", current_job=None):
    heartbeat = {
        "worker_id": config.get("worker_id"),
        "timestamp": int(time.time()),
        "status": status,
        "role": config.get("initial_role"),
        "current_job": current_job,
    }

    hb_path = get_sys_path(os.path.join("_system", "heartbeats", f"{heartbeat['worker_id']}.json"))
    hb_dir = os.path.dirname(hb_path)
    os.makedirs(hb_dir, exist_ok=True)

    with open(hb_path, "w", encoding="utf-8") as f:
        json.dump(heartbeat, f, indent=4)

    print(f"♥ Heartbeat sent: {status}")


def check_commands(config):
    worker_id = config.get("worker_id")
    if not worker_id:
        return False

    cmd_path = get_sys_path(os.path.join("_system", "commands", f"{worker_id}.cmd"))
    if not os.path.exists(cmd_path):
        return False

    try:
        with open(cmd_path, "r", encoding="utf-8") as f:
            cmd_data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return False

    try:
        action = cmd_data.get("action")
        if action == "set_role":
            new_role = cmd_data.get("value", "")
            if new_role:
                config["initial_role"] = new_role
                try:
                    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                        cfg_on_disk = json.load(f)
                except (OSError, json.JSONDecodeError):
                    cfg_on_disk = {}
                cfg_on_disk["initial_role"] = new_role
                try:
                    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
                        json.dump(cfg_on_disk, f, indent=4)
                except OSError:
                    pass
                print(f"⚙️ ROLE CHANGED to {new_role}")
        elif action == "stop":
            print("🛑 Remote Stop received.")
            sys.exit(0)
        else:
            return False
    finally:
        try:
            os.remove(cmd_path)
        except OSError:
            pass

    return True


def process_jobs(config):
    inbox_rel = os.path.join("02_active_floor", config.get("worker_id", ""), "inbox")
    review_rel = "03_review_room"
    inbox_path = get_sys_path(inbox_rel)
    review_path = get_sys_path(review_rel)

    try:
        entries = sorted(os.listdir(inbox_path))
    except OSError:
        entries = []

    job_path = None
    for entry in entries:
        candidate = os.path.join(inbox_path, entry)
        if os.path.isfile(candidate) or os.path.isdir(candidate):
            job_path = candidate
            break

    if not job_path:
        return False

    filename = os.path.basename(job_path)
    print(f"⚡ Job found: {filename}")
    send_heartbeat(config, status="BUSY", current_job=filename)

    ext = os.path.splitext(filename)[1].lower()
    if os.path.isfile(job_path) and ext == ".txt":
        try:
            with open(job_path, "r", encoding="utf-8") as f:
                lines = f.read().splitlines()
        except OSError:
            lines = []

        job_name = os.path.splitext(filename)[0]
        target_dir = os.path.join(review_path, job_name)
        os.makedirs(target_dir, exist_ok=True)

        prompt_index = 0
        for line in lines:
            prompt = line.strip()
            if not prompt:
                continue
            prompt_index += 1
            print(f"🎨 Generating Image for prompt: \"{prompt}\"")
            prompt_job_name = f"{job_name}_p{prompt_index}"
            run_actiona(
                config["scripts"]["img_gen"],
                prompt,
                config,
                output_dir=target_dir,
                job_name=prompt_job_name,
            )
        os.makedirs(review_path, exist_ok=True)
        shutil.move(job_path, os.path.join(target_dir, filename))
        print(f"✅ Job finished: {filename}")
        return True

    if os.path.isdir(job_path):
        staging_cfg = config.get("staging_area", "")
        staging_area = get_sys_path(staging_cfg)
        os.makedirs(staging_area, exist_ok=True)
        prompts_cfg = config.get("staging_prompts", "")
        staging_prompts = get_sys_path(prompts_cfg)
        os.makedirs(staging_prompts, exist_ok=True)

        images = [
            f
            for f in sorted(os.listdir(job_path))
            if os.path.splitext(f)[1].lower() in {".png", ".jpg", ".jpeg"}
        ]
        for image_name in images:
            image_path = os.path.join(job_path, image_name)
            prompt_path = os.path.splitext(image_path)[0] + ".txt"
            try:
                with open(prompt_path, "r", encoding="utf-8") as f:
                    prompt_text = f.read()
            except OSError:
                prompt_text = ""

            try:
                with open(
                    os.path.join(staging_prompts, "current_prompt.txt"),
                    "w",
                    encoding="utf-8",
                ) as f:
                    f.write(prompt_text)
            except OSError:
                pass

            for entry in os.listdir(staging_area):
                path = os.path.join(staging_area, entry)
                if os.path.isfile(path):
                    try:
                        os.remove(path)
                    except OSError:
                        pass
            try:
                shutil.copy2(image_path, os.path.join(staging_area, image_name))
            except OSError:
                continue

            print(f"unknown staging image: {image_name}")
            run_actiona(
                config["scripts"]["vid_gen"],
                "",
                config,
                output_dir=job_path,
                job_name=f"{image_name}_vid",
                output_ext=".mp4",
                num_outputs=2,
                prompt_text=prompt_text,
            )

        archive_path = get_sys_path("04_archive")
        os.makedirs(archive_path, exist_ok=True)
        shutil.move(job_path, os.path.join(archive_path, filename))
        print(f"✅ Video Job finished: {filename}")
        return True

    print(f"🎥 Starting Video Generation for: {filename}")
    run_actiona(config["scripts"]["vid_gen"], "", config, output_dir=None, job_name=None)
    os.makedirs(review_path, exist_ok=True)
    shutil.move(job_path, os.path.join(review_path, filename))
    print(f"✅ Job finished: {filename}")
    return True


def get_idle_workers(config, target_type=None):
    hb_dir = get_sys_path(os.path.join("_system", "heartbeats"))
    hb_files = glob.glob(os.path.join(hb_dir, "*.json"))
    now = int(time.time())
    idle_workers = []
    allowed_roles = None
    if target_type == "img":
        allowed_roles = {"img_worker", "img_lead"}
    elif target_type == "vid":
        allowed_roles = {"vid_worker", "vid_lead"}

    for hb_path in hb_files:
        try:
            with open(hb_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (OSError, json.JSONDecodeError):
            continue

        if not isinstance(data, dict):
            continue

        ts = data.get("timestamp")
        status = data.get("status")
        role = data.get("role")
        worker_id = data.get("worker_id")
        if not worker_id or not isinstance(ts, int):
            continue

        if now - ts < 30 and status == "IDLE":
            if allowed_roles is not None and role not in allowed_roles:
                continue
            idle_workers.append(worker_id)

    return idle_workers


def dispatch_jobs(config):
    role = config.get("initial_role")
    if role == "img_lead":
        source_rel = os.path.join("01_job_factory", "img_queue")
    elif role == "vid_lead":
        source_rel = os.path.join("01_job_factory", "vid_queue")
    else:
        return

    source_path = get_sys_path(source_rel)
    job_files = glob.glob(os.path.join(source_path, "*"))
    if role == "img_lead":
        idle_workers = get_idle_workers(config, target_type="img")
    else:
        idle_workers = get_idle_workers(config, target_type="vid")
    if not job_files or not idle_workers:
        return

    job_path = job_files[0]
    filename = os.path.basename(job_path)
    worker_id = idle_workers[0]
    inbox_path = get_sys_path(os.path.join("02_active_floor", worker_id, "inbox"))
    os.makedirs(inbox_path, exist_ok=True)
    shutil.move(job_path, os.path.join(inbox_path, filename))
    print(f"CMD: Dispatched {filename} to {worker_id}")


def run_actiona(
    script_path,
    arguments,
    config,
    output_dir=None,
    job_name=None,
    output_ext=".png",
    num_outputs=4,
    prompt_text=None,
):
    landing_zone_cfg = config.get("landing_zone", "")
    landing_zone = get_sys_path(landing_zone_cfg)
    os.makedirs(landing_zone, exist_ok=True)

    for entry in os.listdir(landing_zone):
        path = os.path.join(landing_zone, entry)
        if os.path.isfile(path):
            try:
                os.remove(path)
            except OSError:
                pass

    if config.get("scripts", {}).get("dry_run", True):
        msg = f"[DRY RUN] Executing: actexec -e {script_path} {arguments}"
        if prompt_text is not None:
            msg = f"{msg} | prompt: {prompt_text}"
        print(msg)
        for i in range(num_outputs):
            rand = random.randint(1, 99999)
            fname = f"random_{rand}{output_ext}" if i % 2 == 0 else f"temp_{rand}{output_ext}"
            fpath = os.path.join(landing_zone, fname)
            open(fpath, "w").close()
            time.sleep(0.1)
    else:
        try:
            cmd = ["actexec", "-e", script_path]
            if arguments:
                cmd.append(arguments)
            subprocess.run(cmd, check=False)
        except OSError:
            return False

    if not output_dir or not job_name:
        return False

    os.makedirs(output_dir, exist_ok=True)
    files = [
        os.path.join(landing_zone, f)
        for f in os.listdir(landing_zone)
        if os.path.isfile(os.path.join(landing_zone, f))
    ]
    if output_ext:
        files = [f for f in files if f.lower().endswith(output_ext)]
    files.sort(key=os.path.getctime)

    for idx, src in enumerate(files[:num_outputs], start=1):
        new_name = f"{job_name}_take{idx:03d}{output_ext}"
        dest = os.path.join(output_dir, new_name)
        shutil.move(src, dest)
        print(f"♻️ Collected and renamed {os.path.basename(src)} -> {new_name}")
    return True


def main():
    print("🚀 RenderFleet Worker started...")
    try:
        while True:
            check_commands(CONFIG)
            dispatch_jobs(CONFIG)
            did_work = process_jobs(CONFIG)
            if did_work:
                continue
            send_heartbeat(CONFIG, status="IDLE")
            time.sleep(5)
    except KeyboardInterrupt:
        print("\n🛑 Worker stopping...")
        send_heartbeat(CONFIG, status="OFFLINE")
        sys.exit(0)


if __name__ == "__main__":
    main()
