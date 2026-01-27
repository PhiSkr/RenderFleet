import glob
import json
import os
import random
import shutil
import subprocess
import sys
import time
import threading
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
from dispatcher import FleetDispatcher


def load_config():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(base_dir, "config.json")
    local_config_path = os.path.join(base_dir, "local_config.json")
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            config = json.load(f)
        print(f"✅ Config loaded from {config_path}")
    except (OSError, json.JSONDecodeError) as e:
        print(f"❌ ERROR loading config at {config_path}: {e}")
        raise

    local_data = {}
    if os.path.exists(local_config_path):
        try:
            with open(local_config_path, "r", encoding="utf-8") as f:
                local_data = json.load(f)
        except (OSError, json.JSONDecodeError):
            local_data = {}
    else:
        local_data = {
            "worker_id": config.get("worker_id"),
            "initial_role": config.get("initial_role"),
        }
        try:
            with open(local_config_path, "w", encoding="utf-8") as f:
                json.dump(local_data, f, indent=4)
        except OSError:
            pass

    config.update(local_data)

    if "paused" not in config:
        config["paused"] = False
    if "weights" not in config:
        config["weights"] = {"default": 10}

    root = config.get("syncthing_root") or "~/RenderFleet"
    root = os.path.abspath(os.path.expanduser(root))
    if "inbox_path" not in config:
        config["inbox_path"] = os.path.join(root, "01_job_factory")
    if "command_path" not in config:
        config["command_path"] = os.path.join(root, "_system", "commands")
    if "heartbeat_path" not in config:
        config["heartbeat_path"] = os.path.join(root, "_system", "heartbeats")
    for key in ("inbox_path", "command_path", "heartbeat_path"):
        config[key] = os.path.abspath(os.path.expanduser(config[key]))

    if "scripts" not in config:
        raise KeyError("scripts missing from merged config")

    print(f"DEBUG: Config merged. Keys: {list(config.keys())}")
    print(
        f"DEBUG: Using worker_id={config.get('worker_id')} "
        f"role={config.get('initial_role')}"
    )
    return config


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, "config.json")
CONFIG = load_config()
print(f"DEBUG: Config in Memory: {CONFIG}")
print(f"DEBUG: Heartbeat Path value: '{CONFIG.get('heartbeat_path')}'")


def get_sys_path(subpath):
    root = CONFIG.get("syncthing_root")
    if not root:
        root = os.path.expanduser("~/RenderFleet")
        print("⚠️ WARNING: syncthing_root missing in config, using default ~/RenderFleet")
    root = os.path.abspath(os.path.expanduser(root))
    if os.path.isabs(subpath):
        full_path = os.path.abspath(os.path.expanduser(subpath))
    else:
        full_path = os.path.abspath(os.path.join(root, subpath))
    if not os.path.exists(full_path):
        print(f"DEBUG: Path resolution: '{subpath}' -> '{full_path}'")
    return full_path


def send_heartbeat(config, status="IDLE", current_job=None):
    heartbeat = {
        "worker_id": config.get("worker_id"),
        "timestamp": int(time.time()),
        "status": status,
        "role": config.get("initial_role"),
        "current_job": current_job,
    }

    hb_folder = config.get("heartbeat_path", "")
    hb_folder = os.path.expanduser(hb_folder)
    if not os.path.isabs(hb_folder):
        hb_folder = os.path.abspath(hb_folder)
    os.makedirs(hb_folder, exist_ok=True)
    hb_path = os.path.join(hb_folder, f"{heartbeat['worker_id']}.json")
    print(f"DEBUG: Writing Heartbeat to: '{hb_path}'")

    with open(hb_path, "w", encoding="utf-8") as f:
        json.dump(heartbeat, f, indent=4)

    print(f"♥ Heartbeat sent: {status}")


def check_yield_command(config):
    worker_id = config.get("worker_id")
    if not worker_id:
        return False
    cmd_root = config.get("command_path")
    if cmd_root:
        cmd_root = os.path.abspath(os.path.expanduser(cmd_root))
    else:
        cmd_root = get_sys_path(os.path.join("_system", "commands"))
    cmd_path = os.path.join(cmd_root, f"{worker_id}.cmd")
    if not os.path.exists(cmd_path):
        return False
    try:
        with open(cmd_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return False
    if data.get("action") != "yield":
        return False
    try:
        os.remove(cmd_path)
    except OSError:
        pass
    return True


def dispatcher_loop(config):
    dispatcher = FleetDispatcher(config, get_sys_path)
    while True:
        role = config.get("initial_role", "")
        if role.endswith("_lead"):
            dispatcher.recover_dead_workers()
            img_queue = get_sys_path(os.path.join("01_job_factory", "img_queue"))
            active_floor = get_sys_path("02_active_floor")
            dispatcher.enforce_vip_preemption(img_queue, active_floor)
            dispatcher.dispatch_smart()
        time.sleep(15)


def process_command_file(file_path, config):
    if os.path.basename(file_path) != f"{config.get('worker_id')}.cmd":
        return
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return
    print(f"⚡ COMMAND RECEIVED: {data}")
    action = data.get("action")
    if action == "yield":
        return
    new_role = None
    if "role" in data:
        new_role = data.get("role")
    elif action == "set_role":
        new_role = data.get("role") or data.get("value")
    if new_role:
        config["initial_role"] = new_role
        local_config_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "local_config.json"
        )
        try:
            with open(local_config_path, "r", encoding="utf-8") as f:
                cfg_on_disk = json.load(f)
        except (OSError, json.JSONDecodeError):
            cfg_on_disk = {}
        cfg_on_disk["worker_id"] = config.get("worker_id")
        cfg_on_disk["initial_role"] = new_role
        try:
            with open(local_config_path, "w", encoding="utf-8") as f:
                json.dump(cfg_on_disk, f, indent=4)
        except OSError:
            pass
        print(f"🔄 ROLE CHANGED: Now acting as {new_role}")
    if action == "stop":
        print("🛑 STOPPING...")
        try:
            os.remove(file_path)
        except OSError:
            pass
        sys.exit(0)
    if action == "pause":
        config["paused"] = True
    elif action == "unpause":
        config["paused"] = False
    try:
        os.remove(file_path)
    except OSError:
        pass


class RenderFleetHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            return
        print(f"⚡ EVENT DETECTED: {event.src_path}")
        process_command_file(event.src_path, CONFIG)

    def on_modified(self, event):
        if event.is_directory:
            return
        process_command_file(event.src_path, CONFIG)


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
            config["paused"] = True
            try:
                with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                    cfg_on_disk = json.load(f)
            except (OSError, json.JSONDecodeError):
                cfg_on_disk = {}
            cfg_on_disk["paused"] = True
            try:
                with open(CONFIG_PATH, "w", encoding="utf-8") as f:
                    json.dump(cfg_on_disk, f, indent=4)
            except OSError:
                pass
            print("🛑 PAUSED Work.")
        elif action == "start":
            config["paused"] = False
            try:
                with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                    cfg_on_disk = json.load(f)
            except (OSError, json.JSONDecodeError):
                cfg_on_disk = {}
            cfg_on_disk["paused"] = False
            try:
                with open(CONFIG_PATH, "w", encoding="utf-8") as f:
                    json.dump(cfg_on_disk, f, indent=4)
            except OSError:
                pass
            print("▶️ RESUMED Work.")
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
        progress_path = os.path.join(target_dir, "progress.json")
        completed = []
        try:
            with open(progress_path, "r", encoding="utf-8") as f:
                progress_data = json.load(f)
                completed = progress_data.get("completed_files", [])
        except (OSError, json.JSONDecodeError):
            completed = []

        prompt_index = 0
        for line in lines:
            prompt = line.strip()
            if not prompt:
                continue
            prompt_index += 1
            prompt_job_name = f"{job_name}_p{prompt_index}"
            if prompt_job_name in completed:
                continue
            print(f"🎨 Generating Image for prompt: \"{prompt}\"")
            success = run_actiona(
                config["scripts"]["img_gen"],
                prompt,
                config,
                output_dir=target_dir,
                job_name=prompt_job_name,
            )
            if success:
                completed.append(prompt_job_name)
                try:
                    with open(progress_path, "w", encoding="utf-8") as f:
                        json.dump(
                            {"completed_files": completed, "status": "in_progress"},
                            f,
                            indent=4,
                        )
                except OSError:
                    pass
            print(f"DEBUG: Checking for preemption commands for {config.get('worker_id')}...")
            if check_yield_command(config):
                print("🛑 Preemption requested. Yielding job...")
                img_queue = get_sys_path(os.path.join("01_job_factory", "img_queue"))
                os.makedirs(img_queue, exist_ok=True)
                try:
                    shutil.move(job_path, os.path.join(img_queue, filename))
                except OSError:
                    pass
                return True
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
        progress_path = os.path.join(job_path, "progress.json")
        completed = []
        try:
            with open(progress_path, "r", encoding="utf-8") as f:
                progress_data = json.load(f)
                completed = progress_data.get("completed_files", [])
        except (OSError, json.JSONDecodeError):
            completed = []
        for image_name in images:
            if image_name in completed:
                continue
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
            success = run_actiona(
                config["scripts"]["vid_gen"],
                "",
                config,
                output_dir=job_path,
                job_name=f"{image_name}_vid",
                output_ext=".mp4",
                num_outputs=2,
                prompt_text=prompt_text,
            )
            if success:
                completed.append(image_name)
                try:
                    with open(progress_path, "w", encoding="utf-8") as f:
                        json.dump(
                            {"completed_files": completed, "status": "in_progress"},
                            f,
                            indent=4,
                        )
                except OSError:
                    pass
            print(f"DEBUG: Checking for preemption commands for {config.get('worker_id')}...")
            if check_yield_command(config):
                print("🛑 Preemption requested. Yielding job...")
                vid_queue = get_sys_path(os.path.join("01_job_factory", "vid_queue"))
                os.makedirs(vid_queue, exist_ok=True)
                try:
                    shutil.move(job_path, os.path.join(vid_queue, filename))
                except OSError:
                    pass
                return True

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
    dispatcher_thread = threading.Thread(
        target=dispatcher_loop, args=(CONFIG,), daemon=True
    )
    dispatcher_thread.start()
    observer = Observer()
    inbox = CONFIG["inbox_path"]
    cmds = CONFIG["command_path"]
    inbox = os.path.abspath(os.path.expanduser(inbox))
    cmds = os.path.abspath(os.path.expanduser(cmds))
    os.makedirs(inbox, exist_ok=True)
    os.makedirs(cmds, exist_ok=True)
    observer.schedule(RenderFleetHandler(), inbox, recursive=False)
    observer.schedule(RenderFleetHandler(), cmds, recursive=False)
    print(f"DEBUG: 👀 Watching INBOX at: '{inbox}'")
    print(f"DEBUG: 👀 Watching COMMANDS at: '{cmds}'")
    print(
        "DEBUG: Paths -> "
        f"inbox_path='{CONFIG.get('inbox_path')}', "
        f"command_path='{CONFIG.get('command_path')}', "
        f"heartbeat_path='{CONFIG.get('heartbeat_path')}'"
    )
    startup_cmd = os.path.join(cmds, f"{CONFIG.get('worker_id')}.cmd")
    if os.path.exists(startup_cmd):
        process_command_file(startup_cmd, CONFIG)
    send_heartbeat(CONFIG, status="STARTING")
    observer.start()
    try:
        while True:
            check_commands(CONFIG)
            if CONFIG.get("paused"):
                send_heartbeat(CONFIG, status="PAUSED")
                time.sleep(2)
                continue
            did_work = process_jobs(CONFIG)
            if did_work:
                continue
            send_heartbeat(CONFIG, status="IDLE")
            time.sleep(5)
    except KeyboardInterrupt:
        print("\n🛑 Worker stopping...")
        observer.stop()
        observer.join()
        send_heartbeat(CONFIG, status="OFFLINE")
        sys.exit(0)


if __name__ == "__main__":
    main()
