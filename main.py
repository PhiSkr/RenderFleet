import glob
import json
import os
import random
import shutil
import subprocess
import sys
import time
import threading
import tempfile
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

    env_display = os.environ.get("DISPLAY")
    if env_display:
        config["display"] = env_display
        local_data["display"] = env_display
        try:
            with open(local_config_path, "w", encoding="utf-8") as f:
                json.dump(local_data, f, indent=4)
        except OSError:
            pass
    elif "display" not in config:
        config["display"] = ":0"

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
    if not config["scripts"].get("vid_gen"):
        config["scripts"]["vid_gen"] = os.path.join(
            "_system", "scripts", "RunwayVideo.ascr"
        )

    settings_path = os.path.join(root, "_system", "settings.json")
    try:
        if os.path.exists(settings_path):
            with open(settings_path, "r", encoding="utf-8") as f:
                settings = json.load(f)
            if isinstance(settings, dict):
                if "weights" in settings:
                    config["weights"] = settings["weights"]
                if "paused" in settings:
                    config["paused"] = settings["paused"]
        else:
            os.makedirs(os.path.dirname(settings_path), exist_ok=True)
            with open(settings_path, "w", encoding="utf-8") as f:
                json.dump(
                    {"weights": config.get("weights", {"default": 10}), "paused": config.get("paused", False)},
                    f,
                    indent=4,
                )
    except (OSError, json.JSONDecodeError):
        pass

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
    subpath = os.path.expanduser(subpath)
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


def log_activity(msg):
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    log_path = os.path.join(BASE_DIR, "job_activity.log")
    try:
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(f"[{timestamp}] {msg}\n")
    except OSError:
        pass


class ActionaRunner:
    GLOBAL_TIMEOUT_SECONDS = 30 * 60
    INTER_IMAGE_TIMEOUT_SECONDS = 5 * 60

    def __init__(self, config, get_sys_path):
        self.config = config
        self.get_sys_path = get_sys_path

    def _build_env(self):
        env = os.environ.copy()
        env["DISPLAY"] = self.config.get("display", ":0")
        return env

    def _clear_dir_files(self, path):
        os.makedirs(path, exist_ok=True)
        for entry in os.listdir(path):
            file_path = os.path.join(path, entry)
            if os.path.isfile(file_path):
                try:
                    os.remove(file_path)
                except OSError:
                    pass

    def _terminate_process(self, proc):
        if proc.poll() is not None:
            return
        try:
            proc.terminate()
            proc.wait(timeout=10)
        except (OSError, subprocess.TimeoutExpired):
            try:
                proc.kill()
            except OSError:
                pass

    def _list_files(self, landing_zone):
        files = []
        try:
            for entry in os.listdir(landing_zone):
                if os.path.isfile(os.path.join(landing_zone, entry)):
                    files.append(entry)
        except OSError:
            files = []
        return files

    def _list_image_files(self, landing_zone):
        image_exts = {".png", ".jpg", ".jpeg"}
        files = []
        try:
            for entry in os.listdir(landing_zone):
                if os.path.splitext(entry)[1].lower() in image_exts:
                    files.append(entry)
        except OSError:
            files = []
        return files

    def _move_landing_zone_images(self, landing_zone, completed_dir, job_name=None):
        os.makedirs(completed_dir, exist_ok=True)
        moved = []
        print(
            f"DEBUG: Checking Landing Zone at '{landing_zone}'. Exists? {os.path.exists(landing_zone)}"
        )
        try:
            landing_contents = os.listdir(landing_zone)
        except OSError:
            landing_contents = []
        print(f"DEBUG: content of landing zone: {landing_contents}")
        image_exts = {".png", ".jpg", ".jpeg"}
        files = [
            name
            for name in landing_contents
            if os.path.isfile(os.path.join(landing_zone, name))
            and os.path.splitext(name)[1].lower() in image_exts
        ]
        print(f"DEBUG: Filtered image files found: {files}")
        files.sort(key=lambda name: os.path.getctime(os.path.join(landing_zone, name)))
        for idx, name in enumerate(files, start=1):
            src = os.path.join(landing_zone, name)
            ext = os.path.splitext(name)[1]
            if job_name:
                new_name = f"{job_name}_take{idx:03d}{ext}"
            else:
                new_name = name
            dest = os.path.join(completed_dir, new_name)
            try:
                shutil.move(src, dest)
                print(f"♻️ Collected and moved {name} -> {new_name}")
                moved.append(dest)
            except OSError as e:
                print(f"❌ ERROR moving {name} to {dest}: {e}")
                try:
                    shutil.copy2(src, dest)
                    os.remove(src)
                    print(f"⚠️ Fallback: Copied and removed {name}")
                    moved.append(dest)
                except OSError as e2:
                    print(f"❌ CRITICAL: Failed to move/copy {name}: {e2}")
        return moved

    def _run_refresh(self, env):
        refresh_script_cfg = (
            self.config.get("scripts", {}).get("refresh")
            or self.config.get("refresh_script")
            or os.path.join("_system", "scripts", "higgsfield_refresh.ascr")
        )
        refresh_script = self.get_sys_path(refresh_script_cfg)
        cmd = ["actexec", refresh_script]
        try:
            proc = subprocess.Popen(cmd, env=env)
            proc.wait()
        except OSError:
            pass

    def _consume_flags(self, flags_dir):
        image_open_fail = os.path.join(flags_dir, "ImageOpenFail.txt")
        no_hotbar = os.path.join(flags_dir, "NOHOTBAR.txt")
        sensitive = os.path.join(flags_dir, "SENSITIVE.txt")
        issue_flag = os.path.join(flags_dir, "issue.txt")
        prompt_violation = os.path.join(flags_dir, "PromptViolation.txt")

        has_image_open_fail = os.path.exists(image_open_fail)
        has_no_hotbar = os.path.exists(no_hotbar)
        has_sensitive = os.path.exists(sensitive)
        has_issue_flag = os.path.exists(issue_flag)
        has_prompt_violation = os.path.exists(prompt_violation)

        for path in (
            image_open_fail,
            no_hotbar,
            sensitive,
            issue_flag,
            prompt_violation,
        ):
            if os.path.exists(path):
                try:
                    os.remove(path)
                except OSError:
                    pass

        if has_image_open_fail or has_no_hotbar:
            return "retry_refresh"
        if has_sensitive:
            return "retry_sensitive"
        if has_issue_flag or has_prompt_violation:
            return "conditional_retry"

        return None

    def _resolve_script_path(self, script_key):
        scripts = self.config.get("scripts", {})
        script_path_cfg = scripts.get(script_key, script_key)
        if os.path.isabs(script_path_cfg):
            return os.path.abspath(os.path.expanduser(script_path_cfg))
        return self.get_sys_path(script_path_cfg)

    def _execute_with_watchdog(
        self,
        cmd,
        env,
        landing_zone,
        watch_images=True,
        heartbeat_callback=None,
        global_timeout_seconds=None,
    ):
        start_time = time.time()
        first_output_time = None
        last_output_time = None
        seen_files = set()
        partial_success = False
        retry_reason = None
        stdout_file = None
        stderr_file = None
        timeout_val = (
            global_timeout_seconds
            if global_timeout_seconds is not None
            else self.GLOBAL_TIMEOUT_SECONDS
        )

        try:
            print(f"[DEBUG] Executing: {' '.join(cmd)}")
            stdout_file = tempfile.TemporaryFile(mode="w+", encoding="utf-8")
            stderr_file = tempfile.TemporaryFile(mode="w+", encoding="utf-8")
            proc = subprocess.Popen(
                cmd,
                env=env,
                stdout=stdout_file,
                stderr=stderr_file,
            )
        except OSError:
            if stdout_file:
                stdout_file.close()
            if stderr_file:
                stderr_file.close()
            return {"start_failed": True}

        while True:
            if heartbeat_callback:
                heartbeat_callback()
            now = time.time()
            if watch_images:
                current_files = self._list_image_files(landing_zone)
                for name in current_files:
                    if name not in seen_files:
                        seen_files.add(name)
                        if first_output_time is None:
                            first_output_time = now
                        last_output_time = now

                if (
                    first_output_time is not None
                    and last_output_time is not None
                    and now - last_output_time > self.INTER_IMAGE_TIMEOUT_SECONDS
                ):
                    self._terminate_process(proc)
                    partial_success = True
                    break

            if now - start_time > timeout_val:
                self._terminate_process(proc)
                retry_reason = "global_timeout"
                break

            if proc.poll() is not None:
                break

            time.sleep(10)

        stdout_data = ""
        stderr_data = ""
        if proc.poll() is None:
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._terminate_process(proc)
        try:
            stdout_file.seek(0)
            stdout_data = stdout_file.read()
        except OSError:
            stdout_data = ""
        try:
            stderr_file.seek(0)
            stderr_data = stderr_file.read()
        except OSError:
            stderr_data = ""
        if stdout_file:
            stdout_file.close()
        if stderr_file:
            stderr_file.close()

        return {
            "partial_success": partial_success,
            "returncode": proc.returncode,
            "stdout": stdout_data,
            "stderr": stderr_data,
            "retry_reason": retry_reason,
        }

    def run(
        self,
        script_key,
        arguments,
        output_dir=None,
        job_name=None,
        output_ext=".png",
        num_outputs=4,
        prompt_text=None,
        is_image=False,
        heartbeat_callback=None,
        global_timeout=None,
    ):
        landing_zone_cfg = self.config.get("landing_zone", "")
        landing_zone = self.get_sys_path(landing_zone_cfg)
        flags_dir = self.get_sys_path(os.path.join("_system", "flags"))
        completed_dir = (
            output_dir
            if output_dir
            else self.get_sys_path(os.path.join("03_review_room"))
        )
        script_path = self._resolve_script_path(script_key)
        if not os.path.exists(script_path):
            print(f"❌ Script not found: {script_path}")
            return False

        env = self._build_env()
        max_attempts = 2
        watch_images = is_image or (
            output_ext and output_ext.lower() in {".png", ".jpg", ".jpeg"}
        )
        timeout_val = (
            global_timeout
            if global_timeout is not None
            else self.GLOBAL_TIMEOUT_SECONDS
        )

        for attempt in range(1, max_attempts + 1):
            self._clear_dir_files(flags_dir)
            self._clear_dir_files(landing_zone)

            staging_prompts_cfg = self.config.get("staging_prompts") or os.path.join(
                "~/RenderFleet", "_system", "staging_prompts"
            )
            staging_prompts = os.path.abspath(
                os.path.expanduser(staging_prompts_cfg)
            )
            os.makedirs(staging_prompts, exist_ok=True)
            prompt_path = os.path.join(staging_prompts, "current_prompt.txt")
            prompt_text = "" if arguments is None else str(arguments)
            try:
                with open(prompt_path, "w", encoding="utf-8") as f:
                    f.write(prompt_text)
            except OSError:
                pass

            cmd = ["actexec", script_path]

            result = self._execute_with_watchdog(
                cmd,
                env,
                landing_zone,
                watch_images=watch_images,
                heartbeat_callback=heartbeat_callback,
                global_timeout_seconds=timeout_val,
            )

            if result.get("start_failed"):
                return False
            if result.get("retry_reason") == "global_timeout":
                self._run_refresh(env)
                if attempt < max_attempts:
                    continue
                return False

            if not result.get("partial_success"):
                flag_action = self._consume_flags(flags_dir)
                if flag_action == "retry_refresh":
                    self._run_refresh(env)
                    if attempt < max_attempts:
                        continue
                    return False
                if flag_action == "retry_sensitive":
                    if attempt < max_attempts:
                        continue
                    return False
                if flag_action == "conditional_retry":
                    current_files = self._list_files(landing_zone)
                    if output_ext:
                        current_files = [
                            f
                            for f in current_files
                            if f.lower().endswith(output_ext.lower())
                        ]
                    if current_files:
                        print(
                            "⚠️ Flag detected but output exists. Accepting partial success."
                        )
                    else:
                        print("❌ Flag detected and no output. Retrying...")
                        if attempt < max_attempts:
                            continue
                        return False

            if (
                not result.get("partial_success")
                and result.get("returncode") not in (0, None)
            ):
                if result.get("stderr"):
                    print(result["stderr"])
                return False

            if is_image:
                moved_images = self._move_landing_zone_images(
                    landing_zone, completed_dir, job_name=job_name
                )
                if not moved_images:
                    print(
                        "⚠️ Actiona finished but NO images were produced. Marking as failed."
                    )
                    return False
                return True

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
                print(
                    f"♻️ Collected and renamed {os.path.basename(src)} -> {new_name}"
                )
            return True

        return False


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


def load_fleet_settings(config):
    root = config.get("syncthing_root") or "~/RenderFleet"
    root = os.path.abspath(os.path.expanduser(root))
    settings_path = os.path.join(root, "_system", "settings.json")
    if not os.path.exists(settings_path):
        return
    try:
        with open(settings_path, "r", encoding="utf-8") as f:
            settings = json.load(f)
    except (OSError, json.JSONDecodeError):
        return
    if not isinstance(settings, dict):
        return
    if "weights" in settings:
        config["weights"] = settings["weights"]
    config["fleet_paused"] = settings.get("paused", False)


def dispatcher_loop(config):
    dispatcher = FleetDispatcher(config, get_sys_path)
    while True:
        load_fleet_settings(config)
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
        local_config_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "local_config.json"
        )
        try:
            with open(local_config_path, "r", encoding="utf-8") as f:
                cfg_on_disk = json.load(f)
        except (OSError, json.JSONDecodeError):
            cfg_on_disk = {}
        cfg_on_disk["paused"] = True
        try:
            with open(local_config_path, "w", encoding="utf-8") as f:
                json.dump(cfg_on_disk, f, indent=4)
        except OSError:
            pass
    elif action == "unpause":
        config["paused"] = False
        local_config_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "local_config.json"
        )
        try:
            with open(local_config_path, "r", encoding="utf-8") as f:
                cfg_on_disk = json.load(f)
        except (OSError, json.JSONDecodeError):
            cfg_on_disk = {}
        cfg_on_disk["paused"] = False
        try:
            with open(local_config_path, "w", encoding="utf-8") as f:
                json.dump(cfg_on_disk, f, indent=4)
        except OSError:
            pass
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
                local_config_path = os.path.join(
                    os.path.dirname(os.path.abspath(__file__)), "local_config.json"
                )
                try:
                    with open(local_config_path, "r", encoding="utf-8") as f:
                        cfg_on_disk = json.load(f)
                except (OSError, json.JSONDecodeError):
                    cfg_on_disk = {}
                cfg_on_disk["initial_role"] = new_role
                try:
                    with open(local_config_path, "w", encoding="utf-8") as f:
                        json.dump(cfg_on_disk, f, indent=4)
                except OSError:
                    pass
                print(f"⚙️ ROLE CHANGED to {new_role}")
        elif action == "stop":
            config["paused"] = True
            local_config_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "local_config.json"
            )
            try:
                with open(local_config_path, "r", encoding="utf-8") as f:
                    cfg_on_disk = json.load(f)
            except (OSError, json.JSONDecodeError):
                cfg_on_disk = {}
            cfg_on_disk["paused"] = True
            try:
                with open(local_config_path, "w", encoding="utf-8") as f:
                    json.dump(cfg_on_disk, f, indent=4)
            except OSError:
                pass
            print("🛑 PAUSED Work.")
        elif action == "start":
            config["paused"] = False
            local_config_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "local_config.json"
            )
            try:
                with open(local_config_path, "r", encoding="utf-8") as f:
                    cfg_on_disk = json.load(f)
            except (OSError, json.JSONDecodeError):
                cfg_on_disk = {}
            cfg_on_disk["paused"] = False
            try:
                with open(local_config_path, "w", encoding="utf-8") as f:
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
    runner = ActionaRunner(config, get_sys_path)

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
    log_activity(f"⚡ Job found: {filename}")
    send_heartbeat(config, status="BUSY", current_job=filename)
    hb_callback = lambda: send_heartbeat(
        config, status="BUSY", current_job=filename
    )

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
        except FileNotFoundError:
            print("DEBUG: No progress.json found (starting fresh).")
            completed = []
        except (OSError, json.JSONDecodeError) as e:
            print(f"⚠️ Could not load progress.json: {e}")
            log_activity(f"❌ ERROR: Could not load progress.json: {e}")
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
            result = runner.run(
                "img_gen",
                prompt,
                output_dir=target_dir,
                job_name=prompt_job_name,
                is_image=True,
                heartbeat_callback=hb_callback,
            )
            if result:
                completed.append(prompt_job_name)
                log_activity(f"✅ Image set done: {prompt_job_name}")
                if result == "skipped":
                    skipped_marker = os.path.join(
                        target_dir, f"{prompt_job_name}_SKIPPED.txt"
                    )
                    try:
                        with open(skipped_marker, "w", encoding="utf-8") as f:
                            f.write("Skipped due to repeated SENSITIVE flag.")
                    except OSError:
                        pass
                try:
                    with open(progress_path, "w", encoding="utf-8") as f:
                        json.dump(
                            {"completed_files": completed, "status": "in_progress"},
                            f,
                            indent=4,
                        )
                    print(f"💾 Progress saved. {len(completed)} prompts done.")
                except OSError as e:
                    print(f"❌ ERROR writing progress.json: {e}")
                    log_activity(f"❌ ERROR: writing progress.json failed: {e}")
            else:
                log_activity(f"❌ ERROR: Image set failed: {prompt_job_name}")
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
        if not os.path.exists(job_path):
            print(f"⚠️ Job file disappeared (stolen by dispatcher?): {job_path}")
            return False
        try:
            shutil.move(job_path, os.path.join(target_dir, filename))
        except OSError as e:
            log_activity(f"❌ ERROR: Failed to move finished job: {e}")
            print(f"❌ ERROR: Failed to move finished job: {e}")
            return True
        print(f"✅ Job finished: {filename}")
        log_activity(f"✅ Job finished: {filename}")
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
        except FileNotFoundError:
            print("DEBUG: No progress.json found (starting fresh).")
            completed = []
        except (OSError, json.JSONDecodeError) as e:
            print(f"⚠️ Could not load progress.json: {e}")
            log_activity(f"❌ ERROR: Could not load progress.json: {e}")
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
            success = runner.run(
                "vid_gen",
                prompt_text,
                output_dir=job_path,
                job_name=f"{image_name}_vid",
                output_ext=".mp4",
                num_outputs=2,
                prompt_text=prompt_text,
                heartbeat_callback=hb_callback,
                global_timeout=45 * 60,
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
                    print(f"💾 Progress saved. {len(completed)} prompts done.")
                except OSError as e:
                    print(f"❌ ERROR writing progress.json: {e}")
                    log_activity(f"❌ ERROR: writing progress.json failed: {e}")
            else:
                log_activity(f"❌ ERROR: Video generation failed: {image_name}")
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
        if not os.path.exists(job_path):
            print(f"⚠️ Job file disappeared (stolen by dispatcher?): {job_path}")
            return False
        try:
            shutil.move(job_path, os.path.join(archive_path, filename))
        except OSError as e:
            log_activity(f"❌ ERROR: Failed to move finished job: {e}")
            print(f"❌ ERROR: Failed to move finished job: {e}")
            return True
        print(f"✅ Video Job finished: {filename}")
        log_activity(f"✅ Job finished: {filename}")
        return True

    print(f"🎥 Starting Video Generation for: {filename}")
    runner.run(
        "vid_gen",
        "",
        output_dir=None,
        job_name=None,
        heartbeat_callback=hb_callback,
    )
    os.makedirs(review_path, exist_ok=True)
    if not os.path.exists(job_path):
        print(f"⚠️ Job file disappeared (stolen by dispatcher?): {job_path}")
        return False
    shutil.move(job_path, os.path.join(review_path, filename))
    print(f"✅ Job finished: {filename}")
    log_activity(f"✅ Job finished: {filename}")
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
            load_fleet_settings(CONFIG)
            if CONFIG.get("paused", False) or CONFIG.get("fleet_paused", False):
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
