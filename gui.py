import customtkinter as ctk
import os
import json
import time
import glob
from datetime import datetime

ctk.set_appearance_mode("Dark")
ctk.set_default_color_theme("dark-blue")


class WorkerCard(ctk.CTkFrame):
    def __init__(self, master, worker_data, is_online):
        status = worker_data.get("status", "Unknown")
        time_diff = worker_data.get("time_diff", 0)

        if not is_online or time_diff > 90:
            border_color = "#d9534f"  # red
            status_text = "Offline"
        else:
            if status.lower() == "busy":
                border_color = "#1f6feb"  # blue
            else:
                border_color = "#2ecc71"  # green
            status_text = status

        super().__init__(master, fg_color="#1f1f1f", border_width=2, border_color=border_color, corner_radius=10)

        worker_id = worker_data.get("worker_id", "Unknown")
        role = worker_data.get("role", "Unknown")
        current_job = worker_data.get("current_job", "None")
        last_seen = worker_data.get("last_seen", "Unknown")

        title = ctk.CTkLabel(self, text=f"Worker ID: {worker_id}", font=("Helvetica", 16, "bold"))
        title.pack(anchor="w", padx=12, pady=(10, 2))

        status_label = ctk.CTkLabel(self, text=f"Status: {status_text}")
        status_label.pack(anchor="w", padx=12)

        role_label = ctk.CTkLabel(self, text=f"Role: {role}")
        role_label.pack(anchor="w", padx=12)

        job_label = ctk.CTkLabel(self, text=f"Current Job: {current_job}")
        job_label.pack(anchor="w", padx=12)

        last_seen_label = ctk.CTkLabel(self, text=f"Last Seen: {last_seen}")
        last_seen_label.pack(anchor="w", padx=12, pady=(0, 10))


class RenderFleetApp(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("RenderFleet Commander")
        self.geometry("1000x600")

        self.syncthing_root = os.path.expanduser("~/RenderFleet")

        header = ctk.CTkFrame(self, fg_color="#111111")
        header.pack(fill="x", padx=10, pady=10)

        title = ctk.CTkLabel(header, text="Fleet Status", font=("Helvetica", 20, "bold"))
        title.pack(anchor="w", padx=12, pady=10)

        self.monitor_frame = ctk.CTkScrollableFrame(self, fg_color="#151515")
        self.monitor_frame.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        self.refresh_fleet()

    def refresh_fleet(self):
        heartbeat_dir = os.path.join(self.syncthing_root, "_system", "heartbeats")
        heartbeat_files = glob.glob(os.path.join(heartbeat_dir, "*.json"))
        current_time = time.time()

        for widget in self.monitor_frame.winfo_children():
            widget.destroy()

        if not heartbeat_files:
            empty = ctk.CTkLabel(self.monitor_frame, text="No workers found.")
            empty.pack(padx=12, pady=12, anchor="w")
        else:
            for hb_path in heartbeat_files:
                try:
                    with open(hb_path, "r", encoding="utf-8") as f:
                        data = json.load(f)

                    heartbeat_timestamp = data.get("timestamp", 0)
                    time_diff = current_time - heartbeat_timestamp

                    last_seen = f"{int(time_diff)}s ago"
                    data["time_diff"] = time_diff
                    data["last_seen"] = last_seen

                    is_online = time_diff <= 90

                    card = WorkerCard(self.monitor_frame, data, is_online)
                    card.pack(fill="x", padx=12, pady=8)
                except (OSError, json.JSONDecodeError):
                    continue

        self.after(2000, self.refresh_fleet)


if __name__ == "__main__":
    app = RenderFleetApp()
    app.mainloop()
