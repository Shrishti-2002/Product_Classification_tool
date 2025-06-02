# import subprocess
# import sys
# import time
# import psutil
#
# def kill_existing_streamlit():
#     for proc in psutil.process_iter(attrs=['pid', 'name', 'cmdline']):
#         try:
#             cmdline = proc.info.get('cmdline')
#             if cmdline and any("streamlit" in arg.lower() for arg in cmdline):
#                 print(f"Killing existing Streamlit process (PID {proc.pid})")
#                 proc.terminate()
#                 proc.wait(timeout=5)
#         except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.TimeoutExpired):
#             continue
#
# def run_streamlit():
#     streamlit_script = "UI_Classification_tool.py"  # ✅ DO NOT point this to launcher.py
#     port = ("8502")
#
#     subprocess.Popen([
#         sys.executable, "-m", "streamlit", "run", streamlit_script,
#         "--server.port", port,
#         "--server.headless", "true"
#     ])
#     print("✅ Streamlit app launched.")
#     input("Press Enter to exit the launcher...")  # Keeps window open
#
# if __name__ == "__main__":
#     print("🔍 Checking for existing Streamlit processes...")
#     kill_existing_streamlit()
#     time.sleep(2)
#     print("🚀 Launching Streamlit app...")
#     run_streamlit()


import subprocess
import os
import sys
import time

def run_streamlit():
    filename = "UI_Classification_tool.py"
    port = 8502

    try:
        # Start Streamlit as a subprocess
        subprocess.Popen(
            [sys.executable, "-m", "streamlit", "run", filename, "--server.port", str(port)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        time.sleep(2)  # Give Streamlit some time to start
        os.system(f"start http://localhost:{port}")  # Opens the default browser
    except Exception as e:
        print("Failed to launch Streamlit:", e)

if __name__ == "__main__":
    run_streamlit()
