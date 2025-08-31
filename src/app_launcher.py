
import os, sys, webbrowser
from pathlib import Path

def resource_path(rel):
    base = getattr(sys, "_MEIPASS", Path(__file__).parent)
    return str(Path(base) / rel)

def main():
    # Load env from app.env or .env next to the executable (optional)
    try:
        from dotenv import load_dotenv
        for candidate in ("app.env", ".env"):
            f = Path(resource_path(".")) / candidate
            if f.exists():
                load_dotenv(f, override=False)
    except Exception:
        pass

    db = os.getenv("DATABASE_URL", "").strip()
    if not db:
        # Show a simple message and exit (no DB URL field in the UI by design)
        try:
            import tkinter as tk
            from tkinter import messagebox
            root = tk.Tk()
            root.withdraw()
            messagebox.showerror(
                "Missing DATABASE_URL",
                "Create a file named 'app.env' next to the app with a line:\n\n"
                "DATABASE_URL=postgresql://user:pass@host:5432/dbname"
            )
        except Exception:
            print("Missing DATABASE_URL. Create app.env next to the app with:")
            print("DATABASE_URL=postgresql://user:pass@host:5432/dbname")
        sys.exit(1)

    # Pick port (default 8501)
    port = os.environ.get("PORT", "8501")

    # Open browser tab; Streamlit also opens one but this is more reliable when windowed
    url = f"http://localhost:{port}"
    try:
        webbrowser.open_new_tab(url)
    except Exception:
        pass

    # Run Streamlit programmatically
    from streamlit.web.cli import main as streamlit_main
    sys.argv = [
        "streamlit", "run", resource_path("streamlit_app.py"),
        "--server.port", str(port),
        "--server.headless", "false",
    ]
    streamlit_main()

if __name__ == "__main__":
    main()
