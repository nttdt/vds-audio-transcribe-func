# activities/cleanup_local.py
import os
import shutil

import azure.durable_functions as df

bp_cleanup_local = df.Blueprint()


@bp_cleanup_local.activity_trigger(input_name="arg")
def CleanupLocal(arg: dict) -> str:
    paths = arg.get("paths", [])
    for p in paths:
        if not p:
            continue
        try:
            if os.path.isdir(p):
                shutil.rmtree(p, ignore_errors=True)
            elif os.path.exists(p):
                os.remove(p)
            else:
                # Might be a file inside a chunk directory already removed
                pass
        except Exception:
            # Best-effort cleanup
            pass
    return "cleaned"
