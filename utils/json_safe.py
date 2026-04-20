import numpy as np
import pandas as pd

def json_safe(obj):
    """
    Recursively convert Pandas / NumPy objects into JSON-serializable types.
    AML-safe, detector-safe, future-proof.
    """
    if isinstance(obj, dict):
        return {k: json_safe(v) for k, v in obj.items()}

    if isinstance(obj, list):
        return [json_safe(v) for v in obj]

    if isinstance(obj, (np.integer,)):
        return int(obj)

    if isinstance(obj, (np.floating,)):
        return float(obj)

    if isinstance(obj, (pd.Timestamp,)):
        return obj.isoformat()

    return obj
