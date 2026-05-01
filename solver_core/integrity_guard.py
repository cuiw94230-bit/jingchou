"""算法完整性守卫：通过 SHA256 校验关键算法文件。"""

import hashlib
from pathlib import Path


EXPECTED_SHA256 = "9EF8E12453B5D41F20150DAE6D29D50A6C465CDBDAAF3FDE6359567B3261CDFB"

def compute_file_sha256(path):
    file_path = Path(path)
    digest = hashlib.sha256()
    with file_path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def verify_algorithm_file(strict=True):
    target = Path(__file__).resolve().parent / "algorithms_impl.py"
    actual = compute_file_sha256(target)
    expected = EXPECTED_SHA256
    ok = actual.lower() == expected.lower()
    if ok:
        return {"ok": True, "actual": actual, "expected": expected}
    if strict:
        raise RuntimeError("algorithm_file_tampered")
    return {"ok": False, "actual": actual, "expected": expected}
