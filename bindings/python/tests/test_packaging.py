from __future__ import annotations

import importlib.util
from pathlib import Path
from unittest.mock import patch

from setuptools.dist import Distribution


ROOT = Path(__file__).resolve().parents[1]
SETUP_PY = ROOT / "setup.py"


def _load_setup_module():
    spec = importlib.util.spec_from_file_location("datalevin_python_setup", SETUP_PY)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    with patch("setuptools.setup"):
        spec.loader.exec_module(module)
    return module


def test_bdist_wheel_is_universal() -> None:
    module = _load_setup_module()
    command = module.bdist_wheel(Distribution())
    with patch.object(module._bdist_wheel, "finalize_options"):
        command.finalize_options()

    assert command.root_is_pure is True


def test_vendor_runtime_jar_uses_all_platform_runtime() -> None:
    module = _load_setup_module()
    with patch("subprocess.run") as run:
        module._vendor_runtime_jar()
    run.assert_called_once_with(
        [
            "clojure",
            "-T:build",
            "vendor-jar",
            ":native-platform",
            "all",
        ],
        cwd=module.REPO_ROOT,
        check=True,
    )


def test_clean_wheel_build_artifacts_removes_stale_build_lib(tmp_path) -> None:
    module = _load_setup_module()
    build_lib = tmp_path / "build" / "lib" / "datalevin" / "jars"
    build_lib.mkdir(parents=True)
    (build_lib / "datalevin-runtime-0.0.0.jar").write_text("")

    with patch.object(module, "PACKAGE_ROOT", tmp_path):
        module._clean_wheel_build_artifacts()

    assert not (tmp_path / "build").exists()
