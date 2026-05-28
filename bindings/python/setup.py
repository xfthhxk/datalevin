from __future__ import annotations

import subprocess
import shutil
from pathlib import Path

from distutils.errors import DistutilsSetupError
from setuptools import setup
from setuptools.command.bdist_wheel import bdist_wheel as _bdist_wheel
from setuptools.command.sdist import sdist as _sdist


PACKAGE_ROOT = Path(__file__).resolve().parent
REPO_ROOT = PACKAGE_ROOT.parent.parent
BUILD_FILE = REPO_ROOT / "build.clj"


def _clean_wheel_build_artifacts() -> None:
    shutil.rmtree(PACKAGE_ROOT / "build", ignore_errors=True)


def _vendor_runtime_jar() -> None:
    if not BUILD_FILE.exists():
        raise DistutilsSetupError(
            "Datalevin wheel builds must run from a full repository checkout so "
            "the runtime jar can be regenerated."
        )

    command = [
        "clojure",
        "-T:build",
        "vendor-jar",
        ":native-platform",
        "all",
    ]
    try:
        subprocess.run(command, cwd=REPO_ROOT, check=True)
    except FileNotFoundError as exc:
        raise DistutilsSetupError("Clojure CLI is required to build Datalevin wheels.") from exc
    except subprocess.CalledProcessError as exc:
        raise DistutilsSetupError("Datalevin runtime vendoring failed.") from exc


class bdist_wheel(_bdist_wheel):
    def finalize_options(self) -> None:
        super().finalize_options()
        self.root_is_pure = True

    def run(self) -> None:
        self.announce("Vendoring all-platform Datalevin runtime jar", level=2)
        _clean_wheel_build_artifacts()
        _vendor_runtime_jar()
        super().run()


class sdist(_sdist):
    def run(self) -> None:
        raise DistutilsSetupError(
            "Datalevin Python releases are wheel-only because the bundled JVM "
            "runtime jar is generated from a full checkout. Build with "
            "`python -m build --wheel` "
            "or `python -m pip wheel --no-build-isolation bindings/python`."
        )


setup(cmdclass={"bdist_wheel": bdist_wheel, "sdist": sdist})
