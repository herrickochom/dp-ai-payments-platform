"""Programme control system for the DP AI Payments Platform.

The repository is the engineering source of truth. This package reads the
structured registries under ``programme/registry`` and the controlled
configuration under ``programme/config``, collects approved machine
evidence, and renders the enterprise governance and assurance workbook.

Design rules enforced by this package:

* Evidence first. Nothing is claimed that is not evidenced in the repository.
* ``EXISTS != PASS``. An artefact that exists but is empty is reported as
  ``PRESENT_EMPTY``, never as a pass.
* Machine evidence never becomes human approval.
* Historical actual dates are never invented.
* Automation never writes a human-controlled field.
* Secret values never enter programme metadata, the workbook, or reports.
"""

from __future__ import annotations

__version__ = "1.0.0"

PROGRAMME_TOOLING_VERSION = __version__
