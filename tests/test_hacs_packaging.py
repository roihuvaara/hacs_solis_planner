from __future__ import annotations

import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class HacsPackagingTests(unittest.TestCase):
    def test_root_repo_has_hacs_metadata(self) -> None:
        hacs_path = ROOT / "hacs.json"
        self.assertTrue(hacs_path.exists(), "expected hacs.json at repository root")

        hacs = json.loads(hacs_path.read_text())
        self.assertEqual("Solis Planner", hacs["name"])
        self.assertIn("homeassistant", hacs)

    def test_custom_component_is_self_contained(self) -> None:
        component_dir = ROOT / "custom_components" / "solis_planner"
        self.assertTrue(component_dir.exists(), "expected custom component directory")

        manifest = json.loads((component_dir / "manifest.json").read_text())
        self.assertEqual("solis_planner", manifest["domain"])
        self.assertEqual("Solis Planner", manifest["name"])
        self.assertIn("version", manifest)

        self.assertTrue((component_dir / "__init__.py").exists())
        self.assertTrue((component_dir / "config_flow.py").exists())
        self.assertTrue((component_dir / "services.yaml").exists())
        self.assertTrue((component_dir / "translations" / "en.json").exists())
        self.assertTrue((component_dir / "bridge.py").exists())
        self.assertTrue((component_dir / "planner").exists())


if __name__ == "__main__":
    unittest.main()
