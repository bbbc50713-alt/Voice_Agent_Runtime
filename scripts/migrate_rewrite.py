"""一次性迁移改写脚本:把从平台复制的语音文件 import 重定向到新服务。

确定性纯替换,跑完打印每文件替换计数;之后仍有少量手写编辑
(routes/voice.py 鉴权段、voice_tasks.py Message→TrackProbe 三处)。
"""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

COMMON = [
    ("from app.core.config import get_settings", "from app.config import get_settings"),
    ("from app.db.session import SessionLocal", "from app.db import SessionLocal"),
    ("from app.services.realtime import create_realtime_event",
     "from app.realtime_bus import create_realtime_event"),
    ("from app.api.deps import TenantContext", "from app.auth import TenantContext"),
]

PER_FILE = {
    "app/services/voice_tasks.py": [
        ("from app.models import CockpitConversation, CockpitVoiceTask, Message",
         "from app.models import VoiceConversation as CockpitConversation, VoiceTask as CockpitVoiceTask\n"
         "from app.services.track_probe import get_track_probe"),
        ("from app.services import cockpit_runtime, cockpit_voice",
         "from app import runtime as cockpit_runtime\nfrom app.services import cockpit_voice"),
        ("cockpit_runtime._new_async_client", "cockpit_runtime.new_async_client"),
    ],
    "app/services/voice_sidecar.py": [
        ("from app.models import CockpitSpeakerEvent, CockpitSpeakerProfile",
         "from app.models import SpeakerEvent as CockpitSpeakerEvent, SpeakerProfile as CockpitSpeakerProfile"),
    ],
    "app/services/cockpit_voice.py": [],
    "app/services/speaker_matcher.py": [],
    "app/routes/voice.py": [
        ("from app.models import (\n    CockpitConversation,\n    CockpitSpeakerProfile,\n    CockpitVoiceTask,\n    RolePermissionProfile,\n    Tenant,\n    User,\n)",
         "from app.models import SpeakerProfile as CockpitSpeakerProfile, VoiceTask as CockpitVoiceTask"),
        ("Depends(require_permission(\"customer.read\"))", "Depends(require_context)"),
    ],
}


def main() -> None:
    for rel, replacements in [(None, None)]:
        pass
    files = [
        "app/services/voice_tasks.py",
        "app/services/voice_sidecar.py",
        "app/services/cockpit_voice.py",
        "app/services/speaker_matcher.py",
        "app/routes/voice.py",
    ]
    for rel in files:
        path = ROOT / rel
        text = path.read_text(encoding="utf-8")
        count = 0
        for old, new in COMMON + PER_FILE.get(rel, []):
            n = text.count(old)
            if n:
                text = text.replace(old, new)
                count += n
            elif old not in ("",):
                print(f"  MISS [{rel}]: {old[:70]!r}")
        path.write_text(text, encoding="utf-8")
        print(f"{rel}: {count} replacements")


if __name__ == "__main__":
    main()
